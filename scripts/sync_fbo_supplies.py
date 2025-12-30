from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Any, Dict, List, Tuple

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import (
    CustomerOrderDraft,
    build_customerorder_payload,
    ensure_customerorder,
    fbo_external_code as ext_order,
    dedup_customerorders_by_external,
)

from app.ms_move import (
    dedup_moves_by_external,
    create_move,
    update_move_positions_only,
    build_move_positions_from_order_positions,
    try_apply_move,
)

from app.ms_demand import (
    dedup_demands_by_external,
    create_demand,
    build_demand_positions_from_order_positions,
    try_apply_demand,
)

# OZON numeric states
READY_TO_SUPPLY = 2
ACCEPTED_AT_SUPPLY_WAREHOUSE = 3
IN_TRANSIT = 4
ACCEPTANCE_AT_STORAGE_WAREHOUSE = 5
COMPLETED = 8

# в синхронизацию: + COMPLETED
SYNC_STATES = (READY_TO_SUPPLY, ACCEPTED_AT_SUPPLY_WAREHOUSE, IN_TRANSIT, ACCEPTANCE_AT_STORAGE_WAREHOUSE, COMPLETED)

# Отгрузки создаем только для этих статусов
DEMAND_OZON_STATES = {IN_TRANSIT, ACCEPTANCE_AT_STORAGE_WAREHOUSE, ACCEPTED_AT_SUPPLY_WAREHOUSE}

# move
MOVE_STATE_ID = "b0d2c89d-5c7c-11ef-0a80-0cd4001f5885"
MOVE_SOURCE_STORE_ID = "7cdb9b20-9910-11ec-0a80-08670002d998"

# demand state (как ты дал)
DEMAND_STATE_ID = "b543e330-44e4-11f0-0a80-0da5002260ab"

# fallback store (FBO), если вдруг в заказе не проставится store
FBO_STORE_ID_FALLBACK = "77b4a517-3b82-11f0-0a80-18cb00037a24"


def _dt_from_cfg(d: date | None) -> datetime | None:
    if not d:
        return None
    return datetime(d.year, d.month, d.day, 0, 0, 0, tzinfo=timezone.utc)


def _fmt_ms_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def _ext_move(order_id: int) -> str:
    return f"OZON_FBO_MOVE:{order_id}"


def _ext_demand(order_id: int) -> str:
    return f"OZON_FBO_DEMAND:{order_id}"


def _ms_meta_from_row_assortment(a: Dict[str, Any]) -> Dict[str, Any]:
    # a приходит как {"href":..., "type":..., "mediaType":..., ...}
    href = a.get("href")
    t = a.get("type")
    media = a.get("mediaType") or "application/json"
    if not href or not t:
        return {}
    return {"href": href, "type": t, "mediaType": media}


def _expand_positions_with_bundles(
    ms: MoySkladClient,
    oz: OzonFboClient,
    items: List[Tuple[str, float]],
) -> List[Dict[str, Any]]:
    """
    items: [(offer_id/article, qty)]
    Возвращает позиции для МС CustomerOrder, где bundle развёрнут в компоненты.
    """
    out: List[Dict[str, Any]] = []

    for article, qty in items:
        ass = ms.find_assortment_by_article(article)
        if not ass:
            print({"action": "skip_article_not_found_in_ms", "article": article})
            continue

        t = (ass.get("meta") or {}).get("type")
        if t != "bundle":
            # обычный товар
            price = 0
            try:
                full = ms.get_by_href((ass.get("meta") or {}).get("href"))
                price = ms.get_sale_price(full)
            except Exception:
                price = 0

            out.append(
                {
                    "assortment": {"meta": (ass.get("meta") or {})},
                    "quantity": float(qty),
                    "price": int(price or 0),
                }
            )
            continue

        # bundle -> components
        bundle_id = ass.get("id")
        if not bundle_id:
            print({"action": "skip_bundle_no_id", "article": article})
            continue

        rows = ms.get_bundle_components(bundle_id)
        if not rows:
            print({"action": "skip_bundle_no_components", "article": article})
            continue

        for r in rows:
            comp_ass = r.get("assortment") or {}
            comp_meta = _ms_meta_from_row_assortment(comp_ass)
            if not comp_meta:
                continue

            comp_qty = float(r.get("quantity") or 0)
            final_qty = float(qty) * comp_qty
            if final_qty <= 0:
                continue

            price = 0
            try:
                full = ms.get_by_href(comp_meta["href"])
                price = ms.get_sale_price(full)
            except Exception:
                price = 0

            out.append(
                {
                    "assortment": {"meta": comp_meta},
                    "quantity": float(final_qty),
                    "price": int(price or 0),
                }
            )

    return out


def sync() -> int:
    cfg = load_config()
    dry_run = cfg.fbo_dry_run

    ms = MoySkladClient(cfg.moysklad_token)

    planned_from_dt = _dt_from_cfg(cfg.fbo_planned_from)

    processed = 0

    for cab in cfg.cabinets:
        oz = OzonFboClient(cab.client_id, cab.api_key)

        for state in SYNC_STATES:
            for order_id in oz.iter_supply_order_ids(state=state, limit=100):
                if order_id in cfg.fbo_exclude_order_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                d = oz.get_supply_orders([order_id])["orders"][0]

                # отмененные ничего не делаем
                if (d.get("state") or "").upper() == "CANCELLED":
                    print({"action": "skip_cancelled_supply", "order_id": order_id})
                    continue

                order_number = d["order_number"]

                # плановая дата/таймслот
                shipment_dt: datetime | None = None
                try:
                    # в твоих логах slot лежит как planned_delivery_moment / delivery_planned_moment — бывает по-разному
                    # делаем максимально безопасно:
                    for key in ("shipment_planned_at", "planned_delivery_moment", "delivery_planned_moment", "planned_delivery_date"):
                        if d.get(key):
                            # если это строка ISO
                            if isinstance(d[key], str):
                                shipment_dt = datetime.fromisoformat(d[key].replace("Z", "+00:00")).astimezone(timezone.utc)
                            break
                except Exception:
                    shipment_dt = None

                if planned_from_dt and shipment_dt and shipment_dt < planned_from_dt:
                    print({"action": "skip_before_planned_from", "order_number": order_number, "order_id": order_id})
                    continue

                warehouse_name = ""
                try:
                    # часто в supplies[0] есть warehouse_name / warehouse
                    sup0 = (d.get("supplies") or [{}])[0]
                    warehouse_name = sup0.get("warehouse_name") or sup0.get("warehouse") or ""
                except Exception:
                    warehouse_name = ""

                comment = f"{order_number}" + (f" - {warehouse_name}" if warehouse_name else "")

                # ----------- OZON items -----------
                bundle_id = (d.get("supplies") or [{}])[0].get("bundle_id")
                items: List[Tuple[str, float]] = []

                if bundle_id:
                    bundle = oz.get_bundle_items([bundle_id], limit=100)
                    for it in (bundle.get("items") or []):
                        offer_id = str(it.get("offer_id") or "").strip()
                        q = float(it.get("quantity") or 0)
                        if offer_id and q > 0:
                            items.append((offer_id, q))
                else:
                    # fallback: если вдруг нет bundle_id
                    for it in (d.get("items") or []):
                        offer_id = str(it.get("offer_id") or "").strip()
                        q = float(it.get("quantity") or 0)
                        if offer_id and q > 0:
                            items.append((offer_id, q))

                if not items:
                    print({"action": "skip_no_items", "order_number": order_number, "order_id": order_id})
                    continue

                positions = _expand_positions_with_bundles(ms, oz, items)
                if not positions:
                    print({"action": "skip_no_positions_after_expand", "order_number": order_number, "order_id": order_id})
                    continue

                # ----------- CUSTOMER ORDER (MS) -----------
                draft = CustomerOrderDraft(
                    name=order_number,
                    organization_id=cfg.ms_organization_id,
                    agent_id=cfg.ms_agent_id,
                    state_id=cfg.ms_state_fbo_id,
                    saleschannel_id=cab.ms_saleschannel_id,
                    shipment_planned_at=shipment_dt,
                    description=comment,
                    store_id=FBO_STORE_ID_FALLBACK,  # FBO
                )
                payload = build_customerorder_payload(draft, positions)

                result = ensure_customerorder(ms, payload, dry_run=dry_run)
                print(result)

                # Чтобы дальше не падать в DRY_RUN (где может не быть id):
                ext = payload["externalCode"]
                keep_order = dedup_customerorders_by_external(ms, ext, dry_run=True)  # только читаем/печатаем
                order_ms_id = (keep_order or {}).get("id") or "DRY_RUN_NEW"

                # store_id для move/demand: “склад тот же что в Заказе покупателя (FBO)”
                target_store_id = FBO_STORE_ID_FALLBACK

                # ----------- MOVE -----------
                external_mv = _ext_move(order_id)
                keep_move = dedup_moves_by_external(ms, external_mv, dry_run=dry_run)

                move_positions = build_move_positions_from_order_positions(payload["positions"])

                if dry_run:
                    if keep_move:
                        print({"action": "dry_run_move_update", "id": keep_move["id"], "externalCode": external_mv, "positions": len(move_positions)})
                        move_id = keep_move["id"]
                    else:
                        print({"action": "dry_run_move_create", "externalCode": external_mv, "positions": len(move_positions)})
                        move_id = "DRY_RUN_NEW"
                else:
                    if not keep_move:
                        mv = create_move(
                            ms,
                            name=order_number,
                            external_code=external_mv,
                            organization_id=cfg.ms_organization_id,
                            state_id=MOVE_STATE_ID,
                            source_store_id=MOVE_SOURCE_STORE_ID,
                            target_store_id=target_store_id,
                            description=comment,
                            customerorder_id=order_ms_id,
                            positions=move_positions,
                        )
                        move_id = mv["id"]
                        print({"action": "move_created", "id": move_id, "name": order_number})
                    else:
                        move_id = keep_move["id"]
                        update_move_positions_only(ms, move_id, move_positions)
                        print({"action": "move_updated", "id": move_id, "name": order_number})

                    print(try_apply_move(ms, move_id))

                # ----------- DEMAND (Отгрузка) -----------
                if state in DEMAND_OZON_STATES:
                    external_dem = _ext_demand(order_id)

                    existing_dem = dedup_demands_by_external(ms, external_dem, dry_run=dry_run)
                    if existing_dem:
                        # правило: если уже есть отгрузка — НЕ обновляем
                        print({"action": "skip_demand_exists", "id": existing_dem["id"], "externalCode": external_dem})
                    else:
                        demand_positions = build_demand_positions_from_order_positions(payload["positions"])

                        if dry_run:
                            print({"action": "dry_run_demand_create", "order_number": order_number, "externalCode": external_dem, "positions": len(demand_positions)})
                        else:
                            dem = create_demand(
                                ms,
                                name=order_number,
                                external_code=external_dem,
                                organization_id=cfg.ms_organization_id,
                                agent_id=cfg.ms_agent_id,
                                store_id=target_store_id,
                                state_id=DEMAND_STATE_ID,
                                description=comment,
                                customerorder_id=order_ms_id,
                                positions=demand_positions,
                            )
                            demand_id = dem["id"]
                            print({"action": "demand_created", "id": demand_id, "name": order_number})

                            ok = try_apply_demand(ms, demand_id)
                            if ok:
                                print({"action": "demand_applied", "id": demand_id})
                            else:
                                print({"action": "demand_left_unapplied", "id": demand_id})

                processed += 1

    print({"action": "sync_done", "processed": processed})
    return processed


if __name__ == "__main__":
    sync()
