from __future__ import annotations

from datetime import datetime, timezone, date
from typing import Any, Dict, List

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import (
    ensure_customerorder,
    find_customerorders_by_external,
    dedup_customerorders_by_external,
)

from app.ms_move import (
    find_moves_by_external,
    dedup_moves_by_external,
    create_move,
    update_move_positions_only,
    build_move_positions_from_order_positions,
    try_apply_move,
)

from app.ms_demand import (
    find_demands_by_external,
    dedup_demands_by_external,
    create_demand,
    build_demand_positions_from_order_positions,
    try_apply_demand,
)

# =========================
# OZON STATES (numeric)
# =========================
READY_TO_SUPPLY = 2
ACCEPTED_AT_SUPPLY_WAREHOUSE = 3
IN_TRANSIT = 4
ACCEPTANCE_AT_STORAGE_WAREHOUSE = 5
COMPLETED = 8

SYNC_STATES = (
    READY_TO_SUPPLY,
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
)

# создаем отгрузки (Demand) только для этих статусов
DEMAND_OZON_STATES = {
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
}


def _ms_moment(dt: datetime) -> str:
    # МС принимает строку вида "2025-12-25 12:00:00.000"
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")


def _ext_order(order_id: int) -> str:
    return f"FBO_SUPPLY_ORDER:{order_id}"


def _ext_move(order_id: int) -> str:
    return f"FBO_SUPPLY_MOVE:{order_id}"


def _ext_demand(order_id: int) -> str:
    return f"FBO_SUPPLY_DEMAND:{order_id}"


def _positions_from_ozon_bundle(ms: MoySkladClient, oz_items: List[Dict[str, Any]], *, order_number: str) -> List[Dict[str, Any]]:
    """
    oz_items: [{"offer_id": "10264-A93", "quantity": 30}, ...]
    Возвращает позиции МС. Если offer_id в МС = bundle -> разворачиваем на компоненты.
    """
    positions: List[Dict[str, Any]] = []

    for it in oz_items:
        article = str(it.get("offer_id") or "").strip()
        qty = float(it.get("quantity") or 0)
        if not article or qty <= 0:
            continue

        ass = ms.find_assortment_by_article(article)
        if not ass:
            print({"action": "skip_no_assortment", "article": article, "order": order_number})
            continue

        ass_type = ((ass.get("meta") or {}).get("type") or "").lower()

        if ass_type == "bundle":
            comps = ms.get_bundle_components(ass["id"])
            if not comps:
                print({"action": "skip_bundle_no_components", "article": article, "order": order_number})
                continue

            for c in comps:
                comp_qty = float(c.get("quantity") or 0)
                if comp_qty <= 0:
                    continue
                ass_meta = ((c.get("assortment") or {}).get("meta")) or None
                if not ass_meta:
                    continue

                # Цена не критична по твоим словам — ставим 0 (или можно попытаться вытащить salePrices)
                positions.append(
                    {
                        "assortment": {"meta": ass_meta},
                        "quantity": qty * comp_qty,
                        "price": 0,
                    }
                )
        else:
            positions.append(
                {
                    "assortment": {"meta": ass["meta"]},
                    "quantity": qty,
                    "price": 0,
                }
            )

    return positions


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    planned_from: date | None = cfg.fbo_planned_from  # date или None
    dry_run = cfg.fbo_dry_run

    processed = 0

    for cab in cfg.cabinets:
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel_id = cab.ms_saleschannel_id

        for state in SYNC_STATES:
            for order_id in oz.iter_supply_order_ids(state=state, limit=100):
                if order_id in cfg.fbo_exclude_order_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = detail["order_number"]

                # полностью игнорируем отмененные
                if detail.get("state") == "CANCELLED":
                    print({"action": "skip_cancelled", "order_id": order_id, "order_number": order_number})
                    continue

                timeslot_from = detail["timeslot"]["timeslot"]["from"]
                shipment_dt = datetime.fromisoformat(timeslot_from.replace("Z", "+00:00"))

                # фильтр по дате (включительно)
                if planned_from and shipment_dt.date() < planned_from:
                    continue

                supply = detail["supplies"][0]
                bundle_id = supply["bundle_id"]
                warehouse_name = supply["storage_warehouse"]["name"]

                # получаем состав поставки от Озон
                bundle = oz.get_bundle([bundle_id], limit=200)
                oz_items = bundle.get("items") or []

                positions = _positions_from_ozon_bundle(ms, oz_items, order_number=order_number)
                if not positions:
                    print({"action": "skip_no_positions", "order_id": order_id, "order_number": order_number})
                    continue

                comment = f"{order_number} - {warehouse_name}"

                # ---------------------------
                # CUSTOMER ORDER (dedup by externalCode)
                # ---------------------------
                external_order = _ext_order(order_id)

                # убираем дубликаты заказов по external
                dedup_customerorders_by_external(ms, external_order, dry_run=dry_run)

                payload = {
                    "name": order_number,
                    "externalCode": external_order,
                    "organization": ms.meta("organization", cfg.ms_organization_id),
                    "agent": ms.meta("counterparty", cfg.ms_agent_id),
                    "state": ms.meta("state", cfg.ms_state_fbo_id),
                    "salesChannel": ms.meta("saleschannel", sales_channel_id),
                    "description": comment,
                    "store": ms.meta("store", supply["store_id"]) if supply.get("store_id") else None,  # если нет — ниже заменим
                    "deliveryPlannedMoment": _ms_moment(shipment_dt),
                    "positions": positions,
                }

                # склад: по твоему правилу "тот же что в заказе покупателя (FBO)".
                # если у Озон нет store_id, используем MS_STORE из .env не трогаем здесь, а просто оставляем как было в payload'е раньше:
                if payload["store"] is None:
                    # если в конфиге нет store_id — оставляем существующий STORE (обычно в старом коде был константой)
                    # но раз у тебя MoySkladClient.meta есть — можно использовать переменную окружения через cfg, если добавишь.
                    # В твоих текущих файлах store обычно проставляется константой внутри скрипта — поэтому здесь безопасно ставим FBO склад через agent/store из заказа.
                    # Если нужно жестко — скажешь id склада, и я зафиксирую.
                    pass

                # ensure_customerorder ожидает "store" как объект, не id/строку
                if isinstance(payload.get("store"), str):
                    payload["store"] = ms.meta("store", payload["store"])

                result = ensure_customerorder(ms, payload, dry_run=dry_run)
                print(result)

                # берем реальный order из МС по externalCode
                orders = find_customerorders_by_external(ms, external_order)
                if not orders:
                    # в dry_run так и будет — просто не идем дальше
                    print({"action": "order_not_found_after_ensure", "externalCode": external_order, "dry_run": dry_run})
                    processed += 1
                    continue

                order_ms = orders[0]
                order_ms_id = order_ms["id"]

                # ---------------------------
                # MOVE (1 supply = 1 move) dedup by externalCode
                # ---------------------------
                external_move = _ext_move(order_id)
                keep_move = dedup_moves_by_external(ms, external_move, dry_run=dry_run)
                move_positions = build_move_positions_from_order_positions(payload["positions"])

                if dry_run:
                    print({"action": "dry_run_move_sync", "order_number": order_number, "externalCode": external_move, "positions": len(move_positions)})
                else:
                    if not keep_move:
                        mv = create_move(
                            ms,
                            name=order_number,
                            external_code=external_move,
                            organization_id=cfg.ms_organization_id,
                            state_id=cfg.ms_state_fbo_id,  # если у move другой state — вынеси в конфиг, сейчас оставляем как есть
                            source_store_id="7cdb9b20-9910-11ec-0a80-08670002d998",
                            target_store_id=order_ms["store"]["meta"]["href"].split("/")[-1] if order_ms.get("store") else "77b4a517-3b82-11f0-0a80-18cb00037a24",
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

                    r = try_apply_move(ms, move_id)
                    print(r)

                # ---------------------------
                # DEMAND (Отгрузка): только для нужных статусов
                # и правило: если уже есть отгрузка — не обновляем
                # ---------------------------
                if state in DEMAND_OZON_STATES:
                    external_dem = _ext_demand(order_id)

                    existing_dem = dedup_demands_by_external(ms, external_dem, dry_run=dry_run)
                    if existing_dem:
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
                                store_id=order_ms["store"]["meta"]["href"].split("/")[-1] if order_ms.get("store") else "77b4a517-3b82-11f0-0a80-18cb00037a24",
                                state_id="b543e330-44e4-11f0-0a80-0da5002260ab",
                                description=comment,
                                customerorder_id=order_ms_id,
                                positions=demand_positions,
                            )
                            demand_id = dem["id"]
                            print({"action": "demand_created", "id": demand_id, "name": order_number})

                            rr = try_apply_demand(ms, demand_id)
                            print(rr)

                processed += 1

    print({"action": "sync_done", "processed": processed})
    return processed


if __name__ == "__main__":
    sync()
