from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, List, Tuple

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import ensure_customerorder, fbo_external_code
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
CANCELLED = 10  # ничего не делаем

SYNC_STATES = (
    READY_TO_SUPPLY,
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
)

DEMAND_OZON_STATES = {
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    COMPLETED,
}

# MS constants
ORGANIZATION_ID = "12d36dcd-8b6c-11e9-9109-f8fc00176e21"
STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"
AGENT_ID = "f61bfcf9-2d74-11ec-0a80-04c700041e03"

ORDER_STATE_ID = "921c872f-d54e-11ef-0a80-1823001350aa"

MOVE_STATE_ID = "b0d2c89d-5c7c-11ef-0a80-0cd4001f5885"
MOVE_SOURCE_STORE_ID = "7cdb9b20-9910-11ec-0a80-08670002d998"
MOVE_TARGET_STORE_ID = STORE_ID

DEMAND_STATE_ID = "b543e330-44e4-11f0-0a80-0da5002260ab"


def parse_ozon_timeslot_from(detail: dict) -> datetime:
    ts = detail["timeslot"]["timeslot"]["from"]
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def ms_price_for_assortment(ms: MoySkladClient, ass: dict) -> int:
    """
    Берём salePrice по объекту ассортимента.
    Если это short объект (без salePrices) — догружаем по href.
    """
    if (ass.get("salePrices") or []) and any((p.get("value") or 0) for p in (ass.get("salePrices") or [])):
        return ms.get_sale_price(ass)
    href = ((ass.get("meta") or {}).get("href")) if isinstance(ass.get("meta"), dict) else None
    if href:
        full = ms.get_by_href(href)
        return ms.get_sale_price(full)
    return 0


def expand_bundle_positions(
    ms: MoySkladClient,
    bundle_ass: dict,
    bundle_qty: float,
) -> List[Dict]:
    """
    Разворачиваем bundle -> компоненты.
    ВАЖНО: если по какой-то причине компоненты не получить — возвращаем позицию самого bundle (fallback),
    чтобы поставка НЕ пропускалась.
    """
    bundle_id = bundle_ass["id"]
    try:
        rows = ms.get_bundle_components(bundle_id)
    except Exception as e:
        print({"action": "bundle_expansion_failed", "bundle_id": bundle_id, "error": str(e)[:300]})
        rows = []

    if not rows:
        # fallback: оставляем сам комплект
        price = ms_price_for_assortment(ms, bundle_ass)
        return [
            {
                "assortment": {"meta": bundle_ass["meta"]},
                "quantity": float(bundle_qty),
                "price": int(price),
            }
        ]

    out: List[Dict] = []
    for r in rows:
        comp_qty = float(r.get("quantity") or 0)
        a = r.get("assortment") or {}
        meta = a.get("meta") or {}
        href = meta.get("href")
        if not href:
            continue
        comp_full = ms.get_by_href(href)
        price = ms.get_sale_price(comp_full)
        out.append(
            {
                "assortment": {"meta": comp_full["meta"]},
                "quantity": float(bundle_qty) * comp_qty,
                "price": int(price),
            }
        )
    if not out:
        # fallback если вдруг всё было кривое
        price = ms_price_for_assortment(ms, bundle_ass)
        return [
            {
                "assortment": {"meta": bundle_ass["meta"]},
                "quantity": float(bundle_qty),
                "price": int(price),
            }
        ]
    return out


def build_order_positions_from_ozon_items(ms: MoySkladClient, items: List[Tuple[str, float]]) -> List[Dict]:
    """
    items: [(offer_id/article, qty), ...]
    Для каждого offer_id:
      - ищем ассортимент в МС по article
      - если bundle -> разворачиваем на компоненты
      - иначе добавляем как есть
    """
    positions: List[Dict] = []
    for article, qty in items:
        ass = ms.find_assortment_by_article(article)
        if not ass:
            print({"action": "ms_assortment_not_found", "article": article})
            continue

        t = (ass.get("meta") or {}).get("type")
        if t == "bundle":
            expanded = expand_bundle_positions(ms, ass, float(qty))
            positions.extend(expanded)
        else:
            price = ms_price_for_assortment(ms, ass)
            positions.append(
                {
                    "assortment": {"meta": ass["meta"]},
                    "quantity": float(qty),
                    "price": int(price),
                }
            )
    return positions


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    planned_from = cfg.fbo_planned_from  # date | None
    exclude_ids = cfg.fbo_exclude_order_ids or set()

    total_seen = 0
    total_synced = 0

    for cab_index, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel_id = cab.ms_saleschannel_id

        for state in SYNC_STATES:
            if state == CANCELLED:
                continue

            for order_id in oz.iter_supply_order_ids(state=state, limit=100, sort_by=None, sort_dir=None):
                total_seen += 1

                if order_id in exclude_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = str(detail["order_number"])
                ext = fbo_external_code(order_number)

                # timeslot filter
                shipment_dt = parse_ozon_timeslot_from(detail)
                if planned_from is not None:
                    # planned_from в конфиге как date
                    if shipment_dt.date() < planned_from:
                        print({"action": "skip_before_planned_from", "order": order_number, "ts": shipment_dt.isoformat()})
                        continue

                # Ozon bundle items (offer_id, qty)
                bundle_id = detail["supplies"][0]["bundle_id"]
                bundle = oz.post(
                    "/v1/supply-order/bundle",
                    {
                        "bundle_ids": [bundle_id],
                        "limit": 100,
                    },
                )
                oz_items = [(str(i.get("offer_id")), float(i.get("quantity") or 0)) for i in (bundle.get("items") or [])]
                oz_items = [(a, q) for (a, q) in oz_items if a and q > 0]

                positions = build_order_positions_from_ozon_items(ms, oz_items)
                if not positions:
                    # не падаем, просто логируем (но поставка реально пустая/не найдены товары)
                    print({"action": "skip_no_positions", "order": order_number, "order_id": order_id})
                    continue

                warehouse_name = (detail.get("warehouse") or {}).get("name") or ""
                comment = f"{order_number} - {warehouse_name}".strip(" -")

                payload = {
                    "name": order_number,
                    "externalCode": ext,
                    "organization": ms.meta("organization", ORGANIZATION_ID),
                    "agent": ms.meta("counterparty", AGENT_ID),
                    "state": ms.meta("state", ORDER_STATE_ID),
                    "salesChannel": ms.meta("saleschannel", sales_channel_id),
                    "description": comment,
                    "deliveryPlannedMoment": shipment_dt.strftime("%Y-%m-%d %H:%M:%S.000"),
                    "store": ms.meta("store", STORE_ID),
                    "positions": positions,
                }

                result = ensure_customerorder(ms, payload, dry_run=cfg.fbo_dry_run)
                order = result.get("order") or result  # совместимость
                order_id_ms = order["id"]

                total_synced += 1

                # ДЕДУП по externalCode (чистим руками/старые дубли)
                if not cfg.fbo_dry_run:
                    dedup_moves_by_external(ms, ext)
                    dedup_demands_by_external(ms, ext)

                # MOVE: 1 заказ = 1 перемещение (обновляем только состав)
                move_positions = build_move_positions_from_order_positions(ms, order)
                if cfg.fbo_dry_run:
                    print({"action": "dry_run_move", "order": order_number, "positions": len(move_positions)})
                else:
                    mv = create_move(
                        ms,
                        name=order_number,
                        external_code=ext,
                        organization_id=ORGANIZATION_ID,
                        source_store_id=MOVE_SOURCE_STORE_ID,
                        target_store_id=MOVE_TARGET_STORE_ID,
                        state_id=MOVE_STATE_ID,
                        description=comment,
                        customerorder_id=order_id_ms,
                        positions=move_positions,
                    )
                    update_move_positions_only(ms, mv["id"], move_positions)

                    r = try_apply_move(ms, mv["id"])
                    if r.get("applied"):
                        print({"action": "move_applied", "id": mv["id"]})
                    else:
                        print({"action": "move_left_unapplied", "id": mv["id"], "reason": r.get("reason")})

                # DEMAND (отгрузка): только для нужных статусов; 1 заказ = 1 отгрузка; если уже есть — не обновляем
                if state in DEMAND_OZON_STATES:
                    demand_positions = build_demand_positions_from_order_positions(ms, order)

                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_demand", "order": order_number, "positions": len(demand_positions)})
                    else:
                        dm = create_demand(
                            ms,
                            name=order_number,
                            external_code=ext,
                            organization_id=ORGANIZATION_ID,
                            agent_id=AGENT_ID,
                            store_id=STORE_ID,
                            state_id=DEMAND_STATE_ID,
                            description=comment,
                            customerorder_id=order_id_ms,
                            positions=demand_positions,
                        )
                        r = try_apply_demand(ms, dm["id"])
                        if r.get("applied"):
                            print({"action": "demand_applied", "id": dm["id"]})
                        else:
                            print({"action": "demand_left_unapplied", "id": dm["id"], "reason": r.get("reason")})

    print(
        {
            "action": "summary",
            "total_seen_order_ids": total_seen,
            "total_synced_orders": total_synced,
            "dry_run": bool(cfg.fbo_dry_run),
        }
    )


if __name__ == "__main__":
    sync()
