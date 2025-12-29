from __future__ import annotations

from datetime import datetime, timezone

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


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    planned_from = cfg.fbo_planned_from  # date | None
    exclude_ids = cfg.fbo_exclude_order_ids or set()

    for cab_index, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel_id = cab.ms_saleschannel_id

        for state in SYNC_STATES:
            if state == CANCELLED:
                continue

            # ВАЖНО: iter_supply_order_ids должен быть в ozon_fbo.py (у тебя уже был/мы раньше делали)
            for order_id in oz.iter_supply_order_ids(state=state, limit=100, sort_by=1, sort_dir="DESC"):
                if order_id in exclude_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = str(detail["order_number"])
                ext = fbo_external_code(order_number)

                shipment_dt = parse_ozon_timeslot_from(detail)
                if planned_from and shipment_dt.date() < planned_from:
                    continue

                supply = (detail.get("supplies") or [None])[0]
                if not supply:
                    continue

                bundle_id = supply.get("bundle_id")
                if not bundle_id:
                    print({"action": "skip_no_bundle_id", "order": order_number})
                    continue

                warehouse_name = (supply.get("storage_warehouse") or {}).get("name") or "Unknown Warehouse"
                description = f"{order_number} - {warehouse_name}"

                # 1) DEDUP demand по external + если есть demand — НЕ ТРОГАЕМ ВООБЩЕ
                d0 = dedup_demands_by_external(ms, ext, dry_run=cfg.fbo_dry_run)
                if d0:
                    print({"action": "skip_has_demand", "name": order_number, "demand_id": d0["id"]})
                    continue

                # 2) Получаем список товаров поставки из Ozon bundle
                oz_bundle = oz.post("/v1/supply-order/bundle", {"bundle_ids": [bundle_id], "limit": 100})
                items = oz_bundle.get("items") or []
                if not items:
                    print({"action": "skip_empty_ozon_bundle", "order": order_number})
                    continue

                # 3) Строим позиции заказа, разворачиваем комплекты МС
                positions = []
                for item in items:
                    article = str(item.get("offer_id") or "")
                    qty = float(item.get("quantity") or 0)
                    if not article or qty <= 0:
                        continue

                    ass = ms.find_assortment_by_article(article)
                    if not ass:
                        print({"action": "skip_no_assortment", "article": article, "order": order_number})
                        continue

                    ass_type = (ass.get("meta") or {}).get("type")

                    # если это комплект МС — разворачиваем в компоненты
                    if ass_type == "bundle":
                        comps = ms.get_bundle_components(ass["id"])
                        if not comps:
                            # если такое будет — это уже не "не умеем", это "в МС реально пустой комплект"
                            print({"action": "bundle_empty_in_ms", "bundle_id": ass["id"], "article": article, "order": order_number})
                            continue

                        for c in comps:
                            c_ass = c.get("assortment") or {}
                            c_meta = c_ass.get("meta") or {}
                            c_qty = float(c.get("quantity") or 0)
                            if not c_meta or c_qty <= 0:
                                continue

                            comp_obj = ms.get_by_href(c_meta["href"])
                            comp_price = ms.get_sale_price(comp_obj)

                            positions.append(
                                {
                                    "assortment": {"meta": c_meta},
                                    "quantity": qty * c_qty,
                                    "price": comp_price,
                                }
                            )
                    else:
                        # обычный товар/вариант
                        ass_full = ms.get_by_href((ass.get("meta") or {}).get("href"))
                        price = ms.get_sale_price(ass_full) if ass_full else ms.get_sale_price(ass)

                        positions.append(
                            {
                                "assortment": {"meta": ass["meta"]},
                                "quantity": qty,
                                "price": price,
                            }
                        )

                if not positions:
                    continue

                # 4) CustomerOrder (всё строго через meta)
                order_payload = {
                    "name": order_number,
                    "externalCode": ext,
                    "organization": ms.meta("organization", ORGANIZATION_ID),
                    "agent": ms.meta("counterparty", AGENT_ID),
                    "state": ms.meta("state", ORDER_STATE_ID),
                    "salesChannel": ms.meta("saleschannel", sales_channel_id),
                    "description": description,
                    "positions": positions,
                    "deliveryPlannedMoment": shipment_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "store": ms.meta("store", STORE_ID),
                }

                print(ensure_customerorder(ms, order_payload, dry_run=cfg.fbo_dry_run))

                # 5) Получаем id заказа (по externalCode железобетонно)
                rows = ms.get("/entity/customerorder", params={"filter": f"externalCode={ext}", "limit": 1}).get("rows") or []
                if not rows:
                    continue
                order_id_ms = rows[0]["id"]

                # 6) Move 1:1 (по externalCode), обновляем только позиции
                mv0 = dedup_moves_by_external(ms, ext, dry_run=cfg.fbo_dry_run)
                move_positions = build_move_positions_from_order_positions(order_payload["positions"])

                if not mv0:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_create", "name": order_number, "positions": len(move_positions)})
                        move_id = None
                    else:
                        mv = create_move(
                            ms,
                            name=order_number,
                            external_code=ext,
                            organization_id=ORGANIZATION_ID,
                            state_id=MOVE_STATE_ID,
                            source_store_id=MOVE_SOURCE_STORE_ID,
                            target_store_id=MOVE_TARGET_STORE_ID,
                            description=description,
                            customerorder_id=order_id_ms,
                            positions=move_positions,
                        )
                        move_id = mv.get("id")
                        print({"action": "move_created", "id": move_id, "name": order_number})
                else:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_update", "id": mv0["id"], "name": order_number})
                        move_id = mv0["id"]
                    else:
                        update_move_positions_only(ms, mv0["id"], move_positions)
                        move_id = mv0["id"]
                        print({"action": "move_updated", "id": move_id, "name": order_number})

                if (not cfg.fbo_dry_run) and move_id:
                    print(try_apply_move(ms, move_id))

                # 7) Demand 1:1 для нужных статусов
                if state not in DEMAND_OZON_STATES:
                    continue

                demand_positions = build_demand_positions_from_order_positions(order_payload["positions"])

                if cfg.fbo_dry_run:
                    print({"action": "dry_run_demand_create", "name": order_number, "positions": len(demand_positions)})
                    continue

                dm = create_demand(
                    ms,
                    name=order_number,
                    external_code=ext,
                    organization_id=ORGANIZATION_ID,
                    agent_id=AGENT_ID,
                    state_id=DEMAND_STATE_ID,
                    store_id=STORE_ID,
                    description=description,
                    customerorder_id=order_id_ms,
                    positions=demand_positions,
                )
                demand_id = dm.get("id")
                print({"action": "demand_created", "id": demand_id, "name": order_number})
                if demand_id:
                    print(try_apply_demand(ms, demand_id))


if __name__ == "__main__":
    sync()
