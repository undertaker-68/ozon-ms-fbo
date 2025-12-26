from datetime import datetime, timezone

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import ensure_customerorder, fbo_external_code as fbo_ext_order
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

# =========================
# OZON STATES (numeric)
# =========================
READY_TO_SUPPLY = 2
ACCEPTED_AT_SUPPLY_WAREHOUSE = 3
IN_TRANSIT = 4
ACCEPTANCE_AT_STORAGE_WAREHOUSE = 5
COMPLETED = 8
CANCELLED = 10

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

# =========================
# MOYSKLAD CONSTANTS
# =========================
ORGANIZATION_ID = "12d36dcd-8b6c-11e9-9109-f8fc00176e21"
STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"  # FBO склад
AGENT_ID = "f61bfcf9-2d74-11ec-0a80-04c700041e03"

ORDER_STATE_ID = "921c872f-d54e-11ef-0a80-1823001350aa"

MOVE_STATE_ID = "b0d2c89d-5c7c-11ef-0a80-0cd4001f5885"
MOVE_SOURCE_STORE_ID = "7cdb9b20-9910-11ec-0a80-08670002d998"
MOVE_TARGET_STORE_ID = STORE_ID

DEMAND_STATE_ID = "b543e330-44e4-11f0-0a80-0da5002260ab"

SALES_CHANNEL_BY_CABINET = {
    0: "fede2826-9fd0-11ee-0a80-0641000f3d25",
    1: "ff2827b8-9fd0-11ee-0a80-0641000f3d31",
}


def _parse_ozon_timeslot_from(detail: dict) -> datetime:
    # from всегда в ISO, бывает Z
    ts = detail["timeslot"]["timeslot"]["from"]
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    for cab_index, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel = SALES_CHANNEL_BY_CABINET[cab_index]

        for state in SYNC_STATES:
            if state == CANCELLED:
                continue  # отменённые не трогаем вообще

            resp = oz.post(
                "/v3/supply-order/list",
                {
                    "filter": {"states": [state], "from_supply_order_id": 0},
                    "limit": 100,
                    "sort_by": 1,
                    "sort_dir": "DESC",
                },
            )

            for order_id in resp.get("order_ids", []):
                if order_id in cfg.fbo_exclude_order_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = str(detail["order_number"])  # имя всегда голое число

                shipment_dt = _parse_ozon_timeslot_from(detail)
                if shipment_dt.date() < cfg.fbo_planned_from:
                    continue

                # Проверка на наличие bundle_id
                supply = (detail.get("supplies") or [None])[0]
                if not supply:
                    continue

                bundle_id = supply.get("bundle_id")
                if not bundle_id:
                    print({"action": "skip_no_bundle_id", "order": order_number})
                    continue  # Пропускаем поставку без bundle_id

                # Получаем информацию о комплекте
                bundle = oz.post(
                    "/v1/supply-order/bundle",
                    {"bundle_ids": [bundle_id], "limit": 100},
                )

                positions = []
                for item in (bundle.get("items") or []):
                    article = str(item["offer_id"])
                    qty = float(item["quantity"])
                    if qty <= 0:
                        continue

                    # Найдем товар в МС (product или variant или bundle)
                    product = ms.find_assortment_by_article(article)
                    if not product:
                        print({"action": "skip_no_assortment", "article": article, "order": order_number})
                        continue

                    ass_type = product["meta"]["type"]

                    if ass_type == "bundle":
                        # Разворачиваем комплект на компоненты
                        components = ms.get_bundle_components(product["id"])
                        if not components:
                            print({"action": "skip_bundle_no_components", "article": article, "order": order_number})
                            continue

                        for component in components:
                            positions.append({
                                "assortment": {"meta": component["assortment"]},
                                "quantity": qty * float(component["quantity"]),
                                "price": ms.get_sale_price(component["assortment"]),
                            })
                    else:
                        positions.append({
                            "assortment": {"meta": product["meta"]},
                            "quantity": qty,
                            "price": ms.get_sale_price(product),
                        })

                if not positions:
                    continue

                # -------- CustomerOrder (key = externalCode) --------
                order_payload = {
                    "name": order_number,
                    "externalCode": fbo_ext_order(order_number),
                    "organization": {"meta": {"href": f"{ms.base_url}/entity/organization/{ORGANIZATION_ID}", "type": "organization", "mediaType": "application/json"}},
                    "agent": {"meta": {"href": f"{ms.base_url}/entity/counterparty/{AGENT_ID}", "type": "counterparty", "mediaType": "application/json"}},
                    "state": {"meta": {"href": f"{ms.base_url}/entity/state/{ORDER_STATE_ID}", "type": "state", "mediaType": "application/json"}},
                    "salesChannel": {"meta": {"href": f"{ms.base_url}/entity/saleschannel/{sales_channel}", "type": "saleschannel", "mediaType": "application/json"}},
                    "description": f"{order_number} - {supply.get('storage_warehouse', {}).get('name', 'Unknown Warehouse')}",
                    "positions": positions,
                    "deliveryPlannedMoment": shipment_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "store": {"meta": {"href": f"{ms.base_url}/entity/store/{STORE_ID}", "type": "store", "mediaType": "application/json"}},
                }

                result = ensure_customerorder(ms, order_payload, dry_run=cfg.fbo_dry_run)
                print(result)

                # Получим актуальный заказ (по externalCode) — для id
                # (в dry-run id может не быть, но для backfill в реале будет)
                order_rows = ms.get("/entity/customerorder", params={"filter": f"externalCode={fbo_ext_order(order_number)}", "limit": 1}).get("rows") or []
                if not order_rows:
                    # dry-run — дальше смысла нет
                    continue
                order_obj = order_rows[0]
                order_id_ms = order_obj["id"]

                # -------- Demand exists? -> НЕ обновляем дальше (как правило) --------
                demand_existing = dedup_demands_by_external(ms, fbo_ext_order(order_number), dry_run=cfg.fbo_dry_run)

                if demand_existing:
                    # если уже есть отгрузка — не трогаем ни заказ, ни move/demand
                    print({"action": "skip_has_demand", "name": order_number, "demand_id": demand_existing["id"]})
                    continue

                # -------- Move (always, 1:1) --------
                move_existing = dedup_moves_by_external(ms, fbo_ext_order(order_number), dry_run=cfg.fbo_dry_run)
                move_positions = build_move_positions_from_order_positions(order_payload["positions"])

                if not move_existing:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_create", "name": order_number, "positions": len(move_positions)})
                        move_obj = None
                    else:
                        mv = create_move(
                            ms,
                            name=order_number,
                            external_code=fbo_ext_order(order_number),
                            organization_id=ORGANIZATION_ID,
                            state_id=MOVE_STATE_ID,
                            source_store_id=MOVE_SOURCE_STORE_ID,
                            target_store_id=MOVE_TARGET_STORE_ID,
                            description=order_payload["description"],
                            customerorder_id=order_id_ms,
                            positions=move_positions,
                        )
                        print({"action": "move_created", "id": mv.get("id"), "name": order_number})
                        move_obj = mv
                else:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_update", "id": move_existing["id"]})
                        move_obj = move_existing
                    else:
                        update_move_positions_only(ms, move_existing["id"], move_positions)
                        print({"action": "move_updated", "id": move_existing["id"], "name": order_number})
                        move_obj = move_existing

                # пробуем провести move
                if (not cfg.fbo_dry_run) and move_obj and move_obj.get("id"):
                    print(try_apply_move(ms, move_obj["id"]))

                # -------- Demand (only for specific ozon states) --------
                if state not in DEMAND_OZON_STATES:
                    continue

                demand_positions = build_demand_positions_from_order_positions(order_payload["positions"])

                if cfg.fbo_dry_run:
                    print({"action": "dry_run_demand_create", "name": order_number, "positions": len(demand_positions)})
                    continue

                dm = create_demand(
                    ms,
                    name=order_number,
                    external_code=fbo_ext_order(order_number),
                    organization_id=ORGANIZATION_ID,
                    agent_id=AGENT_ID,
                    state_id=DEMAND_STATE_ID,
                    store_id=STORE_ID,
                    description=order_payload["description"],
                    customerorder_id=order_id_ms,
                    positions=demand_positions,
                )
                print({"action": "demand_created", "id": dm.get("id"), "name": order_number})
                if dm.get("id"):
                    print(try_apply_demand(ms, dm["id"]))


if __name__ == "__main__":
    sync()
