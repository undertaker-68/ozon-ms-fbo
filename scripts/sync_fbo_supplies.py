from datetime import datetime, timezone

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient
from app.ms_customerorder import ensure_customerorder

from app.ms_move import (
    find_move_by_name,
    create_move,
    update_move_positions_only,
    build_move_positions_from_order_positions,
    try_apply_move,
    link_move_to_customerorder,
)

from app.ms_demand import (
    find_demand_by_name,
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

SYNC_STATES = (
    READY_TO_SUPPLY,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
)

DEMAND_OZON_STATES = {
    ACCEPTED_AT_SUPPLY_WAREHOUSE,
    IN_TRANSIT,
    ACCEPTANCE_AT_STORAGE_WAREHOUSE,
}

# =========================
# MOYSKLAD CONSTANTS
# =========================
ORGANIZATION_ID = "12d36dcd-8b6c-11e9-9109-f8fc00176e21"
STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"          # FBO склад
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

MS_BASE = "https://api.moysklad.ru/api/remap/1.2"

def ms_ref(entity: str, id_: str):
    return {
        "meta": {
            "href": f"{MS_BASE}/entity/{entity}/{id_}",
            "type": entity,
            "mediaType": "application/json",
        }
    }

def ms_state_ref(entity: str, state_id: str):
    return {
        "meta": {
            "href": f"{MS_BASE}/entity/{entity}/metadata/states/{state_id}",
            "type": "state",
            "mediaType": "application/json",
            "metadataHref": f"{MS_BASE}/entity/{entity}/metadata",
        }
    }

def ms_moment(dt: datetime) -> str:
    # МС ждёт формат вида: '2025-12-25 11:00:00.000' (без timezone)
    dt_utc = dt.astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%d %H:%M:%S.000")

def sync():
    cfg = load_config()
    planned_from = datetime.combine(cfg.fbo_planned_from, datetime.min.time()).replace(tzinfo=timezone.utc)
    ms = MoySkladClient(cfg.moysklad_token)

    for cab_index, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel = SALES_CHANNEL_BY_CABINET[cab_index]

        for state in SYNC_STATES:
            resp = oz.post(
                "/v3/supply-order/list",
                {
                    "filter": {
                        "states": [state],
                        "from_supply_order_id": 0,
                    },
                    "limit": 100,
                    "sort_by": 1,
                    "sort_dir": "DESC",
                },
            )

            for order_id in resp.get("order_ids", []):
                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = detail["order_number"]

                # отменённые поставки полностью игнорируем
                if detail.get("state") == "CANCELLED":
                    continue

                # ---------------------------------
                # ЕСЛИ УЖЕ ЕСТЬ ОТГРУЗКА → НИЧЕГО НЕ ДЕЛАЕМ
                # ---------------------------------
                if find_demand_by_name(ms, order_number):
                    continue

                # ---------------------------------
                # дата отгрузки из таймслота
                # ---------------------------------
                timeslot_from = detail["timeslot"]["timeslot"]["from"]
                shipment_dt = datetime.fromisoformat(timeslot_from.replace("Z", "+00:00"))

                # фильтр по таймслоту (с 22.12.2025 включительно)
                if shipment_dt < planned_from:
                    continue

                supply = detail["supplies"][0]
                bundle_id = supply["bundle_id"]
                warehouse_name = supply["storage_warehouse"]["name"]

                bundle = oz.post(
                    "/v1/supply-order/bundle",
                    {"bundle_ids": [bundle_id], "limit": 100},
                )

                positions = []
                for item in bundle["items"]:
                    article = item["offer_id"]
                    qty = item["quantity"]

                    product = ms.find_product_by_article(article)
                    if not product:
                        continue

                    if product["meta"]["type"] == "bundle":
                        components = ms.get_bundle_components(product["id"])
                        for c in components:
                            positions.append(
                                {
                                    "assortment": {"meta": c["assortment"]},
                                    "quantity": qty * c["quantity"],
                                    "price": ms.get_sale_price(c["assortment"]),
                                }
                            )
                    else:
                        positions.append(
                            {
                                "assortment": {"meta": product["meta"]},
                                "quantity": qty,
                                "price": ms.get_sale_price(product),
                            }
                        )

                if not positions:
                    continue

                payload = {
                    "name": order_number,
                    "organization": ms_ref("organization", ORGANIZATION_ID),
                    "agent": ms_ref("counterparty", AGENT_ID),
                    "state": ms_state_ref("customerorder", ORDER_STATE_ID),
                    "salesChannel": ms_ref("saleschannel", sales_channel),
                    "description": f"{order_number} - {warehouse_name}",
                    "store": ms_ref("store", STORE_ID),
                    "deliveryPlannedMoment": ms_moment(shipment_dt),
                    "positions": positions,
                }

                result = ensure_customerorder(ms, payload, dry_run=cfg.fbo_dry_run)
                print(result)

                move_name = order_number
                move_desc = payload["comment"]
                move_positions = build_move_positions_from_order_positions(positions)

                existing_move = find_move_by_name(ms, move_name)

                if cfg.fbo_dry_run:
                    if not existing_move:
                        print({"action": "dry_run_move_create", "name": move_name})
                    else:
                        print({"action": "dry_run_move_update", "id": existing_move["id"]})

                    if state in DEMAND_OZON_STATES:
                        print({"action": "dry_run_demand_create", "name": order_number})

                else:
                    # -------- MOVE --------
                    if not existing_move:
                        mv = create_move(
                            ms,
                            name=move_name,
                            description=move_desc,
                            organization_id=ORGANIZATION_ID,
                            source_store_id=MOVE_SOURCE_STORE_ID,
                            target_store_id=MOVE_TARGET_STORE_ID,
                            state_id=MOVE_STATE_ID,
                            positions=move_positions,
                        )
                        move_id = mv["id"]
                        print({"action": "move_created", "id": move_id})
                    else:
                        move_id = existing_move["id"]
                        update_move_positions_only(ms, move_id, positions=move_positions, description=move_desc)
                        print({"action": "move_updated", "id": move_id})

                    link_move_to_customerorder(ms, move_id, result["id"])
                    print({"action": "move_linked_to_order", "move_id": move_id})

                    r = try_apply_move(ms, move_id)
                    print({"action": "move_applied" if r.get("applied") else "move_left_unapplied", "id": move_id})

                    # -------- DEMAND --------
                    if state in DEMAND_OZON_STATES:
                        demand_positions = build_demand_positions_from_order_positions(positions)
                        dem = create_demand(
                            ms,
                            name=order_number,
                            description=move_desc,
                            organization_id=ORGANIZATION_ID,
                            agent_id=AGENT_ID,
                            store_id=STORE_ID,
                            state_id=DEMAND_STATE_ID,
                            customerorder_id=result["id"],
                            positions=demand_positions,
                        )
                        demand_id = dem["id"]
                        print({"action": "demand_created", "id": demand_id})

                        r2 = try_apply_demand(ms, demand_id)
                        print({"action": "demand_applied" if r2.get("applied") else "demand_left_unapplied", "id": demand_id})


if __name__ == "__main__":
    sync()
