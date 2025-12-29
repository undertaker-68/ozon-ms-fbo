from datetime import datetime, timezone

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

from app.ms_customerorder import ensure_customerorder, fbo_external_code as fbo_ext
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
CANCELLED = 10  # не трогаем

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


def _parse_ozon_timeslot_from(detail: dict) -> datetime:
    ts = detail["timeslot"]["timeslot"]["from"]
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)


def _ms_ref(ms: MoySkladClient, entity: str, id_: str) -> dict:
    return {
        "meta": {
            "href": f"{ms.base_url}/entity/{entity}/{id_}",
            "type": entity,
            "mediaType": "application/json",
        }
    }


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    planned_from = cfg.fbo_planned_from  # date | None
    exclude_ids = cfg.fbo_exclude_order_ids or set()

    for cab_index, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel = cab.ms_saleschannel_id

        for state in SYNC_STATES:
            if state == CANCELLED:
                continue  # отменённые не трогаем

            # ---------- ПАГИНАЦИЯ: берём ВСЕ order_ids ----------
            for order_id in oz.iter_supply_order_ids(state=state, limit=100, sort_by=1, sort_dir="DESC"):
                if order_id in exclude_ids:
                    print({"action": "skip_excluded_order", "order_id": order_id})
                    continue

                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = str(detail["order_number"])
                ext = fbo_ext(order_number)

                shipment_dt = _parse_ozon_timeslot_from(detail)
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

                # ====== (4) Если уже есть Demand — вообще ничего не обновляем ======
                demand_existing = dedup_demands_by_external(ms, ext, dry_run=cfg.fbo_dry_run)
                if demand_existing:
                    print({"action": "skip_has_demand", "name": order_number, "demand_id": demand_existing["id"]})
                    continue

                # ---------- позиции из Ozon bundle ----------
                bundle = oz.post(
                    "/v1/supply-order/bundle",
                    {"bundle_ids": [bundle_id], "limit": 100},
                )

                positions = []
                for item in (bundle.get("items") or []):
                    article = str(item.get("offer_id") or "")
                    qty = float(item.get("quantity") or 0)
                    if not article or qty <= 0:
                        continue

                    ass = ms.find_assortment_by_article(article)
                    if not ass:
                        print({"action": "skip_no_assortment", "article": article, "order": order_number})
                        continue

                    ass_type = (ass.get("meta") or {}).get("type")

                    if ass_type == "bundle":
                        # компоненты комплекта берём так:
                        components = ms.get_bundle_components(ass["id"])
                        if not components:
                            print({"action": "skip_bundle_no_components", "article": article, "order": order_number})
                            continue

                        for c in components:
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
                        positions.append(
                            {
                                "assortment": {"meta": ass["meta"]},
                                "quantity": qty,
                                "price": ms.get_sale_price(ass),
                            }
                        )

                if not positions:
                    continue

                # -------- CustomerOrder (key=externalCode) --------
                order_payload = {
                    "name": order_number,
                    "externalCode": ext,
                    "organization": _ms_ref(ms, "organization", ORGANIZATION_ID),
                    "agent": _ms_ref(ms, "counterparty", AGENT_ID),
                    "state": _ms_ref(ms, "state", ORDER_STATE_ID),
                    "salesChannel": _ms_ref(ms, "saleschannel", sales_channel),
                    "description": description,
                    "positions": positions,
                    "deliveryPlannedMoment": shipment_dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
                    "store": _ms_ref(ms, "store", STORE_ID),
                }

                result = ensure_customerorder(ms, order_payload, dry_run=cfg.fbo_dry_run)
                print(result)

                # получаем id заказа по externalCode (чтобы связать move/demand)
                order_rows = ms.get(
                    "/entity/customerorder",
                    params={"filter": f"externalCode={ext}", "limit": 1},
                ).get("rows") or []

                if not order_rows:
                    continue
                order_id_ms = order_rows[0]["id"]

                # -------- Move (1:1, key=externalCode) --------
                move_existing = dedup_moves_by_external(ms, ext, dry_run=cfg.fbo_dry_run)
                move_positions = build_move_positions_from_order_positions(order_payload["positions"])

                if not move_existing:
                    if cfg.fbo_dry_run:
                        print({"action": "dry_run_move_create", "name": order_number, "positions": len(move_positions)})
                        move_obj = None
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

                if (not cfg.fbo_dry_run) and move_obj and move_obj.get("id"):
                    print(try_apply_move(ms, move_obj["id"]))

                # -------- Demand (1:1, только для нужных статусов) --------
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
                print({"action": "demand_created", "id": dm.get("id"), "name": order_number})
                if dm.get("id"):
                    print(try_apply_demand(ms, dm["id"]))


if __name__ == "__main__":
    sync()
