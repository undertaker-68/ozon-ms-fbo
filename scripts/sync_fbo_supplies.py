from __future__ import annotations

from datetime import datetime
from typing import Dict, Any, List

from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient
from app.ms_move import (
    find_move_by_name,
    create_move,
    update_move_positions_only,
    build_move_positions_from_order_positions,
    try_apply_move,
)

from app.ms_customerorder import (
    CustomerOrderDraft,
    build_customerorder_payload,
    ensure_customerorder,
)

READY_TO_SUPPLY = 2
DATA_FILLING = 3

ORGANIZATION_ID = "12d36dcd-8b6c-11e9-9109-f8fc00176e21"
STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"
AGENT_ID = "f61bfcf9-2d74-11ec-0a80-04c700041e03"
STATE_ID = "921c872f-d54e-11ef-0a80-1823001350aa"

MOVE_STATE_ID = "b0d2c89d-5c7c-11ef-0a80-0cd4001f5885"
MOVE_SOURCE_STORE_ID = "7cdb9b20-9910-11ec-0a80-08670002d998"
MOVE_TARGET_STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"

SALES_CHANNEL_BY_CABINET = {
    0: "fede2826-9fd0-11ee-0a80-0641000f3d25",
    1: "ff2827b8-9fd0-11ee-0a80-0641000f3d31",
}


def _ms_ref_entity(entity: str, id_: str) -> Dict[str, Any]:
    return {
        "meta": {
            "href": f"https://api.moysklad.ru/api/remap/1.2/entity/{entity}/{id_}",
            "type": entity,
            "mediaType": "application/json",
        }
    }


def _get_bundle_by_article(ms: MoySkladClient, article: str):
    # ВАЖНО: bundle в МС лежит не в /product, а в /bundle
    res = ms.get("/entity/bundle", params={"filter": f"article={article}", "limit": 1})
    rows = res.get("rows") or []
    return rows[0] if rows else None


def _get_product_by_meta(ms: MoySkladClient, meta: dict) -> dict | None:
    # meta -> href -> GET полной карточки (чтобы достать salePrices)
    href = ((meta or {}).get("meta") or {}).get("href") if "meta" in meta else (meta or {}).get("href")
    if not href:
        return None
    # href полный, но наш client работает от base_url + path → вырежем /api/remap/1.2
    marker = "/api/remap/1.2"
    if marker in href:
        path = href.split(marker, 1)[1]
    else:
        # fallback: попробуем как есть (редко нужно)
        path = href
    return ms.get(path)


def _positions_from_bundle_item(ms: MoySkladClient, offer_id: str, qty: float) -> List[Dict[str, Any]]:
    """
    Возвращает список позиций МС для одного offer_id из Озон:
    - если это bundle в МС -> раскладываем на компоненты
    - если это product -> одна позиция
    - если не найдено -> []
    """
    # 1) пробуем как bundle
    b = _get_bundle_by_article(ms, offer_id)
    if b:
        full = ms.get(f"/entity/bundle/{b['id']}")
        comps = ((full.get("components") or {}).get("rows")) or []
        out: List[Dict[str, Any]] = []
        for c in comps:
            comp_qty = float(c.get("quantity") or 0)
            ass = c.get("assortment") or {}
            # тянем полную карточку компонента, чтобы взять salePrices
            ass_full = _get_product_by_meta(ms, ass)
            if not ass_full:
                continue
            price = ms.get_sale_price(ass_full)
            assortment = ass if "meta" in ass else {"meta": ass}
            out.append(
                {
                    "assortment": assortment,  # meta компонента
                    "quantity": qty * comp_qty,
                    "price": price,
                }
            )
        return out

    # 2) пробуем как product
    p = ms.find_product_by_article(offer_id)
    if not p:
        return []

    price = ms.get_sale_price(p)
    return [
        {
            "assortment": {"meta": p["meta"]},
            "quantity": qty,
            "price": price,
        }
    ]


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    for idx, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel = SALES_CHANNEL_BY_CABINET.get(idx) or cab.ms_saleschannel_id

        for state in (READY_TO_SUPPLY, DATA_FILLING):
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
                detail = oz.get_supply_orders([order_id])["orders"][0]
                order_number = detail["order_number"]

                # Если уже есть Demand — ничего не делаем
                if ms.has_demand(order_number):
                    continue

                # Плановая дата отгрузки (таймслот)
                timeslot_from = detail["timeslot"]["timeslot"]["from"]
                shipment_date = datetime.fromisoformat(timeslot_from.replace("Z", "+00:00"))

                supply = detail["supplies"][0]
                bundle_id = supply["bundle_id"]
                warehouse_name = supply["storage_warehouse"]["name"]

                # Фильтр по дате (если задан)
                if cfg.fbo_planned_from and shipment_date.date() < cfg.fbo_planned_from:
                    continue

                bundle = oz.post(
                    "/v1/supply-order/bundle",
                    {"bundle_ids": [bundle_id], "limit": 100},
                )

                positions: List[Dict[str, Any]] = []
                for item in bundle.get("items") or []:
                    offer_id = item["offer_id"]
                    qty = float(item["quantity"])
                    positions.extend(_positions_from_bundle_item(ms, offer_id, qty))

                if not positions:
                    continue

                draft = CustomerOrderDraft(
                    name=order_number,
                    organization_id=ORGANIZATION_ID,
                    agent_id=AGENT_ID,
                    state_id=STATE_ID,
                    saleschannel_id=sales_channel,
                    shipment_planned_at=shipment_date,
                    description=f"{order_number} - {warehouse_name}",
                )

                payload = build_customerorder_payload(draft, positions)

                # В МС ещё нужен склад: добавим прямо в payload
                payload["store"] = _ms_ref_entity("store", STORE_ID)

                result = ensure_customerorder(ms, payload, dry_run=cfg.fbo_dry_run)
                print(result)

                # --- MOVE: создать/обновить перемещение сразу после заказа ---
                move_name = order_number  # номер любой, но для дедупликации удобно = номер поставки/заказа
                move_desc = payload.get("description") or f"{order_number}"

                move_positions = build_move_positions_from_order_positions(payload["positions"])

                existing_move = find_move_by_name(ms, move_name)

                if cfg.fbo_dry_run:
                    if not existing_move:
                        print({"action": "dry_run_move_create", "name": move_name, "positions": len(move_positions)})
                    else:
                        print({"action": "dry_run_move_update", "id": existing_move["id"], "positions": len(move_positions)})
                else:
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
                        print({"action": "move_created", "id": move_id, "name": move_name})
                    else:
                        move_id = existing_move["id"]
                        update_move_positions_only(ms, move_id, positions=move_positions, description=move_desc)
                        print({"action": "move_updated", "id": move_id, "name": move_name})

                    # привязываем перемещение к заказу (Связанные документы)
                    order_patch = {
                        "documents": [
                            {"meta": {"href": f"https://api.moysklad.ru/api/remap/1.2/entity/move/{move_id}",
                                      "type": "move",
                                      "mediaType": "application/json"}}
                        ]
                    }
ms.put(f"/entity/customerorder/{result['id']}", order_patch)
print({"action": "order_linked_to_move", "order_id": result["id"], "move_id": move_id})


                    # пытаемся провести
                    r = try_apply_move(ms, move_id)
                    if r.get("applied"):
                        print({"action": "move_applied", "id": move_id})
                    else:
                        print({"action": "move_left_unapplied", "id": move_id, "reason": r.get("reason")})

if __name__ == "__main__":
    sync()
