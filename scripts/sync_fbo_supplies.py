# scripts/sync_fbo_supplies.py

from datetime import datetime
from app.config import load_config
from app.ozon_fbo import OzonFboClient
from app.moysklad import MoySkladClient

READY_TO_SUPPLY = 2
DATA_FILLING = 3

ORGANIZATION_ID = "12d36dcd-8b6c-11e9-9109-f8fc00176e21"
STORE_ID = "77b4a517-3b82-11f0-0a80-18cb00037a24"
AGENT_ID = "f61bfcf9-2d74-11ec-0a80-04c700041e03"
STATE_ID = "921c872f-d54e-11ef-0a80-1823001350aa"

SALES_CHANNEL_BY_CABINET = {
    0: "fede2826-9fd0-11ee-0a80-0641000f3d25",
    1: "ff2827b8-9fd0-11ee-0a80-0641000f3d31",
}


def sync():
    cfg = load_config()
    ms = MoySkladClient(cfg.moysklad_token)

    for idx, cab in enumerate(cfg.cabinets):
        oz = OzonFboClient(cab.client_id, cab.api_key)
        sales_channel = SALES_CHANNEL_BY_CABINET[idx]

        for state in (READY_TO_SUPPLY, DATA_FILLING):
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

                # если есть Demand — пропускаем
                if ms.has_demand(order_number):
                    continue

                timeslot_from = detail["timeslot"]["timeslot"]["from"]
                shipment_date = datetime.fromisoformat(timeslot_from.replace("Z", "+00:00"))

                supply = detail["supplies"][0]
                bundle_id = supply["bundle_id"]
                warehouse_name = supply["storage_warehouse"]["name"]

                bundle = oz.post(
                    "/v1/supply-order/bundle",
                    {
                        "bundle_ids": [bundle_id],
                        "limit": 100,
                    },
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
                            positions.append({
                                "assortment": c["assortment"],
                                "quantity": qty * c["quantity"],
                                "price": ms.get_sale_price(c["assortment"]),
                            })
                    else:
                        positions.append({
                            "assortment": product["meta"],
                            "quantity": qty,
                            "price": ms.get_sale_price(product),
                        })

                if not positions:
                    continue

                ms.create_customerorder(
                    name=order_number,
                    organization=ORGANIZATION_ID,
                    agent=AGENT_ID,
                    store=STORE_ID,
                    sales_channel=sales_channel,
                    state=STATE_ID,
                    shipment_date=shipment_date,
                    comment=f"{order_number} - {warehouse_name}",
                    positions=positions,
                )


if __name__ == "__main__":
    sync()
