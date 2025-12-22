from __future__ import annotations

from datetime import datetime, date
from typing import Optional

from app.config import load_config
from app.ozon_fbo import OzonFboClient

ALLOWED_STATUSES = {
    "Заполнение данных",
    "Готово к отгрузке",
    "В пути",
    "Приемка на складе",
    "На точке отгрузки",
    "Отменено",
}


def _parse_dt(s: Optional[str]) -> Optional[datetime]:
    if not s:
        return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    c = load_config()
    planned_from: Optional[date] = c.fbo_planned_from
    print(f"planned_from={planned_from}")

    for cab in c.cabinets:
        print("\n=== CABINET:", cab.name, "client_id=", cab.client_id, "===")
        oz = OzonFboClient(cab.client_id, cab.api_key)

        offset = 0
        limit = 50
        shown = 0

        while True:
	    data = oz.list_supplies(
		    states=[
        		"Заполнение данных",
        		"Готово к отгрузке",
      		  	"В пути",
        		"Приемка на складе",
        		"На точке отгрузки",
        		"Отменено",
    		     ],
    		     limit=limit,
    		     offset=offset,
 	    )

            result = data.get("result") or {}

            supplies = (
                result.get("supply_order")
                or result.get("supplies")
                or result.get("rows")
                or []
            )

            if not supplies:
                break

            for s in supplies:
                supply_id = s.get("supply_order_id") or s.get("id")
                supply_number = s.get("supply_order_number") or s.get("number") or s.get("posting_number")
                status = s.get("status")

                slot = (
                    s.get("timeslot")
                    or s.get("shipment_timeslot")
                    or s.get("shipping_timeslot")
                    or s.get("planned_date")
                    or s.get("shipment_date")
                    or s.get("created_at")
                )
                dt = _parse_dt(slot)
                d = dt.date() if dt else None

                receiver = (
                    s.get("warehouse_name")
                    or s.get("warehouse")
                    or s.get("destination")
                    or s.get("destination_warehouse_name")
                    or ""
                )

                if status and status not in ALLOWED_STATUSES:
                    continue
                if planned_from and d and d < planned_from:
                    continue

                print(f"- id={supply_id} number={supply_number} status={status} slot={slot} receiver={receiver}")
                shown += 1

            offset += limit

        print("shown:", shown)


if __name__ == "__main__":
    main()
