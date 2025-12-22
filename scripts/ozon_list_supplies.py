from __future__ import annotations

from datetime import datetime, date
from typing import Optional, List

from app.config import load_config
from app.ozon_fbo import OzonFboClient


STATES: List[str] = [
    "Заполнение данных",
    "Готово к отгрузке",
    "В пути",
    "Приемка на складе",
    "На точке отгрузки",
    "Отменено",
]


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def main() -> None:
    cfg = load_config()
    planned_from: Optional[date] = cfg.fbo_planned_from
    print(f"planned_from={planned_from}")

    for cab in cfg.cabinets:
        print(f"\n=== CABINET {cab.name} (client_id={cab.client_id}) ===")

        oz = OzonFboClient(cab.client_id, cab.api_key)

        offset = 0
        limit = 50
        shown = 0

        while True:
            data = oz.list_supplies(states=STATES, limit=limit, offset=offset)

            result = data.get("result") or {}
            supplies = result.get("supply_order") or result.get("rows") or result.get("supplies") or []

            if not supplies:
                break

            for s in supplies:
                supply_id = s.get("supply_order_id") or s.get("id")
                number = s.get("supply_order_number") or s.get("number") or s.get("posting_number")
                state = s.get("state") or s.get("status")

                slot_raw = (
                    s.get("shipment_timeslot")
                    or s.get("timeslot")
                    or s.get("planned_date")
                    or s.get("shipment_date")
                    or s.get("created_at")
                )

                dt = parse_dt(slot_raw)
                d = dt.date() if dt else None

                if planned_from and d and d < planned_from:
                    continue

                receiver = (
                    s.get("warehouse_name")
                    or s.get("destination_warehouse_name")
                    or s.get("warehouse")
                    or ""
                )

                print(
                    f"- id={supply_id} "
                    f"number={number} "
                    f"state={state} "
                    f"slot={slot_raw} "
                    f"receiver={receiver}"
                )
                shown += 1

            offset += limit

        print("shown:", shown)


if __name__ == "__main__":
    main()
