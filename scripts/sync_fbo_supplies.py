from __future__ import annotations

from datetime import datetime, date
from typing import Any, Dict, List

from app.config import load_config
from app.ozon_fbo import OzonFboClient


def _parse_date(s: str | None) -> date | None:
    if not s:
        return None
    # Ozon обычно отдаёт ISO datetime, но бывает и дата. Пробуем аккуратно.
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date()
    except Exception:
        try:
            return date.fromisoformat(s[:10])
        except Exception:
            return None


def main() -> None:
    cfg = load_config()
    oz = OzonFboClient(cfg.ozon_client_id, cfg.ozon_api_key)

    planned_from = cfg.fbo_planned_from
    print(f"[FBO] planned_from={planned_from} dry_run={cfg.fbo_dry_run}")

    offset = 0
    limit = 100
    total_seen = 0

    while True:
        data = oz.list_supplies(limit=limit, offset=offset)
        supplies = (data.get("result") or {}).get("supply_order") or (data.get("result") or {}).get("supplies") or []

        if not supplies:
            break

        for s in supplies:
            total_seen += 1
            supply_id = s.get("supply_order_id") or s.get("id")
            status = s.get("status")
            planned_dt = s.get("planned_date") or s.get("delivery_date") or s.get("created_at")
            planned_date = _parse_date(planned_dt)

            if planned_from and planned_date and planned_date < planned_from:
                continue

            print(f"\n[FBO] supply_id={supply_id} status={status} planned={planned_dt}")

            if not supply_id:
                print("[FBO] skip: no supply_id")
                continue

            # details
            details = oz.get_supply(int(supply_id))
            print(f"[FBO] details keys: {list((details.get('result') or {}).keys())}")

            # items
            items_resp = oz.get_supply_items(int(supply_id), limit=1000, offset=0)
            items = (items_resp.get("result") or {}).get("items") or (items_resp.get("result") or {}).get("rows") or []
            print(f"[FBO] items: {len(items)}")

            # Здесь будет твоя бизнес-логика:
            # - сопоставление offer_id/article
            # - создание/обновление документов в МойСклад
            # - dry_run режим
            if cfg.fbo_dry_run:
                print("[FBO] dry_run=1 -> no writes")
            else:
                # TODO: интеграция с МойСклад (создание перемещения/поставки/оприходования и т.п.)
                pass

        offset += limit

    print(f"\n[FBO] done. total_seen={total_seen}")


if __name__ == "__main__":
    main()
