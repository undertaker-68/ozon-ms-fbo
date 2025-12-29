from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .http import request_json


@dataclass(frozen=True)
class OzonFboClient:
    client_id: str
    api_key: str
    base_url: str = "https://api-seller.ozon.ru"

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Client-Id": self.client_id,
            "Api-Key": self.api_key,
            "Content-Type": "application/json",
        }

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json(
            "POST",
            self.base_url + path,
            headers=self.headers,
            json_body=payload,
            timeout=30,
        )

    def list_supply_order_ids(
        self,
        states: List[int],
        *,
        limit: int = 100,
        from_supply_order_id: int = 0,
        sort_by: int = 1,
        sort_dir: str = "DESC",
    ) -> Dict[str, Any]:
        payload = {
            "filter": {
                "states": states,
                "from_supply_order_id": from_supply_order_id,
            },
            "limit": limit,
            "sort_by": sort_by,
            "sort_dir": sort_dir,
        }
        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(
        self,
        *,
        state: int,
        limit: int = 100,
        sort_by: int = 1,
        sort_dir: str = "DESC",
        max_pages: int = 10_000,
    ):
        """
        Надёжный пагинатор по /v3/supply-order/list.

        В ответах Ozon в разных версиях бывает:
          - order_ids: [...]
          - has_next: bool
          - last_id: "..."  (иногда строка)
          - last_id: 123    (иногда int)
        Также иногда может быть last_supply_order_id / next_from_supply_order_id.
        Поэтому делаем максимально устойчиво.
        """
        from_id = 0
        pages = 0

        while True:
            pages += 1
            if pages > max_pages:
                break

            resp = self.list_supply_order_ids(
                [state],
                limit=limit,
                from_supply_order_id=from_id,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )

            ids = resp.get("order_ids") or []
            for oid in ids:
                yield oid

            # определяем "следующую страницу"
            has_next = resp.get("has_next")
            last_id = resp.get("last_id")

            # иногда last_id приходит строкой
            next_from: Optional[int] = None
            if isinstance(last_id, int):
                next_from = last_id
            elif isinstance(last_id, str) and last_id.strip().isdigit():
                next_from = int(last_id.strip())

            # альтернативные поля (на всякий случай)
            alt = resp.get("last_supply_order_id") or resp.get("next_from_supply_order_id")
            if next_from is None and isinstance(alt, int):
                next_from = alt
            if next_from is None and isinstance(alt, str) and alt.strip().isdigit():
                next_from = int(alt.strip())

            # условия остановки
            if not ids:
                break
            if has_next is False:
                break
            if next_from is None:
                # если API не даёт курсор — дальше не пойдём, чтобы не зациклиться
                break

            # защита от вечного цикла при одинаковом курсоре
            if next_from == from_id:
                break

            from_id = next_from

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})
