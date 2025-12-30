from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

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
        limit: int = 100,
        from_supply_order_id: int = 0,
        sort_by: Optional[int] = None,
        sort_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "filter": {
                "states": states,
                "from_supply_order_id": from_supply_order_id,
            },
            "limit": limit,
        }
        # sort_by/sort_dir у Озона могут быть капризными — задаём только если явно передали
        if sort_by is not None:
            payload["sort_by"] = sort_by
        if sort_dir is not None:
            payload["sort_dir"] = sort_dir

        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(
        self,
        *,
        state: int,
        limit: int = 100,
        sort_by: Optional[int] = None,
        sort_dir: Optional[str] = None,
        max_pages: int = 10000,
    ) -> Iterable[int]:
        """
        Пагинация через from_supply_order_id.
        Озон обычно возвращает order_ids отсортированные ASC по id при sort_dir=ASC,
        поэтому безопасно двигать курсор как max(order_ids).
        """
        cursor = 0
        seen: set[int] = set()

        for _ in range(max_pages):
            data = self.list_supply_order_ids(
                states=[state],
                limit=limit,
                from_supply_order_id=cursor,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )
            ids = data.get("order_ids") or []
            if not ids:
                break

            progressed = False
            for oid in ids:
                if not isinstance(oid, int):
                    continue
                if oid in seen:
                    continue
                seen.add(oid)
                progressed = True
                yield oid

            # двигаем курсор
            mx = max([i for i in ids if isinstance(i, int)], default=cursor)
            if mx <= cursor and not progressed:
                break
            cursor = mx

            # если Озон вернул меньше лимита — страниц больше нет
            if len(ids) < limit:
                break

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})

    def get_bundle_items(self, bundle_ids: List[str], limit: int = 100) -> Dict[str, Any]:
        """
        Ozon bundle (FBO): возвращает items (offer_id, quantity, ...).
        """
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": limit})
