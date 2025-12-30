from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Iterator, List

from .http import request_json


@dataclass(frozen=True)
class OzonFboClient:
    client_id: str
    api_key: str
    base_url: str = "https://api-seller.ozon.ru"

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Client-Id": str(self.client_id),
            "Api-Key": str(self.api_key),
            "Content-Type": "application/json; charset=utf-8",
        }

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("POST", self.base_url + path, headers=self.headers, json_body=payload)

    # ----------------------------
    # Supply-order list
    # ----------------------------
    def list_supply_order_ids(
        self,
        state: int,
        limit: int = 100,
        from_supply_order_id: int = 0,
        sort_by: Optional[int] = None,
        sort_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Важно:
        - Ozon ругается, если передать sort_by=None как 0 (invalid SortBy [0]).
        - Поэтому sort_by/sort_dir включаем ТОЛЬКО если они явно заданы.
        """
        payload: Dict[str, Any] = {
            "filter": {
                "states": [state],
                "from_supply_order_id": int(from_supply_order_id),
            },
            "limit": int(limit),
        }

        if sort_by is not None:
            payload["sort_by"] = sort_by
        if sort_dir is not None:
            payload["sort_dir"] = sort_dir

        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(self, state: int, limit: int = 100) -> Iterator[int]:
        """
        Постраничный обход order_ids по state.
        """
        last = 0
        while True:
            data = self.list_supply_order_ids(state=state, limit=limit, from_supply_order_id=last)
            ids = data.get("order_ids") or []
            for oid in ids:
                if isinstance(oid, int):
                    yield oid

            # В ответе Ozon часто отдает "last_id" (строка), но для from_supply_order_id нужен int.
            # Поэтому безопаснее двигаться по максимуму из ids.
            if not ids:
                break

            last = max(ids)
            # Иногда API может отдавать повторно тот же last — защита:
            if last <= 0:
                break

    # ----------------------------
    # Order details
    # ----------------------------
    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v2/supply-order/get", {"order_ids": order_ids})

    # ----------------------------
    # Bundle items (Ozon)
    # ----------------------------
    def get_bundle_items(self, bundle_ids: List[str], limit: int = 100) -> Dict[str, Any]:
        """
        Возвращает товары (offer_id, quantity) внутри Ozon bundle_id.
        """
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": int(limit)})
