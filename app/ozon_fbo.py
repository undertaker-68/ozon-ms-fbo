from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional

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
            timeout=60,
        )

    # ---------- Supply Orders ----------

    def list_supply_order_ids(
        self,
        state: int,
        limit: int = 100,
        from_supply_order_id: str | int = 0,
    ) -> Dict[str, Any]:
        # ВАЖНО: sort_by/sort_dir НЕ трогаем динамически.
        # У тебя подтверждено, что вызов без "sort_by=None" работает.
        payload = {
            "filter": {
                "states": [state],
                "from_supply_order_id": from_supply_order_id,
            },
            "limit": limit,
            # Оставляем как “железобетонно рабочее”
            "sort_by": 1,
            "sort_dir": "ASC",
        }
        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(self, state: int, limit: int = 100) -> Iterator[int]:
        last: str | int = 0
        while True:
            data = self.list_supply_order_ids(state=state, limit=limit, from_supply_order_id=last)
            ids = data.get("order_ids") or []
            for oid in ids:
                if isinstance(oid, int):
                    yield oid

            last_id = data.get("last_id")
            if not last_id:
                break
            last = last_id

            # защита от “вечного” цикла, если API вдруг вернет тот же last_id
            if not ids:
                break

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})

    # ---------- Bundle items from Ozon (supply-order bundle) ----------

    def get_bundle_items(self, bundle_ids: List[str], limit: int = 100) -> Dict[str, Any]:
        # Это то, что ты руками тестил: /v1/supply-order/bundle
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": limit})
