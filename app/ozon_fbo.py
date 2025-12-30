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
            timeout=60,
        )

    # ---------------------------------------------------------
    # /v3/supply-order/list
    #
    # ВАЖНО:
    #  - НЕ передаём sort_by/sort_dir вообще (из-за 400: SortBy must not be [0])
    #  - пагинация через last_id (from_supply_order_id)
    # ---------------------------------------------------------
    def list_supply_order_ids(
        self,
        state: int,
        limit: int = 100,
        last_id: str | int = 0,
    ) -> Dict[str, Any]:
        payload = {
            "filter": {
                "states": [state],
                "from_supply_order_id": last_id,
            },
            "limit": limit,
        }
        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(self, state: int, limit: int = 100) -> Any:
        last_id: str | int = 0
        seen = set()

        while True:
            data = self.list_supply_order_ids(state=state, limit=limit, last_id=last_id)
            ids = data.get("order_ids") or []
            for oid in ids:
                if oid in seen:
                    continue
                seen.add(oid)
                yield oid

            new_last = data.get("last_id")
            if not new_last:
                break

            # Если сервер вдруг возвращает тот же last_id — выходим, чтобы не зависать.
            if new_last == last_id:
                break

            last_id = new_last

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})

    # ---------------------------------------------------------
    # /v1/supply-order/bundle
    # ---------------------------------------------------------
    def get_bundle_items(self, bundle_ids: List[str], limit: int = 100) -> Dict[str, Any]:
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": limit})
