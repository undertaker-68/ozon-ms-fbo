from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

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

    def list_supply_order_ids(self, states, limit=100, from_supply_order_id=0):
        payload = {
            "filter": {
            "states": states,
            "from_supply_order_id": from_supply_order_id,
            },
            "limit": limit,
            "sort_by": 1,
            "sort_dir": "ASC",
        }
        print("DEBUG payload:", payload)
        return self.post("/v3/supply-order/list", payload)

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        # ВАЖНО: get принимает массив id (order_ids)
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})
