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
        )

    def list_supplies(self, *, status: Optional[str] = None, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
        # Ozon: /v1/supply-order/list (FBO supplies list)
        payload: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
        }
        if status:
            payload["status"] = status
        return self.post("/v1/supply-order/list", payload)

    def get_supply(self, supply_order_id: int) -> Dict[str, Any]:
        # Ozon: /v1/supply-order/get
        return self.post("/v1/supply-order/get", {"supply_order_id": supply_order_id})

    def get_supply_items(self, supply_order_id: int, *, limit: int = 1000, offset: int = 0) -> Dict[str, Any]:
        # Ozon: /v1/supply-order/items
        return self.post("/v1/supply-order/items", {
            "supply_order_id": supply_order_id,
            "limit": limit,
            "offset": offset,
        })
