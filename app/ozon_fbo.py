from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .http import request_json, HttpError


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

    def _post_with_fallback(self, primary: str, fallback: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        try:
            return self.post(primary, payload)
        except HttpError as e:
            # если v3 недоступен — пробуем v2
            if str(e).startswith("404 ") or " 404 " in str(e):
                return self.post(fallback, payload)
            raise

    def list_supplies(
        self,
        *,
        status: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        # ВАЖНО: sort_by/order должны быть всегда, иначе Ozon отдаёт 400
        payload: Dict[str, Any] = {
            "limit": limit,
            "offset": offset,
            "sortBy": "1",
            "order": "2",
        }
        if status:
            payload["status"] = status

        return self._post_with_fallback(
            "/v3/supply-order/list",
            "/v2/supply-order/list",
            payload,
        )

    def get_supply(self, supply_order_id: int) -> Dict[str, Any]:
        payload = {"supply_order_id": supply_order_id}
        return self._post_with_fallback(
            "/v3/supply-order/get",
            "/v2/supply-order/get",
            payload,
        )

    def get_supply_bundle(self, supply_order_id: int) -> Dict[str, Any]:
        return self.post(
            "/v1/supply-order/bundle",
            {"supply_order_id": supply_order_id},
        )
