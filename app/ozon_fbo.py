cd /root/ozon_ms_fbo_integration

cat > app/ozon_fbo.py <<'PY'
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

    def _post_with_fallback(
        self,
        primary_path: str,
        fallback_path: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        try:
            return self.post(primary_path, payload)
        except HttpError as e:
            # если вдруг на аккаунте не включён v3, пробуем v2
            if " 404 " in str(e) or str(e).startswith("404 "):
                return self.post(fallback_path, payload)
            raise

    def list_supplies(self, *, status: Optional[str] = None, limit: int = 100, offset: int = 0) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "limit": limit,
        "offset": offset,
        "sort_by": "CREATED_AT",
        "order": "DESC",
    }
    if status:
        payload["status"] = status

    return self._post_with_fallback("/v3/supply-order/list", "/v2/supply-order/list", payload)

    def get_supply(self, supply_order_id: int) -> Dict[str, Any]:
        payload = {"supply_order_id": supply_order_id}
        return self._post_with_fallback("/v3/supply-order/get", "/v2/supply-order/get", payload)

    def get_supply_bundle(self, supply_order_id: int) -> Dict[str, Any]:
        # items deprecated -> bundle
        return self.post("/v1/supply-order/bundle", {"supply_order_id": supply_order_id})
PY
