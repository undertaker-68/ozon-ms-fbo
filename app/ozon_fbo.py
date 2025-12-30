from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union

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
    # Ozon валится, если sort_by отсутствует -> он превращается в 0 (невалидно).
    # Поэтому ЖЁСТКО передаём sort_by=1 и sort_dir="DESC".
    # ---------------------------------------------------------
    def list_supply_order_ids(
        self,
        state: int,
        limit: int = 100,
        last_id: Union[str, int] = 0,
        sort_by: int = 1,
        sort_dir: str = "DESC",
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "filter": {
                "states": [int(state)],
                "from_supply_order_id": last_id,
            },
            "limit": int(limit),
            "sort_by": int(sort_by),
            "sort_dir": str(sort_dir),
        }
        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(self, state: int, limit: int = 100):
        last_id: Union[str, int] = 0
        seen = set()

        while True:
            data = self.list_supply_order_ids(
                state=state,
                limit=limit,
                last_id=last_id,
                sort_by=1,
                sort_dir="DESC",
            )

            ids = data.get("order_ids") or []
            for oid in ids:
                if oid in seen:
                    continue
                seen.add(oid)
                yield oid

            new_last = data.get("last_id")
            if not new_last:
                break

            # защита от зацикливания
            if new_last == last_id:
                break

            last_id = new_last

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})

    def get_bundle_items(self, bundle_ids: List[str], limit: int = 100) -> Dict[str, Any]:
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": int(limit)})
