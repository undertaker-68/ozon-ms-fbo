from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List

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
            timeout=60,
        )

    # ---------- Supply Orders ----------

    def list_supply_order_ids(
        self,
        state: int,
        limit: int = 100,
        from_supply_order_id: str | int = 0,
    ) -> Dict[str, Any]:
        """
        ВАЖНО:
        - sort_by=0 в Ozon не проходит (у тебя это подтверждено)
        - для archive states Ozon НЕ принимает sort_dir=ASC и отвечает:
          "Ascending sort direction is not supported for archive order states."
        Поэтому: пробуем ASC, если получаем эту ошибку — повторяем с DESC.
        """
        def _payload(sort_dir: str) -> Dict[str, Any]:
            return {
                "filter": {
                    "states": [state],
                    "from_supply_order_id": from_supply_order_id,
                },
                "limit": limit,
                "sort_by": 1,
                "sort_dir": sort_dir,
            }

        # 1) пробуем ASC
        try:
            return self.post("/v3/supply-order/list", _payload("ASC"))
        except HttpError as e:
            msg = str(e)
            if "Ascending sort direction is not supported for archive order states" in msg:
                # 2) archive states -> пробуем DESC
                return self.post("/v3/supply-order/list", _payload("DESC"))
            raise

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

            # защита от вечного цикла
            if not ids:
                break

    def get_supply_orders(self, order_ids: List[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})

    # ---------- Bundle items from Ozon ----------

    def get_bundle_items(self, bundle_ids: List[str], limit: int = 100) -> Dict[str, Any]:
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": limit})
