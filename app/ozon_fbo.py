from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

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
            "Content-Type": "application/json; charset=utf-8",
        }

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("POST", self.base_url + path, headers=self.headers, json_body=payload)

    # --------- API wrappers ---------

    def list_supply_order_ids(
        self,
        *,
        state: int,
        limit: int = 100,
        from_supply_order_id: str | int = 0,
    ) -> Dict[str, Any]:
        # ВАЖНО: sort_by / sort_dir НЕ отправляем вообще (Ozon валидирует и падает на 0/None)
        payload: Dict[str, Any] = {
            "filter": {
                "states": [state],
                "from_supply_order_id": from_supply_order_id,
            },
            "limit": int(limit),
        }
        return self.post("/v3/supply-order/list", payload)

    def iter_supply_order_ids(self, *, state: int, limit: int = 100) -> Iterable[int]:
        last_id: str | int = 0
        while True:
            data = self.list_supply_order_ids(state=state, limit=limit, from_supply_order_id=last_id)
            ids = data.get("order_ids") or []
            for x in ids:
                if isinstance(x, int):
                    yield x

            new_last = data.get("last_id")
            # если last_id не пришел или список пуст — заканчиваем
            if not new_last or not ids:
                break
            last_id = new_last

    def get_supply_orders(self, order_ids: list[int]) -> Dict[str, Any]:
        return self.post("/v1/supply-order/get", {"order_ids": order_ids})

    def get_bundle(self, bundle_ids: list[str], limit: int = 100) -> Dict[str, Any]:
        # Возвращает items: offer_id, quantity
        return self.post("/v1/supply-order/bundle", {"bundle_ids": bundle_ids, "limit": int(limit)})
