from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, Iterator, Optional

from .http import request_json


@dataclass
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

    def list_supply_order_ids(
        self,
        *,
        state: int,
        from_supply_order_id: int = 0,
        limit: int = 100,
        sort_by: Optional[int] = None,
        sort_dir: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        ВАЖНО (по факту ответа API):
        - sort_by ОБЯЗАТЕЛЕН и НЕ может быть 0
        - поэтому используем дефолт sort_by=1, sort_dir=ASC
        """
        sb = 1 if sort_by is None else int(sort_by)
        if sb == 0:
            sb = 1  # 0 запрещён

        sd = "ASC" if sort_dir is None else str(sort_dir).upper()
        if sd not in ("ASC", "DESC"):
            sd = "ASC"

        payload: Dict[str, Any] = {
            "filter": {
                "states": [int(state)],
                "from_supply_order_id": int(from_supply_order_id),
            },
            "limit": int(limit),
            "sort_by": sb,
            "sort_dir": sd,
        }
        return self.post("/v3/supply-order/list", payload)

    def get_supply_orders(self, order_ids: list[int]) -> Dict[str, Any]:
        return self.post("/v3/supply-order/get", {"order_ids": order_ids})

    def iter_supply_order_ids(
        self,
        *,
        state: int,
        limit: int = 100,
        sort_by: Optional[int] = None,
        sort_dir: Optional[str] = None,
        sleep_sec: float = 0.05,
    ) -> Iterator[int]:
        last = 0
        while True:
            data = self.list_supply_order_ids(
                state=state,
                from_supply_order_id=last,
                limit=limit,
                sort_by=sort_by,
                sort_dir=sort_dir,
            )

            ids = data.get("order_ids") or []
            for oid in ids:
                yield int(oid)

            if not ids:
                break

            last_id = data.get("last_id")
            last = int(last_id) if last_id else int(ids[-1])

            if not data.get("has_next"):
                break

            time.sleep(sleep_sec)
