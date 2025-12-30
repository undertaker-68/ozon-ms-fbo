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

    # ---------- supply orders ----------

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
        ВАЖНО:
        - НЕЛЬЗЯ отправлять sort_by=0 (Ozon 400).
        - Если sort_by/sort_dir не заданы — НЕ отправляем их вообще.
        """
        payload: Dict[str, Any] = {
            "filter": {
                "states": [int(state)],
                "from_supply_order_id": int(from_supply_order_id),
            },
            "limit": int(limit),
        }

        # добавляем сортировку только если она явно задана
        if sort_by is not None:
            sb = int(sort_by)
            if sb == 0:
                # 0 запрещён Ozon-ом -> лучше вообще не слать
                pass
            else:
                payload["sort_by"] = sb

        if sort_dir is not None:
            sd = str(sort_dir).upper()
            if sd in ("ASC", "DESC"):
                payload["sort_dir"] = sd

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
        """
        Итератор по order_id с пагинацией через from_supply_order_id.
        """
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

            # если пусто — закончили
            if not ids:
                break

            # Ozon ожидает “следующую страницу” от last_id / from_supply_order_id
            last = int(data.get("last_id") or ids[-1])

            if not data.get("has_next"):
                break

            time.sleep(sleep_sec)
