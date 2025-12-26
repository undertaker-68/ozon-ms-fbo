from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from .http import request_json


@dataclass(frozen=True)
class MoySkladClient:
    token: str
    base_url: str = "https://api.moysklad.ru/api/remap/1.2"

    @property
    def headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "Accept": "application/json;charset=utf-8",
        }

    def get(self, path: str, params: Dict[str, Any] | None = None) -> Dict[str, Any]:
        return request_json("GET", self.base_url + path, headers=self.headers, params=params)

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("POST", self.base_url + path, headers=self.headers, json_body=payload)

    def put(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("PUT", self.base_url + path, headers=self.headers, json_body=payload)

    def delete(self, path: str) -> Dict[str, Any]:
        return request_json("DELETE", self.base_url + path, headers=self.headers)

    def get_bundle_components(self, bundle_id: str):
        """
        Возвращает компоненты комплекта по его ID.
        """
        res = self.get(f"/entity/product/{bundle_id}/components")
        return res.get("rows", [])

    def find_assortment_by_article(self, article: str):
        res = self.get("/entity/assortment", params={"filter": f"article={article}", "limit": 1})
        rows = res.get("rows") or []
        return rows[0] if rows else None

    # ---- helpers ----

    def get_sale_price(self, product: Dict[str, Any]) -> int:
        """
        Берём базовую цену продажи (первую ненулевую) из salePrices.value.
        Возвращаем int в копейках.
        """
        prices = product.get("salePrices") or []
        for p in prices:
            value = p.get("value")
            if value:
                return int(value)
        return 0
