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

    # ===== ДОБАВИТЬ НИЖЕ =====

    def find_product_by_article(self, article: str):
        res = self.get(
            "/entity/product",
            params={"filter": f"article={article}", "limit": 1},
        )
        rows = res.get("rows") or []
        return rows[0] if rows else None

    def get_bundle_components(self, bundle_id: str):
        res = self.get(f"/entity/bundle/{bundle_id}")
        return (res.get("components") or {}).get("rows") or []

    def get_sale_price(self, product: dict) -> int:
        # базовая цена продажи
        prices = product.get("salePrices") or []
        for p in prices:
            value = p.get("value")
            if value:
                return int(value)
        return 0

    def has_demand(self, order_name: str) -> bool:
        # если по заказу уже есть отгрузка — не трогаем
        res = self.get(
            "/entity/demand",
            params={"filter": f"description~{order_name}", "limit": 1},
        )
        return bool(res.get("rows"))
