from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .http import request_json


@dataclass(frozen=True)
class MoySkladClient:
    token: str
    base_url: str = "https://api.moysklad.ru/api/remap/1.2"

    @property
    def auth_headers(self) -> Dict[str, str]:
        # ВАЖНО:
        # - Accept строго application/json;charset=utf-8 (МС ругался у тебя ранее).
        # - Content-Type НЕ шлём глобально, иначе GET может ловить 415.
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json;charset=utf-8",
        }

    def _headers_for_json(self) -> Dict[str, str]:
        h = dict(self.auth_headers)
        h["Content-Type"] = "application/json;charset=utf-8"
        return h

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return request_json("GET", self.base_url + path, headers=self.auth_headers, params=params)

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("POST", self.base_url + path, headers=self._headers_for_json(), json_body=payload)

    def put(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("PUT", self.base_url + path, headers=self._headers_for_json(), json_body=payload)

    def delete(self, path: str) -> Dict[str, Any]:
        return request_json("DELETE", self.base_url + path, headers=self.auth_headers)

    # -------- helpers --------

    def get_by_href(self, href: str) -> Dict[str, Any]:
        return request_json("GET", href, headers=self.auth_headers)

    def get_bundle_components(self, bundle_id: str) -> list[Dict[str, Any]]:
        # Компоненты комплекта:
        # /entity/bundle/{bundle_id}/components
        res = self.get(f"/entity/bundle/{bundle_id}/components")
        return res.get("rows") or []

    def find_assortment_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        # 1) Прямой фильтр по article (твой кейс)
        res = self.get("/entity/assortment", params={"filter": f"article={article}", "limit": 1})
        rows = res.get("rows") or []
        if rows:
            return rows[0]

        # 2) Железобетонный fallback: search (иногда артикул лежит в code)
        res = self.get("/entity/assortment", params={"search": article, "limit": 50})
        rows = res.get("rows") or []
        for r in rows:
            if r.get("article") == article or r.get("code") == article:
                return r
        return None

    def get_sale_price(self, product: Dict[str, Any]) -> int:
        # Базовая цена продажи: первая ненулевая из salePrices.value
        prices = product.get("salePrices") or []
        for p in prices:
            v = p.get("value")
            if v:
                return int(v)
        return 0
