from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from .http import request_json


@dataclass(frozen=True)
class MoySkladClient:
    token: str
    base_url: str = "https://api.moysklad.ru/api/remap/1.2"

    @property
    def headers(self) -> Dict[str, str]:
        # ВАЖНО: MoySklad принимает только этот Accept
        return {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json;charset=utf-8",
            "Content-Type": "application/json;charset=utf-8",
            "User-Agent": "ozon-ms-fbo-integration/1.0",
        }

    # ---------- low level ----------
    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return request_json("GET", self.base_url + path, headers=self.headers, params=params)

    def post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("POST", self.base_url + path, headers=self.headers, json_body=payload)

    def put(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        return request_json("PUT", self.base_url + path, headers=self.headers, json_body=payload)

    def delete(self, path: str) -> Dict[str, Any]:
        return request_json("DELETE", self.base_url + path, headers=self.headers)

    def get_by_href(self, href: str) -> Dict[str, Any]:
        # href уже полный
        return request_json("GET", href, headers=self.headers)

    # ---------- helpers ----------
    def meta(self, entity: str, id_: str) -> Dict[str, Any]:
        return {
            "meta": {
                "href": f"{self.base_url}/entity/{entity}/{id_}",
                "type": entity,
                "mediaType": "application/json",
            }
        }

    def find_assortment_by_article(self, article: str) -> Optional[Dict[str, Any]]:
        res = self.get("/entity/assortment", params={"filter": f"article={article}", "limit": 1})
        rows = res.get("rows") or []
        return rows[0] if rows else None

    def get_bundle_components(self, bundle_id: str) -> list[Dict[str, Any]]:
        """
        Компоненты комплекта:
        GET /entity/bundle/{id}/components
        """
        res = self.get(f"/entity/bundle/{bundle_id}/components")
        return res.get("rows") or []

    def get_sale_price(self, obj: Dict[str, Any]) -> int:
        """
        Базовая цена продажи: salePrices[].value (первая ненулевая)
        """
        prices = obj.get("salePrices") or []
        for p in prices:
            v = p.get("value")
            if v:
                return int(v)
        return 0
