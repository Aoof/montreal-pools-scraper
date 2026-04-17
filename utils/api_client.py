from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class ApiPoolType:
    id: int
    name: str


class ScraperApiClient:
    def __init__(
        self,
        base_url: str,
        username: str,
        password: str,
        timeout: int = 20,
        retries: int = 2,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.timeout = timeout
        self.retries = retries
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def login(self) -> None:
        response = self._request(
            "POST",
            "/auth/login",
            json={"username": self.username, "password": self.password},
            auth_required=False,
        )
        if response.status_code >= 400:
            raise RuntimeError(
                f"API login failed ({response.status_code}): {response.text.strip()}"
            )

    def get_pools(self) -> list[dict[str, Any]]:
        response = self._request("GET", "/pools", auth_required=False)
        payload = self._parse_json(response)
        pools = payload.get("pools", [])
        if not isinstance(pools, list):
            return []
        return [item for item in pools if isinstance(item, dict)]

    def get_pool_types(self) -> list[ApiPoolType]:
        response = self._request("GET", "/pool-types", auth_required=False)
        if response.status_code == 404:
            return self._infer_pool_types_from_pools()

        payload = self._parse_json(response)
        raw_types = payload.get("types", [])
        if not isinstance(raw_types, list):
            return []

        parsed: list[ApiPoolType] = []
        for item in raw_types:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("id")
            raw_name = item.get("name")
            if not isinstance(raw_id, int) or not isinstance(raw_name, str):
                continue
            parsed.append(ApiPoolType(id=raw_id, name=raw_name))
        return parsed

    def create_pool(self, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._request("POST", "/pools", json=payload)
        if response.status_code >= 400:
            raise RuntimeError(
                f"API create failed ({response.status_code}): {response.text.strip()}"
            )
        return self._parse_json(response)

    def update_pool(self, pool_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        response = self._request("PATCH", f"/pools/{pool_id}", json=payload)
        if response.status_code >= 400:
            raise RuntimeError(
                f"API update failed for pool {pool_id} ({response.status_code}): "
                f"{response.text.strip()}"
            )
        return self._parse_json(response)

    def _infer_pool_types_from_pools(self) -> list[ApiPoolType]:
        pools = self.get_pools()
        by_name: dict[str, ApiPoolType] = {}
        for pool in pools:
            raw_types = pool.get("types", [])
            if not isinstance(raw_types, list):
                continue
            for item in raw_types:
                if not isinstance(item, dict):
                    continue
                raw_id = item.get("id")
                raw_name = item.get("name")
                if isinstance(raw_id, int) and isinstance(raw_name, str) and raw_name.strip():
                    key = raw_name.strip().lower()
                    by_name[key] = ApiPoolType(id=raw_id, name=raw_name.strip())

        return list(by_name.values())

    def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        auth_required: bool = True,
    ) -> requests.Response:
        if auth_required and "pool_my_finger_session" not in self.session.cookies:
            self.login()

        url = f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(self.retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    json=json,
                    timeout=self.timeout,
                )
                if response.status_code >= 500 and attempt < self.retries:
                    time.sleep(0.3 * (attempt + 1))
                    continue
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt < self.retries:
                    time.sleep(0.3 * (attempt + 1))
                    continue
                raise RuntimeError(f"API request failed for {method} {url}: {exc}") from exc

        raise RuntimeError(f"API request failed for {method} {url}: {last_error}")

    @staticmethod
    def _parse_json(response: requests.Response) -> dict[str, Any]:
        try:
            payload = response.json()
        except ValueError as exc:
            raise RuntimeError(
                f"Expected JSON from API, got status {response.status_code}"
            ) from exc

        if not isinstance(payload, dict):
            raise RuntimeError("Expected JSON object from API response")
        return payload
