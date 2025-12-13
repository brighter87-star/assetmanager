from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import urljoin

import requests
from config.settings import Settings

settings = Settings()


class ApiError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApiResponse:
    data: Any
    status_code: int
    headers: Mapping[str, str]


def request_json(
    method: str,
    path: str,
    *,
    headers: Optional[Mapping[str, str]] = None,
    json_body: Optional[Mapping[str, Any]] = None,
    timeout: float = 10.0,
) -> ApiResponse:
    request_url = urljoin(settings.BASE_URL, path)
    try:
        resp = requests.request(
            method=method,
            url=request_url,
            headers=dict(headers) if headers else None,
            json=json_body,
            timeout=timeout,
        )
        resp.raise_for_status()
        return ApiResponse(
            data=resp.json(), status_code=resp.status_code, headers=resp.headers
        )
    except requests.exceptions.HTTPError as e:
        text_snippet = e.response.text[:500] if e.response is not None else ""
        raise ApiError(f"HTTP 오류: {e} | body={text_snippet}") from e

    except requests.exceptions.RequestException as e:
        raise ApiError(f"요청 실패: {e}") from e

    except ValueError as e:
        raise ApiError(f"JSON 파싱 실패: {e} | body={resp.text[:500]}") from e
