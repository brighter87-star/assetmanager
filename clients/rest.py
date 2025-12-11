import json

import requests
from auth.kiwoom_auth import get_access_token
from config.api_endpoints import AccountStatus, RealizedPnLDaily
from config.settings import Settings

settings = Settings()
access_token = get_access_token()


def _make_headers(api_id):
    """
    api_id를 입력받아 공통 kiwoom api header 생성
    """
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "api-id": api_id,
    }


def get_account_balance():
    headers = _make_headers(AccountStatus.api_id)
    body = {"qry_tp": "1", "dmst_stex_tp": "NXT"}
    url = f"{settings.BASE_URL}{AccountStatus.path}"
    response = requests.post(
        f"{settings.BASE_URL}{AccountStatus.path}", headers=headers, json=body
    )
    response.raise_for_status()
    return json.loads(response.text)


def get_realized_pnl_daily(date: str):
    headers = _make_headers(RealizedPnLDaily.api_id)
    body = {"strt_dt": date, "end_dt": date}
    response = requests.post(
        f"{settings.BASE_URL}{RealizedPnLDaily.path}", headers=headers, json=body
    )
    response.raise_for_status()
    return json.loads(response.text)
