import json

import requests
from auth.kiwoom_auth import get_access_token
from config.api_endpoints import AccountStatus, AccountTradeHistory, RealizedPnLDaily
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
    body = {"qry_tp": "1", "dmst_stex_tp": "KRX"}
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


def get_account_trade_history(
    ord_dt: str | None = None,
    qry_tp: str = "4",
    stk_bond_tp: str = "0",
    sell_tp: str = "0",
    stk_cd: str | None = None,
    dmst_stex_tp: str = "%",
):

    headers = _make_headers(AccountTradeHistory.api_id)

    body = {
        "ord_dt": ord_dt,
        "qry_tp": qry_tp,
        "stk_bond_tp": stk_bond_tp,
        "sell_tp": sell_tp,
        "stk_cd": stk_cd or "",
        "fr_ord_no": "",
        "dmst_stex_tp": dmst_stex_tp,
    }

    all_trades: list[dict] = []

    cont_yn = "N"
    next_key = ""

    while True:
        if cont_yn == "Y":
            headers["cont-yn"] = "Y"
            headers["next-key"] = next_key

        resp = requests.post(
            url=f"{settings.BASE_URL}{AccountTradeHistory.path}",
            headers=headers,
            json=body,
        )
        resp.raise_for_status()

        data = resp.json()

        # 1) body: 체결내역 수집
        trades = data.get("acnt_ord_cntr_prps_dtl", [])
        all_trades.extend(trades)

        # 2) header: 연속조회 여부 확인
        cont_yn = resp.headers.get("cont-yn", "N")
        next_key = resp.headers.get("next-key", "")

        if cont_yn != "Y":
            break

    return all_trades
