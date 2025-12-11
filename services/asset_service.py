from typing import Any, Dict

import pymysql
from auth.kiwoom_auth import get_access_token
from clients.rest import get_account_balance
from utils.parsers import to_float, to_int


def fetch_my_assets():
    token = get_access_token()
    return get_account_balance(token)


def save_account_data(
    conn: pymysql.connections.Connection, data: Dict[str, Any]
) -> None:
    """
    키움 잔고 조회 응답(JSON dict)을 trading.account_summary / holdings 테이블에 저장
    """

    with conn.cursor() as cur:
        # 1) 계좌 요약 저장
        cur.execute(
            """
            INSERT INTO account_summary (
                acnt_nm,
                tot_est_amt,
                tot_pur_amt,
                profit_amt,
                profit_rt
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                data["acnt_nm"],
                to_int(data["tot_est_amt"]),
                to_int(data["tot_pur_amt"]),
                to_int(data["lspft_amt"]),
                to_float(data["lspft_ratio"]),
            ),
        )

        account_id = cur.lastrowid  # 방금 insert한 account_summary.id

        # 2) 종목별 보유내역 저장
        for stk in data["stk_acnt_evlt_prst"]:
            cur.execute(
                """
                INSERT INTO holdings (
                    stk_cd,
                    stk_nm,
                    qty,
                    avg_price,
                    cur_price,
                    eval_amt,
                    profit_amt,
                    profit_rt,
                    account_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    stk["stk_cd"],
                    stk["stk_nm"],
                    to_int(stk["rmnd_qty"]),
                    to_int(stk["avg_prc"]),
                    to_int(stk["cur_prc"]),
                    to_int(stk["evlt_amt"]),
                    to_int(stk["pl_amt"]),
                    to_float(stk["pl_rt"]),
                    account_id,
                ),
            )

    conn.commit()
