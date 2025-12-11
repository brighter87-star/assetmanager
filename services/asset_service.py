import json
from datetime import datetime
from typing import Any, Dict

import pymysql
from utils.parsers import to_float, to_int


def save_account_data(
    conn: pymysql.connections.Connection, data: Dict[str, Any]
) -> None:
    """
    키움 잔고 조회 응답(JSON dict)을 trading.account_summary / holdings 테이블에 저장
    """
    with conn.cursor() as cur:
        # 1) account_summary : 상단 계좌 요약 + 모든 숫자/비율 + return_code/msg + raw_json
        cur.execute(
            """
            INSERT INTO account_summary (
                acnt_nm,
                brch_nm,
                entr,
                d2_entra,
                tot_est_amt,
                aset_evlt_amt,
                tot_pur_amt,
                prsm_dpst_aset_amt,
                tot_grnt_sella,
                tdy_lspft_amt,
                invt_bsamt,
                lspft_amt,
                tdy_lspft,
                lspft2,
                lspft,
                tdy_lspft_rt,
                lspft_ratio,
                lspft_rt,
                return_code,
                return_msg,
                raw_json
            )
            VALUES (
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s
            )
            """,
            (
                data.get("acnt_nm"),
                data.get("brch_nm"),
                to_int(data.get("entr")),
                to_int(data.get("d2_entra")),
                to_int(data.get("tot_est_amt")),
                to_int(data.get("aset_evlt_amt")),
                to_int(data.get("tot_pur_amt")),
                to_int(data.get("prsm_dpst_aset_amt")),
                to_int(data.get("tot_grnt_sella")),
                to_int(data.get("tdy_lspft_amt")),
                to_int(data.get("invt_bsamt")),
                to_int(data.get("lspft_amt")),
                to_int(data.get("tdy_lspft")),
                to_int(data.get("lspft2")),
                to_int(data.get("lspft")),
                to_float(data.get("tdy_lspft_rt")),
                to_float(data.get("lspft_ratio")),
                to_float(data.get("lspft_rt")),
                to_int(data.get("return_code")),
                data.get("return_msg"),
                json.dumps(data, ensure_ascii=False),
            ),
        )

        account_id = cur.lastrowid

        for stk in data.get("stk_acnt_evlt_prst", []):
            cur.execute(
                """
                INSERT INTO holdings (
                    account_id,
                    stk_cd,
                    stk_nm,
                    rmnd_qty,
                    avg_prc,
                    cur_prc,
                    evlt_amt,
                    pl_amt,
                    pl_rt,
                    loan_dt,
                    pur_amt,
                    setl_remn,
                    pred_buyq,
                    pred_sellq,
                    tdy_buyq,
                    tdy_sellq,
                    raw_json
                )
                VALUES (
                    %s,  -- account_id
                    %s, %s,
                    %s, %s, %s, %s,
                    %s, %s,
                    %s,
                    %s, %s, %s, %s, %s, %s,
                    %s
                )
                """,
                (
                    account_id,
                    stk.get("stk_cd"),
                    stk.get("stk_nm"),
                    to_int(stk.get("rmnd_qty")),
                    to_int(stk.get("avg_prc")),
                    to_int(stk.get("cur_prc")),
                    to_int(stk.get("evlt_amt")),
                    to_int(stk.get("pl_amt")),
                    to_float(stk.get("pl_rt")),
                    stk.get("loan_dt") or "",
                    to_int(stk.get("pur_amt")),
                    to_int(stk.get("setl_remn")),
                    to_int(stk.get("pred_buyq")),
                    to_int(stk.get("pred_sellq")),
                    to_int(stk.get("tdy_buyq")),
                    to_int(stk.get("tdy_sellq")),
                    json.dumps(stk, ensure_ascii=False),
                ),
            )

    conn.commit()


def save_realized_pnl_daily(
    conn: pymysql.connections.Connection,
    data: Dict[str, Any],
    query_date: str,  # "YYYYMMDD" 형식으로 전달
) -> None:
    """
    일자별 실현손익 응답(JSON)을 trading.realized_pnl_daily 테이블에 저장
    """

    # "20251211" -> date 객체로 변환
    qdate = datetime.strptime(query_date, "%Y%m%d").date()

    rows = data.get("dt_stk_div_rlzt_pl", [])
    return_code = to_int(data.get("return_code"))
    return_msg = data.get("return_msg")

    with conn.cursor() as cur:
        for item in rows:
            cur.execute(
                """
                INSERT INTO realized_pnl_daily (
                    query_date,
                    stk_cd,
                    stk_nm,
                    cntr_qty,
                    buy_uv,
                    cntr_pric,
                    tdy_sel_pl,
                    pl_rt,
                    tdy_trde_cmsn,
                    tdy_trde_tax,
                    wthd_alowa,
                    loan_dt,
                    crd_tp,
                    return_code,
                    return_msg,
                    raw_json
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s,
                    %s
                )
                """,
                (
                    qdate,
                    item.get("stk_cd1"),  # 종목코드
                    item.get("stk_nm"),
                    to_int(item.get("cntr_qty")),
                    to_int(item.get("buy_uv")),
                    to_int(item.get("cntr_pric")),
                    to_int(item.get("tdy_sel_pl1")),  # 실현손익
                    to_float(item.get("pl_rt")),
                    to_int(item.get("tdy_trde_cmsn")),
                    to_int(item.get("tdy_trde_tax")),
                    to_int(item.get("wthd_alowa")),
                    item.get("loan_dt") or "",
                    item.get("crd_tp"),
                    return_code,
                    return_msg,
                    json.dumps(item, ensure_ascii=False),
                ),
            )

    conn.commit()
