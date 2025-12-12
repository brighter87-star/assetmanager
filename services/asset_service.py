import json
from datetime import date, datetime
from typing import Any, Dict, List

import pymysql
from utils.parsers import to_float, to_int


def save_account_data(
    conn: pymysql.connections.Connection, data: Dict[str, Any]
) -> None:
    """
    키움 잔고 조회 응답(JSON dict)을 trading.account_summary / holdings 테이블에 저장
    """
    snapshot_date = date.today()
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM account_summary WHERE snapshot_date=%s", (snapshot_date)
        )
        cur.execute("DELETE FROM holdings WHERE snapshot_date=%s", (snapshot_date))
        # 1) account_summary : 상단 계좌 요약 + 모든 숫자/비율 + return_code/msg + raw_json
        cur.execute(
            """
            INSERT INTO account_summary (
                snapshot_date,
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
                %s,
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
                snapshot_date,
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
                    snapshot_date,
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
                    %s,
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
                    snapshot_date,
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
        cur.execute("DELETE FROM realized_pnl_daily WHERE query_date=%s", (query_date))
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


def save_account_trade_history(
    conn: pymysql.connections.Connection,
    trades: List[Dict],
    trade_date: str,  # YYYYMMDD
) -> int:
    """
    계좌 주문/체결 내역을 account_trade_history 테이블에 저장
    - 중복 데이터는 자동 무시
    - return: 실제로 INSERT된 row 수
    """

    insert_sql = """
    INSERT INTO account_trade_history (
        ord_no,
        ori_ord_no,
        stk_cd,
        stk_nm,
        io_tp_nm,
        trde_tp,
        crd_tp,
        loan_dt,
        ord_qty,
        ord_uv,
        ord_tm,
        acpt_tp,
        rsrv_tp,
        ord_remnq,
        cntr_qty,
        cntr_uv,
        cnfm_qty,
        cnfm_tm,
        mdfy_cncl,
        comm_ord_tp,
        dmst_stex_tp,
        cond_uv,
        trade_date
    )
    VALUES (
        %(ord_no)s,
        %(ori_ord)s,
        %(stk_cd)s,
        %(stk_nm)s,
        %(io_tp_nm)s,
        %(trde_tp)s,
        %(crd_tp)s,
        %(loan_dt)s,
        %(ord_qty)s,
        %(ord_uv)s,
        %(ord_tm)s,
        %(acpt_tp)s,
        %(rsrv_tp)s,
        %(ord_remnq)s,
        %(cntr_qty)s,
        %(cntr_uv)s,
        %(cnfm_qty)s,
        %(cnfm_tm)s,
        %(mdfy_cncl)s,
        %(comm_ord_tp)s,
        %(dmst_stex_tp)s,
        %(cond_uv)s,
        %(trade_date)s
    )
    ON DUPLICATE KEY UPDATE
        ord_no = ord_no
    """

    inserted = 0

    with conn.cursor() as cur:
        for t in trades:
            params = {
                "ord_no": t.get("ord_no"),
                "ori_ord": t.get("ori_ord"),
                "stk_cd": t.get("stk_cd"),
                "stk_nm": t.get("stk_nm"),
                "io_tp_nm": t.get("io_tp_nm"),
                "trde_tp": t.get("trde_tp"),
                "crd_tp": t.get("crd_tp"),
                "loan_dt": t.get("loan_dt") or None,
                "ord_qty": to_int(t.get("ord_qty")),
                "ord_uv": to_int(t.get("ord_uv")),
                "ord_tm": t.get("ord_tm"),
                "acpt_tp": t.get("acpt_tp"),
                "rsrv_tp": t.get("rsrv_tp"),
                "ord_remnq": to_int(t.get("ord_remnq")),
                "cntr_qty": to_int(t.get("cntr_qty")),
                "cntr_uv": to_int(t.get("cntr_uv")),
                "cnfm_qty": to_int(t.get("cnfm_qty")),
                "cnfm_tm": t.get("cnfm_tm"),
                "mdfy_cncl": t.get("mdfy_cncl"),
                "comm_ord_tp": t.get("comm_ord_tp"),
                "dmst_stex_tp": t.get("dmst_stex_tp"),
                "cond_uv": to_int(t.get("cond_uv")),
                "trade_date": trade_date,
            }

            cur.execute(insert_sql, params)
            inserted += cur.rowcount  # 1이면 insert, 0이면 중복

    conn.commit()
    return inserted
