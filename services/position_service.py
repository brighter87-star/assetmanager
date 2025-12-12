from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional, Tuple

import pymysql


def _to_int(x: Any, default: int = 0) -> int:
    if x is None:
        return default
    if isinstance(x, int):
        return x
    s = str(x).strip()
    if s == "":
        return default
    return int(s)


def _to_decimal(x: Any, default: Decimal = Decimal("0")) -> Decimal:
    if x is None:
        return default
    if isinstance(x, Decimal):
        return x
    s = str(x).strip()
    if s == "":
        return default
    return Decimal(s)


def _combine_dt(trade_date, ord_tm: Optional[str]) -> datetime:
    """
    trade_date: DATE (python date) or 'YYYY-MM-DD'
    ord_tm: 'HH:MM:SS' (char(8)) or None
    """
    if hasattr(trade_date, "strftime"):
        d = trade_date.strftime("%Y-%m-%d")
    else:
        d = str(trade_date)

    t = (ord_tm or "00:00:00").strip()
    if len(t) != 8:
        t = "00:00:00"
    return datetime.fromisoformat(f"{d} {t}")


def _side_from_io(io_tp_nm: Optional[str]) -> Optional[str]:
    """
    키움 문자열 기반 간단 판정
    - '매도' 포함 => SELL
    - '매수' 포함 => BUY
    (대부분 '현금매도', '현금매수', '융자매도상환', '시간외신용매수' 등으로 들어옴)
    """
    if not io_tp_nm:
        return None
    s = io_tp_nm.strip()
    # '매도상환'도 매도
    if "매도" in s and "매수" not in s:
        return "SELL"
    if "매수" in s:
        return "BUY"
    return None


@dataclass
class Lot:
    buy_source_id: int
    buy_dt: datetime
    buy_px: Decimal
    remaining_qty: int
    stk_cd: str
    stk_nm: str
    crd_class: str


def build_lifo_lot_matches(
    conn: pymysql.connections.Connection,
    start_date: Optional[str] = None,  # 'YYYY-MM-DD'
    end_date: Optional[str] = None,  # 'YYYY-MM-DD'
) -> None:
    """
    account_trade_history를 읽어서 lot_matches를 (LIFO)로 재생성합니다.
    - 안전한 재실행을 위해: 지정 기간이 있으면 해당 기간의 lot_matches만 삭제 후 재생성
    - start/end 없으면: lot_matches 전체 삭제 후 재생성(간단/확실)
    """

    # 1) 트레이드 로드
    where = []
    params: Dict[str, Any] = {}
    if start_date:
        where.append("trade_date >= %(start_date)s")
        params["start_date"] = start_date
    if end_date:
        where.append("trade_date <= %(end_date)s")
        params["end_date"] = end_date

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        # 2) 파생 테이블 정리 (재실행 안전)
        if where_sql:
            cur.execute(
                f"""
                DELETE FROM lot_matches
                WHERE DATE(sell_dt) >= %(start_date)s AND DATE(sell_dt) <= %(end_date)s
                """,
                params,
            )
        else:
            cur.execute("TRUNCATE TABLE lot_matches")

        cur.execute(
            f"""
            SELECT
              id,
              trade_date,
              ord_tm,
              stk_cd,
              stk_nm,
              crd_class,
              io_tp_nm,
              cntr_qty,
              cntr_uv
            FROM account_trade_history
            {where_sql}
            ORDER BY trade_date ASC, ord_tm ASC, id ASC
            """,
            params,
        )
        rows: List[Dict[str, Any]] = cur.fetchall()

        # 3) (stk_cd, crd_tp)별 LIFO 스택
        stacks: Dict[Tuple[str, str], List[Lot]] = {}

        insert_sql = """
            INSERT INTO lot_matches (
              stk_cd, stk_nm, crd_class,
              buy_source_id, sell_source_id,
              buy_dt, sell_dt,
              buy_px, sell_px,
              match_qty,
              pnl_amt, holding_seconds, holding_days
            )
            VALUES (
              %s, %s, %s,
              %s, %s,
              %s, %s,
              %s, %s,
              %s,
              %s, %s, %s
            )
        """

        for r in rows:
            side = _side_from_io(r.get("io_tp_nm"))
            if side is None:
                continue

            stk_cd = (r.get("stk_cd") or "").strip()
            stk_nm = (r.get("stk_nm") or "").strip()
            # crd_tp = (r.get("crd_tp") or "").strip()
            crd_class = (r.get("crd_class") or "").strip()

            # key = (stk_cd, crd_tp)
            key = (stk_cd, crd_class)
            stacks.setdefault(key, [])

            dt = _combine_dt(r.get("trade_date"), r.get("ord_tm"))
            qty = _to_int(r.get("cntr_qty"), 0)
            px = _to_decimal(r.get("cntr_uv"))

            if qty <= 0:
                continue

            if side == "BUY":
                stacks[key].append(
                    Lot(
                        buy_source_id=_to_int(r["id"]),
                        buy_dt=dt,
                        buy_px=px,
                        remaining_qty=qty,
                        stk_cd=stk_cd,
                        stk_nm=stk_nm,
                        crd_class=crd_class,
                    )
                )
                continue

            # SELL: LIFO로 소진
            sell_source_id = _to_int(r["id"])
            sell_dt = dt
            sell_px = px
            to_close = qty

            while to_close > 0 and stacks[key]:
                lot = stacks[key][-1]
                match_qty = min(lot.remaining_qty, to_close)

                pnl = (sell_px - lot.buy_px) * Decimal(match_qty)
                holding_seconds = int((sell_dt - lot.buy_dt).total_seconds())
                holding_days = holding_seconds // 86400

                cur.execute(
                    insert_sql,
                    (
                        stk_cd,
                        stk_nm,
                        crd_class,
                        lot.buy_source_id,
                        sell_source_id,
                        lot.buy_dt,
                        sell_dt,
                        lot.buy_px,
                        sell_px,
                        match_qty,
                        pnl,
                        holding_seconds,
                        holding_days,
                    ),
                )

                lot.remaining_qty -= match_qty
                to_close -= match_qty

                if lot.remaining_qty == 0:
                    stacks[key].pop()

            # 만약 매도수량이 남았는데 스택이 비었다면(데이터 누락/과거 미수집 등)
            # 여기서는 조용히 스킵(원하시면 경고 로그 추가)
            # if to_close > 0: print("WARN: sell exceeds lots", stk_cd, crd_tp, to_close)

    conn.commit()


def build_position_episodes(
    conn: pymysql.connections.Connection,
) -> None:
    """
    account_trade_history 전체를 시간순으로 누적하여
    (종목+crd_tp)별 포지션이 0->양수 시작 / 양수->0 종료 되는 구간을 episode로 저장합니다.
    재실행 안전을 위해 episodes는 전체 재생성(TRUNCATE)로 처리합니다.
    """

    with conn.cursor(pymysql.cursors.DictCursor) as cur:
        cur.execute("TRUNCATE TABLE position_episodes")

        cur.execute(
            """
            SELECT
              id,
              trade_date,
              ord_tm,
              stk_cd,
              stk_nm,
              crd_class,
              io_tp_nm,
              cntr_qty
            FROM account_trade_history
            ORDER BY trade_date ASC, ord_tm ASC, id ASC
            """
        )
        rows = cur.fetchall()

        pos_qty: Dict[Tuple[str, str], int] = {}
        episode_seq: Dict[Tuple[str, str], int] = {}
        open_episode: Dict[Tuple[str, str], Dict[str, Any]] = {}

        insert_sql = """
            INSERT INTO position_episodes (
              stk_cd, stk_nm, crd_tp,
              episode_seq,
              start_dt, end_dt,
              start_qty, end_qty
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """

        for r in rows:
            side = _side_from_io(r.get("io_tp_nm"))
            if side is None:
                continue

            stk_cd = (r.get("stk_cd") or "").strip()
            stk_nm = (r.get("stk_nm") or "").strip()
            crd_class = (r.get("crd_class") or "").strip()
            # crd_tp = (r.get("crd_tp") or "").strip()

            # key = (stk_cd, crd_tp)
            key = (stk_cd, crd_class)
            pos_qty.setdefault(key, 0)
            episode_seq.setdefault(key, 0)

            dt = _combine_dt(r.get("trade_date"), r.get("ord_tm"))
            qty = _to_int(r.get("cntr_qty"), 0)
            if qty <= 0:
                continue

            before = pos_qty[key]
            after = before + qty if side == "BUY" else before - qty
            pos_qty[key] = after

            # 0 -> 양수 : episode start
            if before == 0 and after > 0:
                episode_seq[key] += 1
                open_episode[key] = {
                    "stk_cd": stk_cd,
                    "stk_nm": stk_nm,
                    "crd_class": crd_class,
                    "episode_seq": episode_seq[key],
                    "start_dt": dt,
                    "start_qty": after,
                }

            # 양수 -> 0 : episode end
            if before > 0 and after == 0:
                ep = open_episode.get(key)
                if ep:
                    cur.execute(
                        insert_sql,
                        (
                            ep["stk_cd"],
                            ep["stk_nm"],
                            ep["crd_class"],
                            ep["episode_seq"],
                            ep["start_dt"],
                            dt,
                            ep["start_qty"],
                            0,
                        ),
                    )
                    open_episode.pop(key, None)

        # 아직 종료되지 않은 episode(보유중)는 end_dt=NULL로 기록
        for key, ep in open_episode.items():
            cur.execute(
                insert_sql,
                (
                    ep["stk_cd"],
                    ep["stk_nm"],
                    ep["crd_class"],
                    ep["episode_seq"],
                    ep["start_dt"],
                    None,
                    ep["start_qty"],
                    pos_qty[key],
                ),
            )

    conn.commit()
