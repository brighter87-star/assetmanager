from datetime import datetime

from clients.rest import (
    get_account_balance,
    get_account_trade_history,
    get_realized_pnl_daily,
)
from db.connection import get_connection
from services.asset_service import (
    save_account_data,
    save_account_trade_history,
    save_realized_pnl_daily,
)
from utils.krx_calendar import is_korea_trading_day_by_samsung


def main():

    if not is_korea_trading_day_by_samsung():
        print("오늘은 KRX 휴장일입니다. 스크립트를 종료합니다.")
        return
    # date = datetime.now().strftime("%Y%m%d")
    date = "20251211"
    asset_data = get_account_balance()
    pnl_data = get_realized_pnl_daily(date)
    trades_data = get_account_trade_history(ord_dt=date)
    conn = get_connection()
    try:
        save_account_data(conn, asset_data)
        save_realized_pnl_daily(conn, pnl_data, query_date=date)
        save_account_trade_history(conn, trades_data, trade_date=date)
        print("DB 저장 완료")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
