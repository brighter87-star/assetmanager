from clients.rest import get_realized_pnl_daily
from db.connection import get_connection
from services.asset_service import save_account_data, save_realized_pnl_daily


def main():
    # data = fetch_my_assets()
    date = "20251211"
    data = get_realized_pnl_daily(date)
    conn = get_connection()
    try:
        # save_account_data(conn, data)
        save_realized_pnl_daily(conn, data, query_date=date)
        print("DB 저장 완료")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
