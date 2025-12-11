from db.connection import get_connection

from services.asset_service import fetch_my_assets, save_account_data


def main():
    data = fetch_my_assets()
    conn = get_connection()
    try:
        save_account_data(conn, data)
        print("DB 저장 완료")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
