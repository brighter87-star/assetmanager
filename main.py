from services.asset_service import fetch_my_assets

def main():
    assets = fetch_my_assets()
    print(assets)

if __name__ == "__main__":
    main()