import requests
from config.settings import Settings

def get_account_balance(access_token: str):
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "api-id": Settings.ACNT_API_ID
    }

    body = {
        "qry_tp": "1",
        "dmst_stex_tp": "NXT"
    }

    response = requests.post(
        f"{Settings.BASE_URL}/api/dostk/acnt",
        headers=headers,
        json=body
    )

    print("BODY:", response.text) 
    response.raise_for_status()






