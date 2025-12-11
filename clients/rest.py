import json

import requests
from config.settings import Settings


def get_account_balance(access_token: str):
    settings = Settings()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "api-id": settings.ACNT_API_ID,
    }

    body = {"qry_tp": "1", "dmst_stex_tp": "NXT"}

    response = requests.post(
        f"{settings.BASE_URL}/api/dostk/acnt", headers=headers, json=body
    )
    response.raise_for_status()
    return json.loads(response.text)
