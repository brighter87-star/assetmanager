import requests
from config.settings import Settings

def get_access_token() -> str:
    url = f"{Settings.BASE_URL}/oauth2/token"

    payload = {
        "grant_type": "client_credentials",
        "appkey": Settings.APP_KEY,
        "secretkey": Settings.SECRET_KEY
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    print(response.json())

    return response.json()["token"]
