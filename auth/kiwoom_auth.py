import requests
from config.settings import Settings


def get_access_token() -> str:
    settings = Settings()
    url = f"{settings.BASE_URL}/oauth2/token"

    payload = {
        "grant_type": "client_credentials",
        "appkey": settings.APP_KEY,
        "secretkey": settings.SECRET_KEY,
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    return response.json()["token"]
