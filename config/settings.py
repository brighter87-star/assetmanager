import os
from dotenv import load_dotenv
from dataclasses import dataclass

class Settings:
    APP_KEY = os.environ["APP_KEY"]
    SECRET_KEY = os.environ["SECRET_KEY"]
    BASE_URL = os.environ["BASE_URL"]
    SOCKET_URL = os.environ["SOCKET_URL"]
    ACNT_API_ID = os.environ["ACNT_API_ID"]