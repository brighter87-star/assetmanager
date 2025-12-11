from auth.kiwoom_auth import get_access_token
from clients.rest import get_account_balance

def fetch_my_assets():
    token = get_access_token()
    return get_account_balance(token)
