import os
import requests

tenant_id = os.getenv("PBI_TENANT_ID")
client_id = os.getenv("PBI_CLIENT_ID")
client_secret = os.getenv("PBI_CLIENT_SECRET")
group_id = os.getenv("PBI_GROUP_ID")
dataset_id = os.getenv("PBI_DATASET_ID")

# 1️⃣ Obtener token
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": "https://analysis.windows.net/powerbi/api/.default",
}

token_response = requests.post(token_url, data=token_data)
access_token = token_response.json().get("access_token")

if not access_token:
    print("❌ Error obteniendo token")
    print(token_response.text)
    exit()

print("✅ Token obtenido")

# 2️⃣ Disparar refresh
refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{group_id}/datasets/{dataset_id}/refreshes"

headers = {"Authorization": f"Bearer {access_token}"}

response = requests.post(refresh_url, headers=headers)

print("Status code:", response.status_code)
print("Response:", response.text)
