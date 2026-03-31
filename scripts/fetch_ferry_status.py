import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"
YAEYAMA_URL = "https://yaeyama.co.jp/operation.html#status"

print("=================================")
print("SCHEDULES Ferry Sync Started")
print("=================================")

print("Fetching ferry status...")

print("Fetching ANEI...")
anei = requests.get(ANEI_URL, timeout=30)
print("ANEI status:", anei.status_code)

print("Fetching YAEYAMA...")
yaeyama = requests.get(YAEYAMA_URL, timeout=30, verify=False)
print("YAEYAMA status:", yaeyama.status_code)

print("=================================")
print("Sync finished")
print("=================================")
