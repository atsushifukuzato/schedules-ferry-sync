import re
import requests
import urllib3
from bs4 import BeautifulSoup
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"
BUBBLE_URL = "https://schedules.jp/api/1.1/wf/receive_ferry_status"

anei = requests.get(ANEI_URL, timeout=30)

soup = BeautifulSoup(anei.text, "lxml")
text = soup.get_text("\n", strip=True)

status_line = ""

for line in text.split("\n"):
    if "高速船" in line:
        status_line = line
        break

status = "unknown"

if "通常運航" in status_line:
    status = "normal"

if "一部" in status_line:
    status = "partial"

if "欠航" in status_line:
    status = "cancelled"

if "上原" in status_line:
    status = "uehara_cancelled"

print("STATUS:", status)

payload = {
    "company": "anei",
    "status": status,
    "source_checked_at": datetime.now().isoformat()
}

print("Sending to Bubble...")
res = requests.post(BUBBLE_URL, json=payload)

print("Bubble response:", res.status_code)
print(res.text)
