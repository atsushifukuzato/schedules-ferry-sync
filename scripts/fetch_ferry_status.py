import re
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"

anei = requests.get(ANEI_URL, timeout=30)

soup = BeautifulSoup(anei.text, "lxml")
text = soup.get_text("\n", strip=True)

status_line = ""

for line in text.split("\n"):
    if "高速船" in line:
        status_line = line
        break

print("RAW:", status_line)

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
