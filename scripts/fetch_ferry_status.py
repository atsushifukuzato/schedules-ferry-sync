import re
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"

print("Fetching ANEI...")
anei = requests.get(ANEI_URL, timeout=30)

soup = BeautifulSoup(anei.text, "lxml")
text = soup.get_text(" ", strip=True)

date_match = re.search(r"(\d{4}/\d{2}/\d{2})の運航状況一覧", text)
time_match = re.search(r"更新時間\s*(\d{2}:\d{2})", text)

print("=================================")
print("ANEI META")
print("=================================")

if date_match:
    print("ANEI page date:", date_match.group(1))
else:
    print("ANEI page date: NOT FOUND")

if time_match:
    print("ANEI update time:", time_match.group(1))
else:
    print("ANEI update time: NOT FOUND")
