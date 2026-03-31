import re
import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"

anei = requests.get(ANEI_URL, timeout=30)

soup = BeautifulSoup(anei.text, "lxml")
text = soup.get_text("\n", strip=True)

print("=================================")
print("ANEI STATUS")
print("=================================")

for line in text.split("\n"):
    if "高速船" in line:
        print(line)
