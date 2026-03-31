import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"

print("Fetching ANEI...")
anei = requests.get(ANEI_URL, timeout=30)

soup = BeautifulSoup(anei.text, "lxml")

text = soup.get_text()

print("=================================")
print("ANEI TEXT SAMPLE")
print("=================================")

print(text[:1000])
