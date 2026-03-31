import time
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"
YAEYAMA_URL = "https://yaeyama.co.jp/operation.html#status"

# 本番環境
BUBBLE_URL = "https://schedules.jp/api/1.1/wf/receive_ferry_status"

# テスト環境を使う場合はこっち
# BUBBLE_URL = "https://schedules.jp/version-test/api/1.1/wf/receive_ferry_status"


def detect_anei_status():
    print("=================================")
    print("FETCHING ANEI")
    print("=================================")

    res = requests.get(ANEI_URL, timeout=30)
    print("ANEI HTTP STATUS:", res.status_code)

    soup = BeautifulSoup(res.text, "lxml")
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

    print("ANEI RAW:", status_line)
    print("ANEI STATUS:", status)

    return status


def detect_yaeyama_status():
    print("=================================")
    print("FETCHING YAEYAMA")
    print("=================================")

    res = requests.get(YAEYAMA_URL, timeout=30, verify=False)
    print("YAEYAMA HTTP STATUS:", res.status_code)

    soup = BeautifulSoup(res.text, "lxml")
    text = soup.get_text("\n", strip=True)

    # いまは簡易判定
    # 後で八重山観光フェリーのHTML構造に合わせて改善する
    status = "unknown"

    if "通常運航" in text:
        status = "normal"
    if "欠航" in text:
        status = "cancelled"

    print("YAEYAMA STATUS:", status)

    return status


def send_to_bubble(operator_name, status):
    payload = {
        "operator": operator_name,
        "status": status,
        "checked_at": datetime.now().isoformat()
    }

    print("=================================")
    print("SENDING TO BUBBLE")
    print("=================================")
    print("BUBBLE_URL:", BUBBLE_URL)
    print("PAYLOAD:", payload)

    res = requests.post(BUBBLE_URL, json=payload, timeout=30)

    print("Bubble response:", res.status_code)
    print(res.text)

    return res


def main():
    print("=================================")
    print("SCHEDULES Ferry Sync Started")
    print("=================================")

    # 1件目: 安栄観光
    anei_status = detect_anei_status()
    send_to_bubble("Anei Kanko", anei_status)

    # 同一タイミングでの連続POSTを少し避ける
    time.sleep(1)

    # 2件目: 八重山観光フェリー
    yaeyama_status = detect_yaeyama_status()
    send_to_bubble("Yaeyama Kanko Ferry", yaeyama_status)

    print("=================================")
    print("Sync finished")
    print("=================================")


if __name__ == "__main__":
    main()
