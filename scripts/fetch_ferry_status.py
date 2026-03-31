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


def get_yaeyama_response():
    last_error = None

    for attempt in range(1, 4):
        try:
            print(f"YAEYAMA fetch attempt {attempt}/3")
            res = requests.get(YAEYAMA_URL, timeout=30, verify=False)
            print("YAEYAMA HTTP STATUS:", res.status_code)
            res.encoding = res.apparent_encoding
            return res
        except requests.RequestException as e:
            last_error = e
            print(f"YAEYAMA fetch failed on attempt {attempt}: {e}")
            time.sleep(3)

    raise last_error


def detect_yaeyama_status():
    print("=================================")
    print("FETCHING YAEYAMA")
    print("=================================")

    res = get_yaeyama_response()

    soup = BeautifulSoup(res.text, "lxml")
    text = soup.get_text("\n", strip=True)

    circle_count = 0
    cross_count = 0

    print("=================================")
    print("YAEYAMA STATUS LINES")
    print("=================================")

    for line in text.split("\n"):
        line = line.strip()
        if not line:
            continue

        # 運航状況行だけ見る
        if line.startswith("〇") or line.startswith("○"):
            circle_count += 1
            print(line)
        elif line.startswith("×") or line.startswith("✕"):
            cross_count += 1
            print(line)

    print("YAEYAMA CIRCLE COUNT:", circle_count)
    print("YAEYAMA CROSS COUNT:", cross_count)

    if cross_count == 0 and circle_count > 0:
        status = "normal"
    elif cross_count > 0 and circle_count > 0:
        status = "partial"
    elif cross_count > 0 and circle_count == 0:
        status = "cancelled"
    else:
        status = "unknown"

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

    anei_status = detect_anei_status()
    send_to_bubble("Anei Kanko", anei_status)

    time.sleep(1)

    yaeyama_status = detect_yaeyama_status()
    send_to_bubble("Yaeyama Kanko Ferry", yaeyama_status)

    print("=================================")
    print("Sync finished")
    print("=================================")


if __name__ == "__main__":
    main()
