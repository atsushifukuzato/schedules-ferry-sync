import time
from datetime import datetime

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ANEI_URL = "https://aneikankou.co.jp/condition"
YAEYAMA_URL = "https://yaeyama.co.jp/operation.html#status"

BUBBLE_URL = "https://schedules.jp/api/1.1/wf/receive_ferry_status"


def today_iso_date():
    return datetime.now().date().isoformat()


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

    for attempt in range(1, 3):
        try:
            print(f"YAEYAMA fetch attempt {attempt}/2")
            res = requests.get(YAEYAMA_URL, timeout=10, verify=False)
            print("YAEYAMA HTTP STATUS:", res.status_code)
            res.encoding = res.apparent_encoding
            return res

        except requests.RequestException as e:
            last_error = e
            print(f"YAEYAMA fetch failed on attempt {attempt}: {e}")
            time.sleep(2)

    raise last_error


def detect_yaeyama_status():
    print("=================================")
    print("FETCHING YAEYAMA")
    print("=================================")

    res = get_yaeyama_response()

    soup = BeautifulSoup(res.text, "lxml")
    text = soup.get_text("\n", strip=True)

    lines = [line.strip() for line in text.split("\n") if line.strip()]

    route_names = []

    exclude_names = {
        "離島定期航路",
        "航路図",
        "離島航路図",
        "上原航路欠航時バス",
        "航路",
    }

    for line in lines:
        if "航路" not in line:
            continue
        if len(line) > 20:
            continue
        if line in exclude_names:
            continue
        route_names.append(line)

    print("=================================")
    print("YAEYAMA ROUTE NAMES")
    print("=================================")

    for name in route_names:
        print(name)

    # 航路 → import_key マッピング
    route_key_map = {
        "竹富航路": "yaeyama-kanko-ferry__石垣→竹富",
        "小浜航路": "yaeyama-kanko-ferry__石垣→小浜",
        "黒島航路": "yaeyama-kanko-ferry__石垣→黒島",
        "西表大原航路": "yaeyama-kanko-ferry__石垣→西表大原",
        "西表上原航路": "yaeyama-kanko-ferry__石垣→西表上原",
        "鳩間航路": "yaeyama-kanko-ferry__石垣→鳩間",
        "上原-鳩間航路": "yaeyama-kanko-ferry__西表上原→鳩間",
    }

    print("=================================")
    print("YAEYAMA ROUTE IMPORT KEYS")
    print("=================================")

    for name in route_names:
        print(name, "->", route_key_map.get(name, "NOT FOUND"))

    circle_count = text.count("〇")
    cross_count = text.count("×")

    print("YAEYAMA CIRCLE COUNT:", circle_count)
    print("YAEYAMA CROSS COUNT:", cross_count)

    if cross_count > 0:
        return "cancelled"

    return "normal"

    print("=================================")
    print("YAEYAMA ROUTE NAMES")
    print("=================================")

    for name in route_names:
        print(name)

    circle_count = 0
    cross_count = 0

    for line in lines:
        if line.startswith("〇") or line.startswith("○"):
            circle_count += 1
        elif line.startswith("×") or line.startswith("✕"):
            cross_count += 1

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


def send_to_bubble(
    operator_name,
    status,
    service_date,
    source_url,
    route_import_key="",
    departure_hhmm=""
):
    payload = {
        "operator": operator_name,
        "status": status,
        "checked_at": datetime.now().isoformat(),
        "service_date": service_date,
        "source_url": source_url,
        "route_import_key": route_import_key,
        "departure_hhmm": departure_hhmm
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

    service_date = today_iso_date()

    # 安栄観光
    anei_status = detect_anei_status()

    if anei_status != "normal":
        send_to_bubble(
            operator_name="Anei Kanko",
            status=anei_status,
            service_date=service_date,
            source_url=ANEI_URL
        )
    else:
        print("ANEI is normal -> skip save")

    time.sleep(1)

    # 八重山観光フェリー
    yaeyama_status = detect_yaeyama_status()

    if yaeyama_status != "normal":
        send_to_bubble(
            operator_name="Yaeyama Kanko Ferry",
            status=yaeyama_status,
            service_date=service_date,
            source_url=YAEYAMA_URL,
            route_import_key="yaeyama-kanko-ferry__鳩間→西表上原",
            departure_hhmm="16:40"
        )
    else:
        print("YAEYAMA is normal -> skip save")

    print("=================================")
    print("Sync finished")
    print("=================================")


if __name__ == "__main__":
    main()
