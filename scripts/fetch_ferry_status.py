import re
import time
from datetime import datetime, timezone, timedelta

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

JST = timezone(timedelta(hours=9))

ANEI_URL = "https://aneikankou.co.jp/condition"
YAEYAMA_URL = "https://yaeyama.co.jp/operation.html#status"

BUBBLE_URL = "https://schedules.jp/api/1.1/wf/receive_ferry_status"


def today_iso_date():
    return datetime.now(JST).date().isoformat()


def now_iso_jst():
    return datetime.now(JST).isoformat()


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

    if "運航未定" in status_line or "判断" in status_line:
        status = "pending"
    elif "通常運航" in status_line:
        status = "normal"
    elif "一部" in status_line:
        status = "partial"
    elif "欠航" in status_line:
        status = "cancelled"
    elif "上原" in status_line:
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


def normalize_mark(mark):
    if mark in ("〇", "○"):
        return "circle"
    if mark in ("×", "✕", "✖"):
        return "cross"
    if mark in ("△", "▲"):
        return "pending"
    return ""


def extract_status_and_time(line):
    """
    例:
      '〇 08:30'
      '× 09:00'
      '△ 11:00'
      '△ 11:00 運航判断'
      '11:00 運航未定'
    などを拾う
    """
    line = line.strip()

    # 記号つき
    m = re.match(r"^\s*([〇○×✕✖△▲])\s*([0-2]?\d:\d{2})(?:\s+.*)?$", line)
    if m:
        status = normalize_mark(m.group(1))
        departure_hhmm = m.group(2)
        if len(departure_hhmm) == 4:
            departure_hhmm = f"0{departure_hhmm}"
        return status, departure_hhmm

    # 記号なし + 未定/判断/調整中
    if ("未定" in line) or ("判断" in line) or ("調整中" in line):
        m2 = re.search(r"([0-2]?\d:\d{2})", line)
        if m2:
            departure_hhmm = m2.group(1)
            if len(departure_hhmm) == 4:
                departure_hhmm = f"0{departure_hhmm}"
            return "pending", departure_hhmm

    return None, None


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
        "その他航路は通常運航を予定しています。",
    }

    for line in lines:
        if "航路" not in line:
            continue
        if len(line) > 20:
            continue
        if line in exclude_names:
            continue
        if line not in route_names:
            route_names.append(line)

    print("=================================")
    print("YAEYAMA ROUTE NAMES")
    print("=================================")
    for name in route_names:
        print(name)

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

    print("=================================")
    print("YAEYAMA ABNORMAL SAILINGS")
    print("=================================")

    cancelled_sailings = []
    pending_sailings = []
    circle_count = 0
    cross_count = 0
    pending_count = 0

    for i, line in enumerate(lines):
        if line not in route_names:
            continue

        route_name = line
        route_import_key = route_key_map.get(route_name, "")

        for next_line in lines[i + 1:i + 40]:
            if next_line in route_names:
                break

            status_kind, departure_hhmm = extract_status_and_time(next_line)
            if not status_kind or not departure_hhmm:
                continue

            if status_kind == "circle":
                circle_count += 1

            elif status_kind == "cross":
                cross_count += 1
                cancelled_sailings.append({
                    "route_name": route_name,
                    "route_import_key": route_import_key,
                    "departure_hhmm": departure_hhmm,
                })
                print(
                    "CANCELLED:",
                    route_name,
                    "->",
                    departure_hhmm,
                    "->",
                    route_import_key
                )

            elif status_kind == "pending":
                pending_count += 1
                pending_sailings.append({
                    "route_name": route_name,
                    "route_import_key": route_import_key,
                    "departure_hhmm": departure_hhmm,
                })
                print(
                    "PENDING:",
                    route_name,
                    "->",
                    departure_hhmm,
                    "->",
                    route_import_key
                )

    print("YAEYAMA CIRCLE COUNT:", circle_count)
    print("YAEYAMA CROSS COUNT:", cross_count)
    print("YAEYAMA PENDING COUNT:", pending_count)

    if cross_count == 0 and pending_count == 0 and circle_count > 0:
        status = "normal"
    elif cross_count > 0 and pending_count == 0 and circle_count == 0:
        status = "cancelled"
    elif pending_count > 0 and cross_count == 0 and circle_count == 0:
        status = "pending"
    elif cross_count > 0 or pending_count > 0:
        status = "partial"
    else:
        status = "unknown"

    print("YAEYAMA STATUS:", status)

    return status, cancelled_sailings, pending_sailings


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
        "checked_at": now_iso_jst(),
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
    try:
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

    except Exception as e:
        print("ANEI fetch/parse failed -> skip")
        print("ERROR:", repr(e))

    time.sleep(1)

    # 八重山観光フェリー
    try:
        yaeyama_status, cancelled_sailings, pending_sailings = detect_yaeyama_status()

        print("YAEYAMA CANCELLED SAILINGS:", cancelled_sailings)
        print("YAEYAMA PENDING SAILINGS:", pending_sailings)

        if yaeyama_status == "normal":
            print("YAEYAMA is normal -> skip save")

        else:
            if cancelled_sailings:
                print("YAEYAMA will send cancelled sailings count:", len(cancelled_sailings))
                for sailing_data in cancelled_sailings:
                    print("YAEYAMA CANCELLED SEND TARGET:", sailing_data)
                    send_to_bubble(
                        operator_name="Yaeyama Kanko Ferry",
                        status="cancelled",
                        service_date=service_date,
                        source_url=YAEYAMA_URL,
                        route_import_key=sailing_data.get("route_import_key", ""),
                        departure_hhmm=sailing_data.get("departure_hhmm", "")
                    )
                    time.sleep(0.3)

            if pending_sailings:
                print("YAEYAMA will send pending sailings count:", len(pending_sailings))
                for sailing_data in pending_sailings:
                    print("YAEYAMA PENDING SEND TARGET:", sailing_data)
                    send_to_bubble(
                        operator_name="Yaeyama Kanko Ferry",
                        status="pending",
                        service_date=service_date,
                        source_url=YAEYAMA_URL,
                        route_import_key=sailing_data.get("route_import_key", ""),
                        departure_hhmm=sailing_data.get("departure_hhmm", "")
                    )
                    time.sleep(0.3)

            if not cancelled_sailings and not pending_sailings:
                print("YAEYAMA abnormal but no abnormal sailings parsed -> fallback send")
                send_to_bubble(
                    operator_name="Yaeyama Kanko Ferry",
                    status=yaeyama_status,
                    service_date=service_date,
                    source_url=YAEYAMA_URL
                )

    except Exception as e:
        print("YAEYAMA fetch/parse failed -> skip")
        print("ERROR:", repr(e))

    print("=================================")
    print("Sync finished")
    print("=================================")


if __name__ == "__main__":
    main()
