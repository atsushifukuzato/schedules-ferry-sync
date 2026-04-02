import re
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup

BUBBLE_URL = "https://schedules.jp/api/1.1/wf/receive_ferry_status"

ANEI_URL = "https://aneikankou.co.jp/condition"
YAEYAMA_URL = "https://yaeyama.co.jp/operation.html"

JST = timezone(timedelta(hours=9))

# ===============================
# 固定ターゲット
# ===============================

# 安栄 波照間 第1〜第3便
ANEI_HATERUMA = {
    "1": [
        {"route_import_key": "anei-kanko__石垣→波照間", "departure_hhmm": "08:00"},
        {"route_import_key": "anei-kanko__波照間→石垣", "departure_hhmm": "09:50"},
    ],
    "2": [
        {"route_import_key": "anei-kanko__石垣→波照間", "departure_hhmm": "11:45"},
        {"route_import_key": "anei-kanko__波照間→石垣", "departure_hhmm": "13:00"},
    ],
    "3": [
        {"route_import_key": "anei-kanko__石垣→波照間", "departure_hhmm": "15:00"},
        {"route_import_key": "anei-kanko__波照間→石垣", "departure_hhmm": "16:50"},
    ],
}

# 安栄 上原関連（鳩間・上原↔鳩間含む）
ANEI_UEHARA_RELATED = [
    # 石垣→西表上原
    {"route_import_key": "anei-kanko__石垣→西表上原", "departure_hhmm": "07:00"},
    {"route_import_key": "anei-kanko__石垣→西表上原", "departure_hhmm": "08:30"},
    {"route_import_key": "anei-kanko__石垣→西表上原", "departure_hhmm": "13:30"},
    {"route_import_key": "anei-kanko__石垣→西表上原", "departure_hhmm": "16:30"},

    # 西表上原→石垣
    {"route_import_key": "anei-kanko__西表上原→石垣", "departure_hhmm": "08:00"},
    {"route_import_key": "anei-kanko__西表上原→石垣", "departure_hhmm": "09:30"},
    {"route_import_key": "anei-kanko__西表上原→石垣", "departure_hhmm": "14:30"},
    {"route_import_key": "anei-kanko__西表上原→石垣", "departure_hhmm": "17:45"},

    # 石垣→鳩間
    {"route_import_key": "anei-kanko__石垣→鳩間", "departure_hhmm": "08:30"},
    {"route_import_key": "anei-kanko__石垣→鳩間", "departure_hhmm": "16:30"},

    # 鳩間→石垣
    {"route_import_key": "anei-kanko__鳩間→石垣", "departure_hhmm": "09:45"},
    {"route_import_key": "anei-kanko__鳩間→石垣", "departure_hhmm": "17:25"},

    # 西表上原→鳩間
    {"route_import_key": "anei-kanko__西表上原→鳩間", "departure_hhmm": "09:30"},

    # 鳩間→西表上原
    {"route_import_key": "anei-kanko__鳩間→西表上原", "departure_hhmm": "17:25"},
]

# 八重山 上原関連（鳩間・上原↔鳩間含む）
YAEYAMA_UEHARA_RELATED = [
    # 石垣→西表上原
    {"route_import_key": "yaeyama-kanko-ferry__石垣→西表上原", "departure_hhmm": "08:00"},
    {"route_import_key": "yaeyama-kanko-ferry__石垣→西表上原", "departure_hhmm": "11:00"},
    {"route_import_key": "yaeyama-kanko-ferry__石垣→西表上原", "departure_hhmm": "13:30"},
    {"route_import_key": "yaeyama-kanko-ferry__石垣→西表上原", "departure_hhmm": "15:45"},

    # 西表上原→石垣
    {"route_import_key": "yaeyama-kanko-ferry__西表上原→石垣", "departure_hhmm": "09:00"},
    {"route_import_key": "yaeyama-kanko-ferry__西表上原→石垣", "departure_hhmm": "12:00"},
    {"route_import_key": "yaeyama-kanko-ferry__西表上原→石垣", "departure_hhmm": "14:30"},
    {"route_import_key": "yaeyama-kanko-ferry__西表上原→石垣", "departure_hhmm": "17:05"},

    # 石垣→鳩間
    {"route_import_key": "yaeyama-kanko-ferry__石垣→鳩間", "departure_hhmm": "08:00"},
    {"route_import_key": "yaeyama-kanko-ferry__石垣→鳩間", "departure_hhmm": "15:45"},

    # 鳩間→石垣
    {"route_import_key": "yaeyama-kanko-ferry__鳩間→石垣", "departure_hhmm": "09:20"},
    {"route_import_key": "yaeyama-kanko-ferry__鳩間→石垣", "departure_hhmm": "16:40"},

    # 西表上原→鳩間
    {"route_import_key": "yaeyama-kanko-ferry__西表上原→鳩間", "departure_hhmm": "09:00"},

    # 鳩間→西表上原
    {"route_import_key": "yaeyama-kanko-ferry__鳩間→西表上原", "departure_hhmm": "16:40"},
]


def now_jst():
    return datetime.now(JST)


def today_iso_date():
    return now_jst().date().isoformat()


def send_to_bubble(
    operator_name: str,
    status: str,
    route_import_key: str,
    departure_hhmm: str,
    source_url: str,
) -> None:
    payload = {
        "operator": operator_name,
        "status": status,
        "checked_at": now_jst().isoformat(),
        "service_date": today_iso_date(),
        "source_url": source_url,
        "route_import_key": route_import_key,
        "departure_hhmm": departure_hhmm,
    }

    print("=================================")
    print("SENDING TO BUBBLE")
    print("=================================")
    print("PAYLOAD:", payload)

    try:
        res = requests.post(BUBBLE_URL, json=payload, timeout=30)
        print("Bubble response:", res.status_code)
        print(res.text)
    except Exception as e:
        print("Bubble send failed:", repr(e))


def send_targets(
    operator_name: str,
    status: str,
    targets: list[dict],
    source_url: str,
) -> None:
    for target in targets:
        send_to_bubble(
            operator_name=operator_name,
            status=status,
            route_import_key=target["route_import_key"],
            departure_hhmm=target["departure_hhmm"],
            source_url=source_url,
        )


def fetch_text(url: str, timeout: int = 20, verify: bool = True) -> str:
    res = requests.get(url, timeout=timeout, verify=verify)
    res.raise_for_status()
    res.encoding = res.apparent_encoding
    soup = BeautifulSoup(res.text, "lxml")
    return soup.get_text("\n", strip=True)


# ===============================
# 安栄
# ===============================

def detect_anei_hateruma_trip_statuses(text: str) -> dict[str, str]:
    """
    戻り値例:
    {
        "1": "cancelled",
        "2": "pending",
        "3": "normal"
    }
    """
    result = {"1": "normal", "2": "normal", "3": "normal"}

    for trip in ("1", "2", "3"):
        # 第N便 を含む文をざっくり拾う
        # 句点までを1まとまりとして判定
        pattern = rf"[^。]*第{trip}便[^。]*。?"
        matches = re.findall(pattern, text)

        joined = " ".join(matches)
        if not joined:
            continue

        if "未定" in joined or "判断" in joined:
            result[trip] = "pending"
        elif "欠航" in joined:
            result[trip] = "cancelled"
        else:
            result[trip] = "normal"

    return result


def detect_anei_uehara_cancelled(text: str) -> bool:
    """
    上原航路欠航系のざっくり判定
    """
    keywords = [
        "上原航路欠航",
        "上原航路、欠航",
        "上原航路 欠航",
        "西表上原",
    ]

    if not any(k in text for k in keywords):
        return False

    return "欠航" in text


def check_anei() -> None:
    print("=================================")
    print("FETCHING ANEI")
    print("=================================")

    try:
        text = fetch_text(ANEI_URL, timeout=30, verify=True)
    except Exception as e:
        print("ANEI fetch failed:", repr(e))
        return

    print("ANEI TEXT:")
    print(text)

    # 波照間 第1/2/3便
    trip_statuses = detect_anei_hateruma_trip_statuses(text)
    print("ANEI HATERUMA TRIP STATUSES:", trip_statuses)

    for trip, status in trip_statuses.items():
        if status == "normal":
            continue

        print(f"ANEI HATERUMA trip {trip} -> {status}")
        send_targets(
            operator_name="Anei Kanko",
            status=status,
            targets=ANEI_HATERUMA[trip],
            source_url=ANEI_URL,
        )

    # 上原関連
    if detect_anei_uehara_cancelled(text):
        print("ANEI UEHARA RELATED -> cancelled")
        send_targets(
            operator_name="Anei Kanko",
            status="cancelled",
            targets=ANEI_UEHARA_RELATED,
            source_url=ANEI_URL,
        )


# ===============================
# 八重山
# ===============================

def detect_yaeyama_uehara_related_abnormal(text: str) -> str | None:
    """
    上原関連に異常があるかをざっくり判定
    戻り値:
      "pending" / "cancelled" / None
    """

    # 上原・鳩間・上原-鳩間あたりに未定/判断系があれば pending
    pending_keywords = ["未定", "判断", "調整中", "△", "▲"]
    cancelled_keywords = ["欠航", "×", "✕", "✖"]

    route_keywords = ["西表上原航路", "鳩間航路", "上原-鳩間航路"]

    related_text = "\n".join(
        line for line in text.split("\n")
        if any(route in line for route in route_keywords)
        or any(route in line for route in route_keywords for _ in [0])
    )

    # route見出し以降のテキスト全体から見る
    # シンプルに全体に route名と異常語があるかで判定
    has_related = any(route in text for route in route_keywords)
    if not has_related:
        return None

    if any(k in text for k in pending_keywords):
        # pendingがある日に cancelled も混じることはあるが、
        # 今回の簡略ルールでは pending を優先させず、
        # cancelled があれば cancelled を優先
        pass

    if any(k in text for k in cancelled_keywords):
        # 上原関連 route 名も存在している前提
        return "cancelled"

    if any(k in text for k in pending_keywords):
        return "pending"

    return None


def check_yaeyama() -> None:
    print("=================================")
    print("FETCHING YAEYAMA")
    print("=================================")

    try:
        text = fetch_text(YAEYAMA_URL, timeout=20, verify=False)
    except Exception as e:
        print("YAEYAMA fetch failed:", repr(e))
        return

    print("YAEYAMA TEXT:")
    print(text)

    abnormal_status = detect_yaeyama_uehara_related_abnormal(text)
    print("YAEYAMA UEHARA RELATED STATUS:", abnormal_status)

    if abnormal_status in ("pending", "cancelled"):
        send_targets(
            operator_name="Yaeyama Kanko Ferry",
            status=abnormal_status,
            targets=YAEYAMA_UEHARA_RELATED,
            source_url=YAEYAMA_URL,
        )


def main() -> None:
    print("=================================")
    print("SCHEDULES Ferry Sync Started")
    print("=================================")

    try:
        check_anei()
    except Exception as e:
        print("ANEI ERROR:", repr(e))

    try:
        check_yaeyama()
    except Exception as e:
        print("YAEYAMA ERROR:", repr(e))

    print("=================================")
    print("Sync finished")
    print("=================================")


if __name__ == "__main__":
    main()