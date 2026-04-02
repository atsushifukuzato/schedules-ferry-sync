import os
import re
from datetime import datetime, timezone, timedelta

import requests
from bs4 import BeautifulSoup


# =========================================================
# 設定
# =========================================================

JST = timezone(timedelta(hours=9))

ANEI_URL = "https://aneikankou.co.jp/condition"
YAEYAMA_URL = "https://yaeyama.co.jp/operation.html#status"

BUBBLE_BASE_URL = os.environ["BUBBLE_BASE_URL"].rstrip("/")
BUBBLE_API_TOKEN = os.environ["BUBBLE_API_TOKEN"]

BUBBLE_WORKFLOW_URL = f"{BUBBLE_BASE_URL}/api/1.1/wf/receive_ferry_status"


SYMBOL_TO_STATUS = {
    "◯": "normal",
    "〇": "normal",
    "△": "pending",
    "✕": "cancelled",
    "×": "cancelled",
}


TIME_RE = re.compile(r"^\d{2}:\d{2}$")
DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")
UPDATE_RE = re.compile(r"更新時間\s*(\d{1,2}):(\d{2})")


# =========================================================
# 航路定義
# =========================================================

ANEI_ROUTES = {
    "波照間航路": {
        "out": "anei-kanko__石垣→波照間",
        "in": "anei-kanko__波照間→石垣",
    },
    "上原航路": {
        "out": "anei-kanko__石垣→西表上原",
        "in": "anei-kanko__西表上原→石垣",
    },
    "大原航路": {
        "out": "anei-kanko__石垣→西表大原",
        "in": "anei-kanko__西表大原→石垣",
    },
    "竹富航路": {
        "out": "anei-kanko__石垣→竹富",
        "in": "anei-kanko__竹富→石垣",
    },
    "小浜航路": {
        "out": "anei-kanko__石垣→小浜",
        "in": "anei-kanko__小浜→石垣",
    },
    "黒島航路": {
        "out": "anei-kanko__石垣→黒島",
        "in": "anei-kanko__黒島→石垣",
    },
    "鳩間航路": {
        "out": "anei-kanko__石垣→鳩間",
        "in": "anei-kanko__鳩間→石垣",
    },
}


YAEYAMA_ROUTES = {
    "竹富航路": {
        "out": "yaeyama-kanko-ferry__石垣→竹富",
        "in": "yaeyama-kanko-ferry__竹富→石垣",
    },
    "小浜航路": {
        "out": "yaeyama-kanko-ferry__石垣→小浜",
        "in": "yaeyama-kanko-ferry__小浜→石垣",
    },
    "黒島航路": {
        "out": "yaeyama-kanko-ferry__石垣→黒島",
        "in": "yaeyama-kanko-ferry__黒島→石垣",
    },
    "西表大原航路": {
        "out": "yaeyama-kanko-ferry__石垣→西表大原",
        "in": "yaeyama-kanko-ferry__西表大原→石垣",
    },
    "西表上原航路": {
        "out": "yaeyama-kanko-ferry__石垣→西表上原",
        "in": "yaeyama-kanko-ferry__西表上原→石垣",
    },
    "鳩間航路": {
        "out": "yaeyama-kanko-ferry__石垣→鳩間",
        "in": "yaeyama-kanko-ferry__鳩間→石垣",
    },
}


# =========================================================
# 共通
# =========================================================

def fetch(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def clean_lines(html):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style"]):
        tag.decompose()

    text = soup.get_text("\n")
    lines = []

    for l in text.splitlines():
        l = l.replace("\u3000", " ").strip()
        if l:
            lines.append(l)

    return lines


def parse_date(text):
    m = DATE_RE.search(text)
    if m:
        y, m2, d = map(int, m.groups())
        return datetime(y, m2, d, tzinfo=JST).date()

    return datetime.now(JST).date()


def parse_update(text, date):
    m = UPDATE_RE.search(text)

    if not m:
        return datetime.now(JST)

    hh, mm = map(int, m.groups())

    return datetime(
        date.year,
        date.month,
        date.day,
        hh,
        mm,
        tzinfo=JST,
    )


def send(
    operator,
    route_key,
    dep,
    status,
    date,
    checked_at,
    source,
):
    payload = {
        "operator": operator,
        "route_import_key": route_key,
        "departure_hhmm": dep,  # ← コロン付き
        "status": status,
        "service_date": datetime(
            date.year,
            date.month,
            date.day,
            tzinfo=JST
        ).isoformat(),
        "checked_at": checked_at.isoformat(),
        "source_url": source,
    }

    r = requests.post(
        BUBBLE_WORKFLOW_URL,
        headers={
            "Authorization": f"Bearer {BUBBLE_API_TOKEN}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=30,
    )

    r.raise_for_status()

    print(
        f"SENT {operator} {route_key} {dep} {status}"
    )


# =========================================================
# 安栄
# =========================================================

def sync_anei():

    print("FETCHING ANEI")

    html = fetch(ANEI_URL)
    lines = clean_lines(html)

    text = "\n".join(lines)

    date = parse_date(text)
    checked = parse_update(text, date)

    for i, line in enumerate(lines):

        if line not in ANEI_ROUTES:
            continue

        route = line
        conf = ANEI_ROUTES[route]

        block = lines[i:i+120]

        current = None

        for l in block:

            if l == "石垣発":
                current = "out"

            elif "発" in l and "石垣" not in l:
                current = "in"

            elif TIME_RE.match(l):
                dep = l

            elif l in SYMBOL_TO_STATUS and current:
                status = SYMBOL_TO_STATUS[l]

                route_key = conf[current]

                send(
                    "anei-kanko",
                    route_key,
                    dep,
                    status,
                    date,
                    checked,
                    ANEI_URL,
                )


# =========================================================
# 八重山
# =========================================================

def sync_yaeyama():

    print("FETCHING YAEYAMA")

    html = fetch(YAEYAMA_URL)
    lines = clean_lines(html)

    text = "\n".join(lines)

    date = parse_date(text)
    checked = datetime.now(JST)

    for i, line in enumerate(lines):

        if line not in YAEYAMA_ROUTES:
            continue

        conf = YAEYAMA_ROUTES[line]

        block = lines[i:i+120]

        for l in block:

            m = re.match(
                r"([◯〇△✕×])\s*(\d{2}:\d{2})\s*([◯〇△✕×])\s*(\d{2}:\d{2})",
                l,
            )

            if not m:
                continue

            s1, t1, s2, t2 = m.groups()

            send(
                "yaeyama-kanko-ferry",
                conf["out"],
                t1,
                SYMBOL_TO_STATUS[s1],
                date,
                checked,
                YAEYAMA_URL,
            )

            send(
                "yaeyama-kanko-ferry",
                conf["in"],
                t2,
                SYMBOL_TO_STATUS[s2],
                date,
                checked,
                YAEYAMA_URL,
            )


# =========================================================
# main
# =========================================================

def main():

    print("=================================")
    print("SCHEDULES Ferry Sync Started")
    print("=================================")

    sync_anei()
    sync_yaeyama()

    print("=================================")
    print("FINISHED")
    print("=================================")


if __name__ == "__main__":
    main()
