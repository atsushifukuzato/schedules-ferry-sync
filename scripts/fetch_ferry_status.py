#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests
import urllib3
from bs4 import BeautifulSoup

JST = ZoneInfo("Asia/Tokyo")

ANEI_URL = "https://aneikankou.co.jp/condition"
YKF_URL = "https://yaeyama.co.jp/operation.html#status"
DEFAULT_TIMEOUT = 30

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class Sailing:
    operator: str
    route_import_key: str
    departure_hhmm: str


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"環境変数 {name} が未設定です。")
    return value


def now_jst() -> datetime:
    return datetime.now(JST)


def normalize_text(text: str) -> str:
    text = (text or "").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            )
        }
    )
    return session


def get_with_retry(
    session: requests.Session,
    url: str,
    retries: int = 2,
    delay_sec: int = 3,
    verify: bool = True,
) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=DEFAULT_TIMEOUT, verify=verify)
            response.raise_for_status()
            return response.text
        except Exception as e:
            last_error = e
            if attempt < retries:
                time.sleep(delay_sec)
    raise RuntimeError(f"取得失敗: {url} / {last_error}")


def load_sailings(csv_path: str) -> List[Sailing]:
    sailings: List[Sailing] = []
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            operator = normalize_text(row.get("operator", ""))
            route_import_key = normalize_text(row.get("route", ""))
            departure_hhmm = normalize_text(row.get("departure_hhmm", ""))

            if not operator or not route_import_key or not departure_hhmm:
                continue

            sailings.append(
                Sailing(
                    operator=operator,
                    route_import_key=route_import_key,
                    departure_hhmm=departure_hhmm,
                )
            )
    return sailings


def extract_anei_summary_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    full_text = normalize_text(soup.get_text(" ", strip=True))

    patterns = [
        r"【高速船】[^。]*。",
        r"【高速船】[^ ]*",
        r"全航路[^。]*。",
        r"全航路[^ ]*",
    ]
    for pattern in patterns:
        match = re.search(pattern, full_text)
        if match:
            return normalize_text(match.group(0))

    return full_text[:500]


def parse_anei_status(summary_text: str) -> str:
    text = normalize_text(summary_text)
    if "欠航" in text:
        return "cancelled"
    return "pending"


def parse_ykf_route_statuses(html: str) -> Dict[str, str]:
    """
    八重山観光フェリーのページから航路ごとの状態を抽出。
    戻り値のキーは route_import_key。
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [normalize_text(line) for line in text.splitlines() if normalize_text(line)]

    route_name_to_import_key = {
        "竹富航路": "yaeyama-kanko-ferry__石垣→竹富",
        "小浜航路": "yaeyama-kanko-ferry__石垣→小浜",
        "黒島航路": "yaeyama-kanko-ferry__石垣→黒島",
        "西表大原航路": "yaeyama-kanko-ferry__石垣→西表大原",
        "西表上原航路": "yaeyama-kanko-ferry__石垣→西表上原",
        "鳩間航路": "yaeyama-kanko-ferry__石垣→鳩間",
        "上原-鳩間航路": "yaeyama-kanko-ferry__西表上原→鳩間",
    }

    results: Dict[str, str] = {}
    current_import_key: Optional[str] = None
    buffer: List[str] = []

    def flush() -> None:
        nonlocal current_import_key, buffer
        if not current_import_key:
            return
        note = normalize_text(" ".join(buffer))
        status = "cancelled" if "欠航" in note else "pending"
        results[current_import_key] = status
        buffer = []

    for line in lines:
        if line in route_name_to_import_key:
            flush()
            current_import_key = route_name_to_import_key[line]
            buffer = []
            continue

        if current_import_key:
            buffer.append(line)

    flush()
    return results


def build_status_map_for_today() -> Dict[Tuple[str, str], Tuple[str, str]]:
    """
    戻り値:
    {
      (operator, route_import_key): (status, source_url)
    }
    """
    session = build_session()

    anei_html = get_with_retry(session, ANEI_URL, retries=2, delay_sec=3, verify=True)
    anei_summary = extract_anei_summary_text(anei_html)
    anei_status = parse_anei_status(anei_summary)

    ykf_html = get_with_retry(session, YKF_URL, retries=2, delay_sec=3, verify=False)
    ykf_route_statuses = parse_ykf_route_statuses(ykf_html)

    status_map: Dict[Tuple[str, str], Tuple[str, str]] = {}

    status_map[("安栄観光", "__DEFAULT__")] = (anei_status, ANEI_URL)

    for route_import_key, status in ykf_route_statuses.items():
        status_map[("八重山観光フェリー", route_import_key)] = (status, YKF_URL)

    return status_map


class BubbleWorkflowClient:
    def __init__(self, base_url: str, workflow_name: str, secret: str):
        self.base_url = base_url.rstrip("/")
        self.workflow_name = workflow_name
        self.secret = secret
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Content-Type": "application/json",
            }
        )

    def send_status(
        self,
        operator: str,
        status: str,
        checked_at_iso: str,
        service_date_iso: str,
        source_url: str,
        route_import_key: str,
        departure_hhmm: str,
    ) -> None:
        url = f"{self.base_url}/{self.workflow_name}"
        payload = {
            "secret": self.secret,
            "operator": operator,
            "status": status,
            "checked_at": checked_at_iso,
            "service_date": service_date_iso,
            "source_url": source_url,
            "route_import_key": route_import_key,
            "departure_hhmm": departure_hhmm,
        }
        response = self.session.post(url, json=payload, timeout=DEFAULT_TIMEOUT)
        response.raise_for_status()


def main() -> int:
    try:
        bubble_base_url = env_required("BUBBLE_BASE_URL")
        ferry_sailing_csv = env_required("FERRY_SAILING_CSV")
        ferry_secret = env_required("FERRY_SECRET")
        workflow_name = os.getenv("WF_NAME", "receive_ferry_status").strip()

        now = now_jst()
        checked_at_iso = now.isoformat()
        service_date_iso = now.date().isoformat()

        print("=" * 50)
        print("SCHEDULES Ferry Sync Started")
        print("=" * 50)
        print(f"checked_at  : {checked_at_iso}")
        print(f"service_date: {service_date_iso}")
        print(f"csv_path    : {ferry_sailing_csv}")

        sailings = load_sailings(ferry_sailing_csv)
        print(f"sailings loaded: {len(sailings)}")

        status_map = build_status_map_for_today()
        print(f"status_map size: {len(status_map)}")

        bubble = BubbleWorkflowClient(
            base_url=bubble_base_url,
            workflow_name=workflow_name,
            secret=ferry_secret,
        )

        sent = 0
        skipped = 0

        print("=" * 50)
        print("SEND EACH SAILING")
        print("=" * 50)

        for sailing in sailings:
            if sailing.operator == "安栄観光":
                row = status_map.get(("安栄観光", "__DEFAULT__"))
            elif sailing.operator == "八重山観光フェリー":
                row = status_map.get(("八重山観光フェリー", sailing.route_import_key))
            else:
                row = None

            if row is None:
                skipped += 1
                print(
                    f"SKIP  operator={sailing.operator} "
                    f"route_import_key={sailing.route_import_key} "
                    f"departure_hhmm={sailing.departure_hhmm}"
                )
                continue

            status, source_url = row

            bubble.send_status(
                operator=sailing.operator,
                status=status,
                checked_at_iso=checked_at_iso,
                service_date_iso=service_date_iso,
                source_url=source_url,
                route_import_key=sailing.route_import_key,
                departure_hhmm=sailing.departure_hhmm,
            )
            sent += 1
            print(
                f"SENT  operator={sailing.operator} "
                f"route_import_key={sailing.route_import_key} "
                f"departure_hhmm={sailing.departure_hhmm} "
                f"status={status}"
            )

        print("=" * 50)
        print("DONE")
        print("=" * 50)
        print(f"sent   : {sent}")
        print(f"skipped: {skipped}")
        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
