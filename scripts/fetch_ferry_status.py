#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_ferry_status.py

確認済み仕様のみで作成
- 安栄観光 / 八重山観光フェリーの運航状況ページを取得
- FerrySailing CSV を読み込む
- Bubble Backend workflow `receive_ferry_status` に POST する
- Bubble Data API /obj/... は使わない

確認済み事項
- API workflow 名: receive_ferry_status
- 送信パラメータ:
    operator (text)
    status (text)
    checked_at (date)
    service_date (date)
    source_url (text)
    route_import_key (text)
    departure_hhmm (text)
- route_import_key に渡す値は import_key
- FerrySailing CSV には import_key 列はない
- FerrySailing CSV の route 列の値は、FerryRoute.import_key と一致する
- service_date は date 型で送る
- checked_at は毎回 Python から送る
- source_url は operator ごとに固定
"""

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
from bs4 import BeautifulSoup

JST = ZoneInfo("Asia/Tokyo")

ANEI_URL = "https://aneikankou.co.jp/condition"
YKF_URL = "https://yaeyama.co.jp/operation.html#status"
DEFAULT_TIMEOUT = 30


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


def get_with_retry(session: requests.Session, url: str, retries: int = 2, delay_sec: int = 3) -> str:
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            response = session.get(url, timeout=DEFAULT_TIMEOUT)
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


def parse_anei_status(summary_text: str) -> Tuple[str, str]:
    text = normalize_text(summary_text)
    if "欠航" in text:
        return "cancelled", text
    return "pending", text


def parse_ykf_route_statuses(html: str) -> Dict[str, Dict[str, str]]:
    """
    八重山観光フェリーのページから、航路ラベルごとの状態を抽出する。
    返り値のキーは route_import_key ではなく route_name 相当の簡易ラベル。
    その後 ROUTE_NAME_TO_IMPORT_KEY で import_key に変換する。
    """
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [normalize_text(line) for line in text.splitlines() if normalize_text(line)]

    alias_to_route_name = {
        "竹富航路": "石垣→竹富",
        "小浜航路": "石垣→小浜",
        "黒島航路": "石垣→黒島",
        "西表大原航路": "石垣→西表大原",
        "西表上原航路": "石垣→西表上原",
        "鳩間航路": "石垣→鳩間",
        "上原-鳩間航路": "西表上原→鳩間",
    }

    results: Dict[str, Dict[str, str]] = {}
    current_route_name: Optional[str] = None
    buffer: List[str] = []

    def flush() -> None:
        nonlocal current_route_name, buffer
        if not current_route_name:
            return
        note = normalize_text(" ".join(buffer))
        status = "cancelled" if "欠航" in note else "pending"
        results[current_route_name] = {"status": status, "note": note}
        buffer = []

    for line in lines:
        if line in alias_to_route_name:
            flush()
            current_route_name = alias_to_route_name[line]
            buffer = []
            continue

        if current_route_name:
            buffer.append(line)

    flush()
    return results


ROUTE_NAME_TO_IMPORT_KEY = {
    ("安栄観光", "石垣→竹富"): "anei-kanko__石垣→竹富",
    ("安栄観光", "石垣→小浜"): "anei-kanko__石垣→小浜",
    ("安栄観光", "石垣→黒島"): "anei-kanko__石垣→黒島",
    ("安栄観光", "石垣→西表大原"): "anei-kanko__石垣→西表大原",
    ("安栄観光", "石垣→西表上原"): "anei-kanko__石垣→西表上原",
    ("安栄観光", "石垣→鳩間"): "anei-kanko__石垣→鳩間",
    ("安栄観光", "西表上原→鳩間"): "anei-kanko__西表上原→鳩間",

    ("八重山観光フェリー", "石垣→竹富"): "yaeyama-kanko-ferry__石垣→竹富",
    ("八重山観光フェリー", "石垣→小浜"): "yaeyama-kanko-ferry__石垣→小浜",
    ("八重山観光フェリー", "石垣→黒島"): "yaeyama-kanko-ferry__石垣→黒島",
    ("八重山観光フェリー", "石垣→西表大原"): "yaeyama-kanko-ferry__石垣→西表大原",
    ("八重山観光フェリー", "石垣→西表上原"): "yaeyama-kanko-ferry__石垣→西表上原",
    ("八重山観光フェリー", "石垣→鳩間"): "yaeyama-kanko-ferry__石垣→鳩間",
    ("八重山観光フェリー", "西表上原→鳩間"): "yaeyama-kanko-ferry__西表上原→鳩間",
}


def build_status_map_for_today() -> Dict[Tuple[str, str], Tuple[str, str, str]]:
    """
    戻り値:
    {
      (operator, route_import_key): (status, note, source_url)
    }
    """
    session = build_session()

    anei_html = get_with_retry(session, ANEI_URL, retries=2, delay_sec=3)
    anei_summary = extract_anei_summary_text(anei_html)
    anei_status, anei_note = parse_anei_status(anei_summary)

    ykf_html = get_with_retry(session, YKF_URL, retries=2, delay_sec=3)
    ykf_route_name_statuses = parse_ykf_route_statuses(ykf_html)

    status_map: Dict[Tuple[str, str], Tuple[str, str, str]] = {}

    # 安栄観光はページ全体の判定をそのまま全航路に適用
    for (operator, route_name), route_import_key in ROUTE_NAME_TO_IMPORT_KEY.items():
        if operator == "安栄観光":
            status_map[(operator, route_import_key)] = (anei_status, anei_note, ANEI_URL)

    # 八重山観光フェリーは航路ごとの判定
    for route_name, row in ykf_route_name_statuses.items():
        key = ("八重山観光フェリー", route_name)
        route_import_key = ROUTE_NAME_TO_IMPORT_KEY.get(key)
        if not route_import_key:
            continue
        status_map[("八重山観光フェリー", route_import_key)] = (row["status"], row["note"], YKF_URL)

    return status_map


class BubbleWorkflowClient:
    def __init__(self, base_url: str, workflow_name: str, api_token: str):
        self.base_url = base_url.rstrip("/")
        self.workflow_name = workflow_name
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {api_token}",
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
        bubble_api_token = env_required("BUBBLE_API_TOKEN")
        ferry_sailing_csv = env_required("FERRY_SAILING_CSV")
        workflow_name = os.getenv("WF_NAME", "receive_ferry_status").strip()

        now = now_jst()
        checked_at_iso = now.isoformat()
        service_date_iso = now.date().isoformat()

        print("=" * 50)
        print("SCHEDULES Ferry Sync Started")
        print("=" * 50)
        print(f"checked_at : {checked_at_iso}")
        print(f"service_date: {service_date_iso}")
        print(f"csv_path   : {ferry_sailing_csv}")

        sailings = load_sailings(ferry_sailing_csv)
        print(f"sailings loaded: {len(sailings)}")

        status_map = build_status_map_for_today()
        print(f"status_map size: {len(status_map)}")

        bubble = BubbleWorkflowClient(
            base_url=bubble_base_url,
            workflow_name=workflow_name,
            api_token=bubble_api_token,
        )

        sent = 0
        skipped = 0

        print("=" * 50)
        print("SEND EACH SAILING")
        print("=" * 50)

        for sailing in sailings:
            key = (sailing.operator, sailing.route_import_key)
            row = status_map.get(key)

            if row is None:
                skipped += 1
                print(
                    f"SKIP  operator={sailing.operator} "
                    f"route_import_key={sailing.route_import_key} "
                    f"departure_hhmm={sailing.departure_hhmm}"
                )
                continue

            status, note, source_url = row
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
