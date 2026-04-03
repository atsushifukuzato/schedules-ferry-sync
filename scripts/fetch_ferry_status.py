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

# 確定
CANCELLED_KEYWORDS = ["欠航", "全便欠航", "運航停止", "中止"]

# pending は固定しすぎない。
# 必要に応じて GitHub Actions 側で追加:
# FERRY_PENDING_KEYWORDS: "未定,調整中,条件付,条件付き,一部欠航,運航可否確認中,見合わせ,一部運休,変更"
PENDING_KEYWORDS = [
    x.strip() for x in os.getenv("FERRY_PENDING_KEYWORDS", "").split(",") if x.strip()
]

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")
SYMBOL_RE = re.compile(r"[〇◯△✕×]")


@dataclass(frozen=True)
class Sailing:
    operator: str
    route_import_key: str
    departure_hhmm: str


@dataclass(frozen=True)
class AbnormalSailing:
    operator: str
    route_import_key: str
    departure_hhmm: str
    status: str
    source_url: str


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
    retries: int = 3,
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


def load_sailings(csv_path: str) -> set[Tuple[str, str, str]]:
    result: set[Tuple[str, str, str]] = set()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            operator = normalize_text(row.get("operator", ""))
            route_import_key = normalize_text(row.get("route", ""))
            departure_hhmm = normalize_text(row.get("departure_hhmm", ""))
            if operator and route_import_key and departure_hhmm:
                result.add((operator, route_import_key, departure_hhmm))
    return result


def port_to_departure_label(port_name: str) -> Optional[str]:
    mapping = {
        "石垣港離島ターミナル": "石垣発",
        "竹富港": "竹富発",
        "小浜港": "小浜発",
        "黒島港": "黒島発",
        "西表大原港": "大原発",
        "西表上原港": "上原発",
        "鳩間港": "鳩間発",
        "波照間港": "波照間発",
    }
    return mapping.get(normalize_text(port_name))


def build_route_lookup(route_csv_path: str) -> Dict[Tuple[str, str, str], str]:
    """
    key: (operator, section_label, departure_label)
    value: route_import_key
    """
    lookup: Dict[Tuple[str, str, str], str] = {}

    direct_section_label_map = {
        "石垣→竹富": "竹富航路",
        "竹富→石垣": "竹富航路",
        "石垣→小浜": "小浜航路",
        "小浜→石垣": "小浜航路",
        "石垣→黒島": "黒島航路",
        "黒島→石垣": "黒島航路",
        "石垣→西表大原": "大原航路",
        "西表大原→石垣": "大原航路",
        "石垣→西表上原": "上原航路",
        "西表上原→石垣": "上原航路",
        "石垣→鳩間": "鳩間航路",
        "鳩間→石垣": "鳩間航路",
        "石垣→波照間": "波照間航路",
        "波照間→石垣": "波照間航路",
        "西表上原→鳩間": "上原-鳩間航路",
        "鳩間→西表上原": "上原-鳩間航路",
    }

    with open(route_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            operator = normalize_text(row.get("operator", ""))
            import_key = normalize_text(row.get("import_key", ""))
            route_name = normalize_text(row.get("route_name", ""))
            from_port = normalize_text(row.get("from_port", ""))

            section_label = direct_section_label_map.get(route_name)
            departure_label = port_to_departure_label(from_port)

            if not operator or not import_key or not section_label or not departure_label:
                continue

            # ユーザー確認済み:
            # 安栄の inter-island はここでは対象にしない
            # （竹富→黒島 / 黒島→竹富 などは current page から一意に取りづらいため）
            if operator == "安栄観光" and section_label == "上原-鳩間航路":
                continue

            lookup[(operator, section_label, departure_label)] = import_key

    return lookup


def extract_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    lines = [normalize_text(x) for x in text.splitlines() if normalize_text(x)]
    return lines


def classify_status_from_symbol_or_text(text: str) -> Optional[str]:
    t = normalize_text(text)

    if "✕" in t or "×" in t:
        return "cancelled"
    if "△" in t:
        return "pending"

    for kw in CANCELLED_KEYWORDS:
        if kw in t:
            return "cancelled"

    for kw in PENDING_KEYWORDS:
        if kw in t:
            return "pending"

    return None


def find_section_ranges(lines: List[str], section_labels: List[str]) -> List[Tuple[str, int, int]]:
    positions: List[Tuple[str, int]] = []
    section_set = set(section_labels)

    for idx, line in enumerate(lines):
        line2 = line.lstrip("#").strip()
        if line2 in section_set:
            positions.append((line2, idx))

    ranges: List[Tuple[str, int, int]] = []
    for i, (label, start) in enumerate(positions):
        end = positions[i + 1][1] if i + 1 < len(positions) else len(lines)
        ranges.append((label, start, end))
    return ranges


def parse_anei_abnormal_sailings(route_lookup: Dict[Tuple[str, str, str], str], html: str) -> List[AbnormalSailing]:
    operator = "安栄観光"
    section_labels = ["波照間航路", "上原航路", "鳩間航路", "大原航路", "竹富航路", "小浜航路", "黒島航路"]
    lines = extract_lines(html)
    ranges = find_section_ranges(lines, section_labels)

    result: List[AbnormalSailing] = []

    dep_labels = {"石垣発", "波照間発", "上原発", "鳩間発", "大原発", "竹富発", "小浜発", "黒島発"}

    for section_label, start, end in ranges:
        block = lines[start:end]
        current_departure_label: Optional[str] = None

        i = 0
        while i < len(block):
            line = block[i]

            if line in dep_labels:
                current_departure_label = line
                i += 1
                continue

            if TIME_RE.match(line) and current_departure_label:
                departure_hhmm = line
                nearby = " ".join(block[i + 1:i + 4])
                status = classify_status_from_symbol_or_text(nearby)

                if status:
                    route_import_key = route_lookup.get((operator, section_label, current_departure_label))
                    if route_import_key:
                        result.append(
                            AbnormalSailing(
                                operator=operator,
                                route_import_key=route_import_key,
                                departure_hhmm=departure_hhmm,
                                status=status,
                                source_url=ANEI_URL,
                            )
                        )
                i += 1
                continue

            i += 1

    return result


def parse_ykf_abnormal_sailings(route_lookup: Dict[Tuple[str, str, str], str], html: str) -> List[AbnormalSailing]:
    operator = "八重山観光フェリー"
    section_labels = ["竹富航路", "小浜航路", "黒島航路", "西表大原航路", "西表上原航路", "鳩間航路", "上原-鳩間航路"]
    lines = extract_lines(html)
    ranges = find_section_ranges(lines, section_labels)

    result: List[AbnormalSailing] = []

    dep_label_pattern = re.compile(r"(\S+発)")
    pair_pattern = re.compile(r"([〇◯△✕×])\s*(\d{1,2}:\d{2})")

    for section_label, start, end in ranges:
        block = lines[start:end]
        departure_labels: List[str] = []

        for line in block[:6]:
            found = dep_label_pattern.findall(line)
            if found:
                departure_labels = found
                break

        if not departure_labels:
            continue

        for line in block:
            pairs = pair_pattern.findall(line)
            if not pairs:
                continue

            # 例: "× 08:00 × 09:00"
            # departure_labels の順と対応
            for idx, (symbol, departure_hhmm) in enumerate(pairs):
                if idx >= len(departure_labels):
                    continue

                status = classify_status_from_symbol_or_text(symbol)
                if not status:
                    continue

                departure_label = departure_labels[idx]
                route_import_key = route_lookup.get((operator, section_label, departure_label))
                if not route_import_key:
                    continue

                result.append(
                    AbnormalSailing(
                        operator=operator,
                        route_import_key=route_import_key,
                        departure_hhmm=departure_hhmm,
                        status=status,
                        source_url=YKF_URL,
                    )
                )

    return result


class BubbleWorkflowClient:
    def __init__(self, base_url: str, workflow_name: str, secret: str):
        self.base_url = base_url.rstrip("/")
        self.workflow_name = workflow_name
        self.secret = secret
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

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
        ferry_route_csv = env_required("FERRY_ROUTE_CSV")
        ferry_secret = env_required("FERRY_SECRET")
        workflow_name = os.getenv("WF_NAME", "receive_ferry_status").strip()

        now = now_jst()
        checked_at_iso = now.isoformat()
        service_date_iso = now.date().isoformat()

        print("=" * 60)
        print("Ferry abnormal status sync started")
        print("=" * 60)
        print(f"checked_at   : {checked_at_iso}")
        print(f"service_date : {service_date_iso}")
        print(f"sailing_csv  : {ferry_sailing_csv}")
        print(f"route_csv    : {ferry_route_csv}")
        print(f"pending words: {PENDING_KEYWORDS}")

        existing_sailings = load_sailings(ferry_sailing_csv)
        route_lookup = build_route_lookup(ferry_route_csv)
        print(f"existing sailings: {len(existing_sailings)}")
        print(f"route lookup keys: {len(route_lookup)}")

        session = build_session()
        anei_html = get_with_retry(session, ANEI_URL, retries=3, delay_sec=3, verify=True)
        ykf_html = get_with_retry(session, YKF_URL, retries=3, delay_sec=3, verify=False)

        abnormal = []
        abnormal.extend(parse_anei_abnormal_sailings(route_lookup, anei_html))
        abnormal.extend(parse_ykf_abnormal_sailings(route_lookup, ykf_html))

        # CSV に存在する便だけ残す
        dedup: Dict[Tuple[str, str, str], AbnormalSailing] = {}
        for item in abnormal:
            key = (item.operator, item.route_import_key, item.departure_hhmm)
            if key in existing_sailings:
                dedup[key] = item

        final_items = list(dedup.values())
        print(f"abnormal sailings detected: {len(final_items)}")

        if not final_items:
            print("No abnormal sailings found. Nothing to send.")
            return 0

        bubble = BubbleWorkflowClient(
            base_url=bubble_base_url,
            workflow_name=workflow_name,
            secret=ferry_secret,
        )

        sent = 0
        for item in final_items:
            bubble.send_status(
                operator=item.operator,
                status=item.status,
                checked_at_iso=checked_at_iso,
                service_date_iso=service_date_iso,
                source_url=item.source_url,
                route_import_key=item.route_import_key,
                departure_hhmm=item.departure_hhmm,
            )
            sent += 1
            print(
                f"SENT  operator={item.operator} "
                f"route_import_key={item.route_import_key} "
                f"departure_hhmm={item.departure_hhmm} "
                f"status={item.status}"
            )

        print("=" * 60)
        print("Done")
        print("=" * 60)
        print(f"sent: {sent}")
        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
