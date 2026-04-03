#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
fetch_ferry_status.py

目的:
- 安栄観光 / 八重山観光フェリーの運航状況ページを解析
- 「記号 + 時刻」単位で異常便のみ抽出
- FerrySailing マスターCSVに存在する便のみ採用
- Bubble Backend Workflow に送信
- route_import_key + departure_hhmm の完全一致前提で送る
- 誤った時刻は絶対送らない
- 一致しない便は送らない
- 複数異常便はすべて送る

必須環境変数:
- BUBBLE_WORKFLOW_URL
任意:
- BUBBLE_API_TOKEN
- REQUEST_TIMEOUT
- LOG_LEVEL

想定入力CSV:
- data/FerrySailing_COMPLETE_FINAL_with_conditions_corrected.csv

想定CSV必須列:
- operator
- route_import_key
- departure_hhmm

※ operator の表記ゆれがある場合でも route_import_key + departure_hhmm を主キーとして扱う
"""

from __future__ import annotations

import csv
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple, Set

import requests
from bs4 import BeautifulSoup


JST = ZoneInfo("Asia/Tokyo")

ANEI_URL = "https://aneikankou.co.jp/condition"
YKF_URL = "https://yaeyama.co.jp/operation.html#status"

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

FERRY_SAILING_CSV = os.getenv(
    "FERRY_SAILING_CSV",
    "data/FerrySailing_COMPLETE_FINAL_with_conditions_corrected.csv",
)

BUBBLE_WORKFLOW_URL = os.getenv("BUBBLE_WORKFLOW_URL", "").strip()
BUBBLE_API_TOKEN = os.getenv("BUBBLE_API_TOKEN", "").strip()


# ----------------------------
# Logging
# ----------------------------

def log(level: str, message: str, **kwargs) -> None:
    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    if levels.index(level) < levels.index(LOG_LEVEL):
        return
    payload = {"level": level, "message": message, **kwargs}
    print(json.dumps(payload, ensure_ascii=False), flush=True)


# ----------------------------
# Data models
# ----------------------------

@dataclass(frozen=True)
class MasterSailing:
    operator: str
    route_import_key: str
    departure_hhmm: str


@dataclass(frozen=True)
class CandidateStatus:
    operator: str
    route_import_key: str
    departure_hhmm: str
    status: str
    source_url: str
    route_label: str
    direction_label: str
    symbol: str


# ----------------------------
# Constants / mappings
# ----------------------------

STATUS_SYMBOL_MAP = {
    "◯": "normal",
    "〇": "normal",
    "△": "partial",
    "✕": "cancelled",
    "×": "cancelled",
    "―": "unknown",
    "-": "unknown",
}

# 安栄観光 航路名 + 発地/向き -> import_key
ANEI_ROUTE_DIRECTION_TO_KEY: Dict[Tuple[str, str], str] = {
    ("波照間航路", "石垣発"): "anei-kanko__石垣→波照間",
    ("波照間航路", "波照間発"): "anei-kanko__波照間→石垣",

    ("上原航路", "石垣発"): "anei-kanko__石垣→西表上原",
    ("上原航路", "上原発"): "anei-kanko__西表上原→石垣",

    ("鳩間航路", "石垣発"): "anei-kanko__石垣→鳩間",
    ("鳩間航路", "鳩間発"): "anei-kanko__鳩間→石垣",

    ("大原航路", "石垣発"): "anei-kanko__石垣→西表大原",
    ("大原航路", "大原発"): "anei-kanko__西表大原→石垣",

    ("竹富航路", "石垣発"): "anei-kanko__石垣→竹富",
    ("竹富航路", "竹富発"): "anei-kanko__竹富→石垣",

    ("小浜航路", "石垣発"): "anei-kanko__石垣→小浜",
    ("小浜航路", "小浜発"): "anei-kanko__小浜→石垣",

    ("黒島航路", "石垣発"): "anei-kanko__石垣→黒島",
    ("黒島航路", "黒島発"): "anei-kanko__黒島→石垣",
}

# 八重山観光フェリー 航路名 + 発地/向き -> import_key
YKF_ROUTE_DIRECTION_TO_KEY: Dict[Tuple[str, str], str] = {
    ("竹富航路", "石垣発"): "yaeyama-kanko-ferry__石垣→竹富",
    ("竹富航路", "竹富発"): "yaeyama-kanko-ferry__竹富→石垣",

    ("小浜航路", "石垣発"): "yaeyama-kanko-ferry__石垣→小浜",
    ("小浜航路", "小浜発"): "yaeyama-kanko-ferry__小浜→石垣",

    ("黒島航路", "石垣発"): "yaeyama-kanko-ferry__石垣→黒島",
    ("黒島航路", "黒島発"): "yaeyama-kanko-ferry__黒島→石垣",

    ("西表大原航路", "石垣発"): "yaeyama-kanko-ferry__石垣→西表大原",
    ("西表大原航路", "大原発"): "yaeyama-kanko-ferry__西表大原→石垣",

    ("西表上原航路", "石垣発"): "yaeyama-kanko-ferry__石垣→西表上原",
    ("西表上原航路", "上原発"): "yaeyama-kanko-ferry__西表上原→石垣",

    ("鳩間航路", "石垣発"): "yaeyama-kanko-ferry__石垣→鳩間",
    ("鳩間航路", "鳩間発"): "yaeyama-kanko-ferry__鳩間→石垣",

    ("上原-鳩間航路", "上原発"): "yaeyama-kanko-ferry__西表上原→鳩間",
    ("上原-鳩間航路", "鳩間発"): "yaeyama-kanko-ferry__鳩間→西表上原",
}

ANEI_ROUTE_NAMES = [
    "波照間航路",
    "上原航路",
    "鳩間航路",
    "大原航路",
    "竹富航路",
    "小浜航路",
    "黒島航路",
]

YKF_ROUTE_NAMES = [
    "竹富航路",
    "小浜航路",
    "黒島航路",
    "西表大原航路",
    "西表上原航路",
    "鳩間航路",
    "上原-鳩間航路",
]


# ----------------------------
# Utility
# ----------------------------

TIME_RE = re.compile(r"(?<!\d)(\d{1,2}:\d{2})(?!\d)")
SYMBOL_RE = re.compile(r"^[◯〇△✕×―-]$")
DATE_RE = re.compile(r"(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})")
WHITESPACE_RE = re.compile(r"\s+")

def normalize_space(text: str) -> str:
    return WHITESPACE_RE.sub(" ", text).strip()

def normalize_lines(text: str) -> List[str]:
    lines = []
    for line in text.splitlines():
        s = normalize_space(line)
        if s:
            lines.append(s)
    return lines

def extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text)
    if not m:
        return None
    hhmm = m.group(1)
    hh, mm = hhmm.split(":")
    return f"{int(hh):02d}:{mm}"

def extract_service_date(lines: List[str]) -> str:
    for line in lines[:50]:
        m = DATE_RE.search(line)
        if m:
            y, mo, d = map(int, m.groups())
            return f"{y:04d}-{mo:02d}-{d:02d}"
    # 取れなければ実行日
    now = datetime.now(JST)
    return now.strftime("%Y-%m-%d")

def requests_get(url: str) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ferry-status-bot/1.0)"
    }
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding
    return r.text

def soup_text(html: str) -> Tuple[BeautifulSoup, List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    return soup, normalize_lines(text)


# ----------------------------
# Master loading
# ----------------------------

def load_master(csv_path: str) -> Dict[Tuple[str, str], MasterSailing]:
    """
    key = (route_import_key, departure_hhmm)
    """
    master: Dict[Tuple[str, str], MasterSailing] = {}
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        required = {"operator", "route_import_key", "departure_hhmm"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Missing required CSV columns: {sorted(missing)}")

        for row in reader:
            operator = normalize_space(row.get("operator", ""))
            route_import_key = normalize_space(row.get("route_import_key", ""))
            departure_hhmm = normalize_space(row.get("departure_hhmm", ""))

            if not route_import_key or not departure_hhmm:
                continue

            time_norm = extract_time(departure_hhmm)
            if not time_norm:
                continue

            key = (route_import_key, time_norm)
            master[key] = MasterSailing(
                operator=operator,
                route_import_key=route_import_key,
                departure_hhmm=time_norm,
            )

    log("INFO", "master_loaded", csv_path=csv_path, total=len(master))
    return master


# ----------------------------
# Section split helpers
# ----------------------------

def split_sections(lines: List[str], route_names: List[str]) -> Dict[str, List[str]]:
    """
    ルート見出しごとのテキストブロックを切り出す
    """
    idxs = []
    route_set = set(route_names)

    for i, line in enumerate(lines):
        if line in route_set:
            idxs.append((i, line))

    sections: Dict[str, List[str]] = {}
    for n, (start_idx, route_name) in enumerate(idxs):
        end_idx = idxs[n + 1][0] if n + 1 < len(idxs) else len(lines)
        chunk = lines[start_idx:end_idx]
        sections[route_name] = chunk

    return sections

def is_direction_label(line: str) -> bool:
    return line in {"石垣発", "波照間発", "上原発", "鳩間発", "大原発", "竹富発", "小浜発", "黒島発"}

def classify_status_from_symbol(symbol: str) -> str:
    return STATUS_SYMBOL_MAP.get(symbol, "unknown")


# ----------------------------
# Parser: 安栄観光
# ----------------------------

def parse_anei(html: str) -> Tuple[str, List[CandidateStatus]]:
    _, lines = soup_text(html)
    service_date = extract_service_date(lines)
    sections = split_sections(lines, ANEI_ROUTE_NAMES)

    candidates: List[CandidateStatus] = []

    for route_name, chunk in sections.items():
        log("DEBUG", "anei_route_section_found", route_name=route_name, lines=chunk[:80])

        current_direction: Optional[str] = None
        pending_time: Optional[str] = None

        for line in chunk:
            if is_direction_label(line):
                current_direction = line
                pending_time = None
                continue

            if current_direction is None:
                continue

            # 時刻抽出
            t = extract_time(line)
            if t is not None:
                pending_time = t
                continue

            # 記号抽出
            if SYMBOL_RE.match(line):
                if pending_time is None:
                    continue

                symbol = line
                status = classify_status_from_symbol(symbol)

                if status == "normal":
                    pending_time = None
                    continue

                route_import_key = ANEI_ROUTE_DIRECTION_TO_KEY.get((route_name, current_direction))
                if not route_import_key:
                    log(
                        "WARNING",
                        "anei_route_mapping_missing",
                        route_name=route_name,
                        direction=current_direction,
                        time=pending_time,
                        symbol=symbol,
                    )
                    pending_time = None
                    continue

                candidate = CandidateStatus(
                    operator="安栄観光",
                    route_import_key=route_import_key,
                    departure_hhmm=pending_time,
                    status=status,
                    source_url=ANEI_URL,
                    route_label=route_name,
                    direction_label=current_direction,
                    symbol=symbol,
                )
                candidates.append(candidate)
                pending_time = None

    log("INFO", "anei_candidates_parsed", service_date=service_date, count=len(candidates))
    for c in candidates:
        log(
            "INFO",
            "anei_candidate",
            route_import_key=c.route_import_key,
            departure_hhmm=c.departure_hhmm,
            status=c.status,
            symbol=c.symbol,
            route_label=c.route_label,
            direction_label=c.direction_label,
        )

    return service_date, dedupe_candidates(candidates)


# ----------------------------
# Parser: 八重山観光フェリー
# ----------------------------

def parse_ykf(html: str) -> Tuple[str, List[CandidateStatus]]:
    _, lines = soup_text(html)
    service_date = extract_service_date(lines)
    sections = split_sections(lines, YKF_ROUTE_NAMES)

    candidates: List[CandidateStatus] = []

    for route_name, chunk in sections.items():
        log("DEBUG", "ykf_route_section_found", route_name=route_name, lines=chunk[:80])

        # 方向ラベルは通常2つ出る
        direction_labels: List[str] = [line for line in chunk if is_direction_label(line)]
        if not direction_labels:
            log("WARNING", "ykf_direction_labels_missing", route_name=route_name)
            continue

        # 例:
        # 石垣発 上原発
        # × 08:00 × 09:00
        # × 11:00 × 12:00
        # ...
        # → 各行から [記号, 時刻, 記号, 時刻] を読む
        primary = direction_labels[0]
        secondary = direction_labels[1] if len(direction_labels) >= 2 else None

        for line in chunk:
            symbols = re.findall(r"[◯〇△✕×―-]", line)
            times = TIME_RE.findall(line)

            if not symbols or not times:
                continue

            # 行によって 〇 07:30 〇 07:50 のような2本建て
            # または 〇 07:30 だけの可能性もある
            parsed_pairs: List[Tuple[str, str, str]] = []

            if len(symbols) >= 1 and len(times) >= 1 and primary:
                parsed_pairs.append((primary, extract_time(times[0]) or times[0], symbols[0]))

            if len(symbols) >= 2 and len(times) >= 2 and secondary:
                parsed_pairs.append((secondary, extract_time(times[1]) or times[1], symbols[1]))

            for direction_label, hhmm, symbol in parsed_pairs:
                status = classify_status_from_symbol(symbol)
                if status == "normal":
                    continue

                route_import_key = YKF_ROUTE_DIRECTION_TO_KEY.get((route_name, direction_label))
                if not route_import_key:
                    log(
                        "WARNING",
                        "ykf_route_mapping_missing",
                        route_name=route_name,
                        direction=direction_label,
                        time=hhmm,
                        symbol=symbol,
                    )
                    continue

                candidate = CandidateStatus(
                    operator="八重山観光フェリー",
                    route_import_key=route_import_key,
                    departure_hhmm=hhmm,
                    status=status,
                    source_url=YKF_URL,
                    route_label=route_name,
                    direction_label=direction_label,
                    symbol=symbol,
                )
                candidates.append(candidate)

    log("INFO", "ykf_candidates_parsed", service_date=service_date, count=len(candidates))
    for c in candidates:
        log(
            "INFO",
            "ykf_candidate",
            route_import_key=c.route_import_key,
            departure_hhmm=c.departure_hhmm,
            status=c.status,
            symbol=c.symbol,
            route_label=c.route_label,
            direction_label=c.direction_label,
        )

    return service_date, dedupe_candidates(candidates)


# ----------------------------
# Candidate validation
# ----------------------------

def dedupe_candidates(candidates: List[CandidateStatus]) -> List[CandidateStatus]:
    seen: Set[Tuple[str, str, str]] = set()
    out: List[CandidateStatus] = []
    for c in candidates:
        key = (c.route_import_key, c.departure_hhmm, c.status)
        if key in seen:
            continue
        seen.add(key)
        out.append(c)
    return out

def filter_candidates_by_master(
    candidates: List[CandidateStatus],
    master: Dict[Tuple[str, str], MasterSailing],
) -> Tuple[List[CandidateStatus], List[CandidateStatus]]:
    resolved: List[CandidateStatus] = []
    rejected: List[CandidateStatus] = []

    for c in candidates:
        key = (c.route_import_key, c.departure_hhmm)
        if key in master:
            resolved.append(c)
        else:
            rejected.append(c)

    return resolved, rejected


# ----------------------------
# Bubble sending
# ----------------------------

def send_to_bubble(candidate: CandidateStatus, checked_at_iso: str, service_date: str) -> bool:
    if not BUBBLE_WORKFLOW_URL:
        raise RuntimeError("BUBBLE_WORKFLOW_URL is not set")

    payload = {
        "operator": candidate.operator,
        "route_import_key": candidate.route_import_key,
        "departure_hhmm": candidate.departure_hhmm,
        "status": candidate.status,
        "checked_at": checked_at_iso,
        "service_date": service_date,
        "source_url": candidate.source_url,
    }

    headers = {
        "Content-Type": "application/json",
    }
    if BUBBLE_API_TOKEN:
        headers["Authorization"] = f"Bearer {BUBBLE_API_TOKEN}"

    log("INFO", "sending_abnormal_sailing", **payload)

    r = requests.post(
        BUBBLE_WORKFLOW_URL,
        headers=headers,
        json=payload,
        timeout=REQUEST_TIMEOUT,
    )

    ok = 200 <= r.status_code < 300
    log(
        "INFO" if ok else "ERROR",
        "bubble_response",
        status_code=r.status_code,
        response_text=(r.text[:1000] if r.text else ""),
        route_import_key=candidate.route_import_key,
        departure_hhmm=candidate.departure_hhmm,
        status=candidate.status,
    )
    return ok


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    checked_at = datetime.now(JST)
    checked_at_iso = checked_at.isoformat()
    execution_service_date = checked_at.strftime("%Y-%m-%d")  # 要件: 実行日を使う

    log(
        "INFO",
        "ferry_abnormal_status_sync_started",
        checked_at=checked_at_iso,
        service_date=execution_service_date,
        ferry_sailing_csv=FERRY_SAILING_CSV,
    )

    try:
        master = load_master(FERRY_SAILING_CSV)

        # Fetch pages
        anei_html = requests_get(ANEI_URL)
        ykf_html = requests_get(YKF_URL)

        # Parse
        anei_page_date, anei_candidates = parse_anei(anei_html)
        ykf_page_date, ykf_candidates = parse_ykf(ykf_html)

        all_candidates = anei_candidates + ykf_candidates
        log(
            "INFO",
            "abnormal_candidates_parsed",
            execution_service_date=execution_service_date,
            anei_page_date=anei_page_date,
            ykf_page_date=ykf_page_date,
            total=len(all_candidates),
        )

        # Master resolution
        resolved, rejected = filter_candidates_by_master(all_candidates, master)
        log(
            "INFO",
            "master_resolution_summary",
            parsed=len(all_candidates),
            resolved=len(resolved),
            rejected=len(rejected),
        )

        for c in rejected:
            log(
                "WARNING",
                "candidate_rejected_not_in_master",
                route_import_key=c.route_import_key,
                departure_hhmm=c.departure_hhmm,
                status=c.status,
                route_label=c.route_label,
                direction_label=c.direction_label,
                source_url=c.source_url,
            )

        # Send
        sent = 0
        failed = 0
        for c in resolved:
            try:
                ok = send_to_bubble(
                    candidate=c,
                    checked_at_iso=checked_at_iso,
                    service_date=execution_service_date,
                )
                if ok:
                    sent += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                log(
                    "ERROR",
                    "bubble_send_exception",
                    error=str(e),
                    route_import_key=c.route_import_key,
                    departure_hhmm=c.departure_hhmm,
                    status=c.status,
                )

        log(
            "INFO",
            "ferry_abnormal_status_sync_finished",
            parsed=len(all_candidates),
            resolved=len(resolved),
            rejected=len(rejected),
            sent=sent,
            failed=failed,
            service_date=execution_service_date,
        )
        return 0 if failed == 0 else 1

    except Exception as e:
        log("ERROR", "ferry_abnormal_status_sync_failed", error=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
