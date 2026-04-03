#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Set
from zoneinfo import ZoneInfo

import requests
from bs4 import BeautifulSoup


# =========================================================
# Settings
# =========================================================

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

VERIFY_SSL = os.getenv("VERIFY_SSL", "true").lower() not in {"0", "false", "no"}
YKF_SSL_VERIFY = os.getenv("YKF_SSL_VERIFY", "true").lower() not in {"0", "false", "no"}

# pending とみなすキーワード
PENDING_KEYWORDS = [
    "未定",
    "調整中",
    "条件付",
    "条件付き",
    "運航可否確認中",
    "見合わせ",
]

# cancelled とみなすキーワード
CANCELLED_KEYWORDS = [
    "欠航",
    "運休",
]

# =========================================================
# Logging
# =========================================================

def log(level: str, message: str, **kwargs) -> None:
    levels = ["DEBUG", "INFO", "WARNING", "ERROR"]
    if level not in levels:
        level = "INFO"
    if levels.index(level) < levels.index(LOG_LEVEL):
        return
    payload = {"level": level, "message": message, **kwargs}
    print(json.dumps(payload, ensure_ascii=False), flush=True)


# =========================================================
# Models
# =========================================================

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


# =========================================================
# Regex / utils
# =========================================================

TIME_RE = re.compile(r"(?<!\d)(\d{1,2}:\d{2})(?!\d)")
DATE_RE = re.compile(r"(\d{4})[年/-](\d{1,2})[月/-](\d{1,2})")
WHITESPACE_RE = re.compile(r"\s+")
SYMBOL_RE = re.compile(r"[◯〇△▲✕×―-]")

STATUS_SYMBOL_MAP = {
    "◯": "normal",
    "〇": "normal",
    "△": "pending",
    "▲": "pending",
    "✕": "cancelled",
    "×": "cancelled",
    "―": "unknown",
    "-": "unknown",
}


def normalize_space(text: str) -> str:
    return WHITESPACE_RE.sub(" ", (text or "")).strip()


def normalize_lines(text: str) -> List[str]:
    lines: List[str] = []
    for line in text.splitlines():
        s = normalize_space(line)
        if s:
            lines.append(s)
    return lines


def extract_time(text: str) -> Optional[str]:
    m = TIME_RE.search(text or "")
    if not m:
        return None
    hhmm = m.group(1)
    hh, mm = hhmm.split(":")
    return f"{int(hh):02d}:{mm}"


def extract_service_date(lines: List[str]) -> Optional[str]:
    for line in lines[:100]:
        m = DATE_RE.search(line)
        if m:
            y, mo, d = map(int, m.groups())
            return f"{y:04d}-{mo:02d}-{d:02d}"
    return None


def requests_get(url: str, verify: bool = True) -> str:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; ferry-status-bot/1.0)"
    }
    r = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, verify=verify)
    r.raise_for_status()
    r.encoding = r.apparent_encoding or r.encoding
    return r.text


def soup_text(html: str) -> Tuple[BeautifulSoup, List[str]]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n")
    return soup, normalize_lines(text)


def detect_first_existing_field(fieldnames: List[str], candidates: List[str]) -> Optional[str]:
    normalized = {f.strip(): f for f in fieldnames if f}
    for c in candidates:
        if c in normalized:
            return normalized[c]
    return None


def is_direction_label(line: str) -> bool:
    return line in {
        "石垣発", "波照間発", "上原発", "鳩間発", "大原発", "竹富発", "小浜発", "黒島発"
    }


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


def filter_sendable_statuses(candidates: List[CandidateStatus]) -> List[CandidateStatus]:
    # 今回は cancelled / pending のみ送信
    return [c for c in candidates if c.status in {"cancelled", "pending"}]


def classify_status_from_symbol(symbol: str) -> str:
    return STATUS_SYMBOL_MAP.get(symbol, "unknown")


def classify_status_from_text(text: str) -> str:
    text = normalize_space(text)

    if any(k in text for k in CANCELLED_KEYWORDS):
        return "cancelled"

    if any(k in text for k in PENDING_KEYWORDS):
        return "pending"

    if "通常運航" in text:
        return "normal"

    return "unknown"


# =========================================================
# Route mappings
# =========================================================

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


# =========================================================
# Master loading
# =========================================================

def load_master(
    sailing_csv_path: str,
) -> Dict[Tuple[str, str], MasterSailing]:
    """
    前提:
    - FerrySailing.route はすでに route_import_key
    - master key は (route_import_key, departure_hhmm)
    """
    with open(sailing_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        log("INFO", "sailing_csv_headers_detected", headers=fieldnames, csv_path=sailing_csv_path)

        operator_field = detect_first_existing_field(fieldnames, ["operator"])
        route_field = detect_first_existing_field(fieldnames, ["route"])
        departure_field = detect_first_existing_field(
            fieldnames, ["departure_hhmm", "departure", "departure_time"]
        )

        missing = []
        if not operator_field:
            missing.append("operator")
        if not route_field:
            missing.append("route")
        if not departure_field:
            missing.append("departure_hhmm|departure|departure_time")
        if missing:
            raise ValueError(f"Missing required FerrySailing CSV columns: {missing}")

        master: Dict[Tuple[str, str], MasterSailing] = {}
        skipped_departures: List[Dict[str, str]] = []

        for row in reader:
            operator = normalize_space(row.get(operator_field, ""))
            route_import_key = normalize_space(row.get(route_field, ""))
            departure_raw = normalize_space(row.get(departure_field, ""))

            if not route_import_key or not departure_raw:
                continue

            departure_hhmm = extract_time(departure_raw)
            if not departure_hhmm:
                skipped_departures.append(
                    {"route_import_key": route_import_key, "departure_raw": departure_raw}
                )
                log(
                    "WARNING",
                    "departure_time_parse_failed",
                    route_import_key=route_import_key,
                    departure_raw=departure_raw,
                )
                continue

            key = (route_import_key, departure_hhmm)
            master[key] = MasterSailing(
                operator=operator,
                route_import_key=route_import_key,
                departure_hhmm=departure_hhmm,
            )

        log(
            "INFO",
            "master_loaded",
            csv_path=sailing_csv_path,
            total=len(master),
            skipped_departures=skipped_departures[:20],
        )
        return master


# =========================================================
# Section split
# =========================================================

def split_sections(lines: List[str], route_names: List[str]) -> Dict[str, List[str]]:
    idxs = []
    route_set = set(route_names)

    for i, line in enumerate(lines):
        if line in route_set:
            idxs.append((i, line))

    sections: Dict[str, List[str]] = {}
    for n, (start_idx, route_name) in enumerate(idxs):
        end_idx = idxs[n + 1][0] if n + 1 < len(idxs) else len(lines)
        sections[route_name] = lines[start_idx:end_idx]

    return sections


# =========================================================
# Parser: ANEI
# =========================================================

def parse_anei(html: str) -> Tuple[Optional[str], List[CandidateStatus]]:
    """
    安栄観光ページを方向ブロックごとに解析する

    方針:
    - 航路セクションごとに分割
    - その中で「石垣発」「波照間発」などの方向ラベル位置を拾う
    - 各方向ブロックを独立して解析
    - ブロック内の「時刻 -> 記号」を対応付け
    - cancelled / pending のみ採用
    """
    _, lines = soup_text(html)
    page_service_date = extract_service_date(lines)
    sections = split_sections(lines, ANEI_ROUTE_NAMES)

    candidates: List[CandidateStatus] = []

    for route_name, chunk in sections.items():
        log("DEBUG", "anei_route_section_found", route_name=route_name, lines=chunk[:120])

        # この航路セクション内の方向ラベル位置を拾う
        direction_positions: List[Tuple[int, str]] = []
        for i, line in enumerate(chunk):
            if is_direction_label(line):
                direction_positions.append((i, line))

        if not direction_positions:
            log("WARNING", "anei_direction_labels_missing", route_name=route_name)
            continue

        # 各方向ラベルごとにブロックを切る
        for n, (start_idx, direction_label) in enumerate(direction_positions):
            end_idx = direction_positions[n + 1][0] if n + 1 < len(direction_positions) else len(chunk)
            direction_block = chunk[start_idx:end_idx]

            route_import_key = ANEI_ROUTE_DIRECTION_TO_KEY.get((route_name, direction_label))
            if not route_import_key:
                log(
                    "WARNING",
                    "anei_route_mapping_missing",
                    route_name=route_name,
                    direction=direction_label,
                )
                continue

            log(
                "DEBUG",
                "anei_direction_block_found",
                route_name=route_name,
                direction_label=direction_label,
                route_import_key=route_import_key,
                lines=direction_block[:80],
            )

            pending_time: Optional[str] = None

            for line in direction_block[1:]:
                # 1) 単独時刻行
                maybe_time = extract_time(line)
                if maybe_time and line == maybe_time:
                    pending_time = maybe_time
                    continue

                # 2) 単独記号行
                if line in STATUS_SYMBOL_MAP:
                    if pending_time is None:
                        continue

                    status = classify_status_from_symbol(line)
                    if status in {"cancelled", "pending"}:
                        candidates.append(
                            CandidateStatus(
                                operator="安栄観光",
                                route_import_key=route_import_key,
                                departure_hhmm=pending_time,
                                status=status,
                                source_url=ANEI_URL,
                                route_label=route_name,
                                direction_label=direction_label,
                                symbol=line,
                            )
                        )
                    pending_time = None
                    continue

                # 3) 同一行に「時刻 + 記号」が1組ある場合
                times = TIME_RE.findall(line)
                symbols = SYMBOL_RE.findall(line)

                if len(times) == 1 and len(symbols) == 1:
                    hhmm = extract_time(times[0])
                    symbol = symbols[0]
                    status = classify_status_from_symbol(symbol)

                    if hhmm and status in {"cancelled", "pending"}:
                        candidates.append(
                            CandidateStatus(
                                operator="安栄観光",
                                route_import_key=route_import_key,
                                departure_hhmm=hhmm,
                                status=status,
                                source_url=ANEI_URL,
                                route_label=route_name,
                                direction_label=direction_label,
                                symbol=symbol,
                            )
                        )
                    pending_time = None
                    continue

                # 4) 保険: 直前に時刻があり、その次の行がテキスト説明の場合
                if pending_time is not None:
                    status = classify_status_from_text(line)
                    if status in {"cancelled", "pending"}:
                        candidates.append(
                            CandidateStatus(
                                operator="安栄観光",
                                route_import_key=route_import_key,
                                departure_hhmm=pending_time,
                                status=status,
                                source_url=ANEI_URL,
                                route_label=route_name,
                                direction_label=direction_label,
                                symbol="text",
                            )
                        )
                    pending_time = None

    candidates = dedupe_candidates(candidates)
    candidates = filter_sendable_statuses(candidates)

    log(
        "INFO",
        "anei_candidates_parsed",
        page_service_date=page_service_date,
        count=len(candidates),
    )
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

    return page_service_date, candidates


# =========================================================
# Parser: YKF
# =========================================================

def parse_ykf(html: str) -> Tuple[Optional[str], List[CandidateStatus]]:
    _, lines = soup_text(html)
    page_service_date = extract_service_date(lines)
    sections = split_sections(lines, YKF_ROUTE_NAMES)

    candidates: List[CandidateStatus] = []

    for route_name, chunk in sections.items():
        log("DEBUG", "ykf_route_section_found", route_name=route_name, lines=chunk[:120])

        direction_labels = [line for line in chunk if is_direction_label(line)]
        if not direction_labels:
            log("WARNING", "ykf_direction_labels_missing", route_name=route_name)
            continue

        primary = direction_labels[0]
        secondary = direction_labels[1] if len(direction_labels) >= 2 else None

        for line in chunk:
            symbols = SYMBOL_RE.findall(line)
            times = TIME_RE.findall(line)

            if not symbols or not times:
                continue

            pairs: List[Tuple[str, str, str]] = []

            if len(symbols) >= 1 and len(times) >= 1 and primary:
                hhmm = extract_time(times[0])
                if hhmm:
                    pairs.append((primary, hhmm, symbols[0]))

            if len(symbols) >= 2 and len(times) >= 2 and secondary:
                hhmm = extract_time(times[1])
                if hhmm:
                    pairs.append((secondary, hhmm, symbols[1]))

            for direction_label, hhmm, symbol in pairs:
                status = classify_status_from_symbol(symbol)
                if status not in {"cancelled", "pending"}:
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

                candidates.append(
                    CandidateStatus(
                        operator="八重山観光フェリー",
                        route_import_key=route_import_key,
                        departure_hhmm=hhmm,
                        status=status,
                        source_url=YKF_URL,
                        route_label=route_name,
                        direction_label=direction_label,
                        symbol=symbol,
                    )
                )

            # テキストベースの保険
            if not pairs:
                status = classify_status_from_text(line)
                if status in {"cancelled", "pending"}:
                    if len(times) == 1 and primary:
                        hhmm = extract_time(times[0])
                        if hhmm:
                            route_import_key = YKF_ROUTE_DIRECTION_TO_KEY.get((route_name, primary))
                            if route_import_key:
                                candidates.append(
                                    CandidateStatus(
                                        operator="八重山観光フェリー",
                                        route_import_key=route_import_key,
                                        departure_hhmm=hhmm,
                                        status=status,
                                        source_url=YKF_URL,
                                        route_label=route_name,
                                        direction_label=primary,
                                        symbol="text",
                                    )
                                )

    candidates = dedupe_candidates(candidates)
    candidates = filter_sendable_statuses(candidates)

    log(
        "INFO",
        "ykf_candidates_parsed",
        page_service_date=page_service_date,
        count=len(candidates),
    )
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

    return page_service_date, candidates


# =========================================================
# Resolution
# =========================================================

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


# =========================================================
# Bubble sending
# =========================================================

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
        verify=VERIFY_SSL,
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


# =========================================================
# Main
# =========================================================

def main() -> int:
    checked_at = datetime.now(JST)
    checked_at_iso = checked_at.isoformat()

    # 重要: service_date は実行日
    execution_service_date = checked_at.strftime("%Y-%m-%d")

    log(
        "INFO",
        "ferry_abnormal_status_sync_started",
        checked_at=checked_at_iso,
        service_date=execution_service_date,
        ferry_sailing_csv=FERRY_SAILING_CSV,
        ykf_ssl_verify=YKF_SSL_VERIFY,
        pending_keywords=PENDING_KEYWORDS,
    )

    try:
        master = load_master(FERRY_SAILING_CSV)

        anei_html = requests_get(ANEI_URL, verify=VERIFY_SSL)
        ykf_html = requests_get(YKF_URL, verify=YKF_SSL_VERIFY)

        anei_page_date, anei_candidates = parse_anei(anei_html)
        ykf_page_date, ykf_candidates = parse_ykf(ykf_html)

        all_candidates = anei_candidates + ykf_candidates
        all_candidates = filter_sendable_statuses(dedupe_candidates(all_candidates))

        log(
            "INFO",
            "abnormal_candidates_parsed",
            execution_service_date=execution_service_date,
            anei_page_date=anei_page_date,
            ykf_page_date=ykf_page_date,
            total=len(all_candidates),
        )

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
