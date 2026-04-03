#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime
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

CANCELLED_KEYWORDS = ["欠航", "全便欠航", "運航停止", "中止"]
PENDING_KEYWORDS = [
    x.strip() for x in os.getenv("FERRY_PENDING_KEYWORDS", "").split(",") if x.strip()
]

TIME_RE = re.compile(r"^\d{1,2}:\d{2}$")


@dataclass(frozen=True)
class OperatorConfig:
    key: str
    bubble_operator_name_en: str
    source_url: str
    csv_operator_aliases: Tuple[str, ...]


OPERATORS: Dict[str, OperatorConfig] = {
    "anei": OperatorConfig(
        key="anei",
        bubble_operator_name_en="Anei Kanko",
        source_url=ANEI_URL,
        csv_operator_aliases=("安栄観光", "Anei Kanko"),
    ),
    "ykf": OperatorConfig(
        key="ykf",
        bubble_operator_name_en="Yaeyama Kanko Ferry",
        source_url=YKF_URL,
        csv_operator_aliases=("八重山観光フェリー", "Yaeyama Kanko Ferry"),
    ),
}


@dataclass(frozen=True)
class AbnormalCandidate:
    operator_key: str
    section_label: str
    departure_label: str
    departure_hhmm_raw: str
    status: str
    raw_status_text: str
    source_url: str


@dataclass(frozen=True)
class AbnormalSailing:
    operator_bubble_name_en: str
    route_import_key: str
    departure_hhmm: str  # CSV の正規値
    status: str
    source_url: str
    raw_status_text: str
    section_label: str
    departure_label: str


@dataclass(frozen=True)
class Config:
    bubble_base_url: str
    workflow_name: str
    ferry_sailing_csv: str
    ferry_route_csv: str
    ferry_secret: str
    anei_url: str
    ykf_url: str
    ykf_ssl_verify: bool
    request_delay_sec: int
    retries: int


class Logger:
    @staticmethod
    def info(message: str, **kwargs) -> None:
        payload = {"level": "INFO", "message": message, **kwargs}
        print(json.dumps(payload, ensure_ascii=False))

    @staticmethod
    def warning(message: str, **kwargs) -> None:
        payload = {"level": "WARNING", "message": message, **kwargs}
        print(json.dumps(payload, ensure_ascii=False))

    @staticmethod
    def error(message: str, **kwargs) -> None:
        payload = {"level": "ERROR", "message": message, **kwargs}
        print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)


def env_required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"環境変数 {name} が未設定です。")
    return value


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    return int(raw)


def load_config() -> Config:
    return Config(
        bubble_base_url=env_required("BUBBLE_BASE_URL"),
        workflow_name=os.getenv("WF_NAME", "receive_ferry_status").strip() or "receive_ferry_status",
        ferry_sailing_csv=env_required("FERRY_SAILING_CSV"),
        ferry_route_csv=env_required("FERRY_ROUTE_CSV"),
        ferry_secret=env_required("FERRY_SECRET"),
        anei_url=os.getenv("ANEI_URL", ANEI_URL).strip() or ANEI_URL,
        ykf_url=os.getenv("YKF_URL", YKF_URL).strip() or YKF_URL,
        ykf_ssl_verify=env_bool("YKF_SSL_VERIFY", False),
        request_delay_sec=env_int("REQUEST_DELAY_SEC", 3),
        retries=env_int("REQUEST_RETRIES", 3),
    )


def now_jst() -> datetime:
    return datetime.now(JST)


def normalize_text(text: str) -> str:
    text = (text or "").replace("\u3000", " ").strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_hhmm_for_match(text: str) -> str:
    t = normalize_text(text)
    t = t.replace("：", ":")
    m = re.match(r"^(\d{1,2}):(\d{2})$", t)
    if not m:
        return t
    hour = int(m.group(1))
    minute = m.group(2)
    return f"{hour}:{minute}"


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
            Logger.warning(
                "fetch_retry",
                url=url,
                attempt=attempt,
                retries=retries,
                verify=verify,
                error=str(e),
            )
            if attempt < retries:
                time.sleep(delay_sec)
    raise RuntimeError(f"取得失敗: {url} / {last_error}")


def csv_operator_to_key(operator_value: str) -> Optional[str]:
    value = normalize_text(operator_value)
    for key, cfg in OPERATORS.items():
        if value in cfg.csv_operator_aliases:
            return key
    return None


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
    key: (operator_key, section_label, departure_label)
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
        "石垣→西表大原": "西表大原航路",
        "西表大原→石垣": "西表大原航路",
        "石垣→西表上原": "西表上原航路",
        "西表上原→石垣": "西表上原航路",
    }

    with open(route_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            operator_key = csv_operator_to_key(row.get("operator", ""))
            import_key = normalize_text(row.get("import_key", ""))
            route_name = normalize_text(row.get("route_name", ""))
            from_port = normalize_text(row.get("from_port", ""))

            section_label = direct_section_label_map.get(route_name)
            departure_label = port_to_departure_label(from_port)

            if not operator_key or not import_key or not section_label or not departure_label:
                continue

            # 安栄の inter-island は current page から一意に取りづらいため対象外
            if operator_key == "anei" and section_label == "上原-鳩間航路":
                continue

            lookup[(operator_key, section_label, departure_label)] = import_key

    return lookup


def build_canonical_sailing_lookup(
    sailing_csv_path: str,
) -> Dict[Tuple[str, str, str], str]:
    """
    key: (operator_key, route_import_key, normalized_hhmm_for_match)
    value: canonical departure_hhmm from CSV
    """
    lookup: Dict[Tuple[str, str, str], str] = {}

    with open(sailing_csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            operator_key = csv_operator_to_key(row.get("operator", ""))
            route_import_key = normalize_text(row.get("route", ""))
            departure_hhmm_raw = normalize_text(row.get("departure_hhmm", ""))

            if not operator_key or not route_import_key or not departure_hhmm_raw:
                continue

            canonical = departure_hhmm_raw.replace("：", ":")
            match_key = normalize_hhmm_for_match(canonical)

            lookup[(operator_key, route_import_key, match_key)] = canonical

    return lookup


def extract_lines(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text("\n", strip=True)
    return [normalize_text(x) for x in text.splitlines() if normalize_text(x)]


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


def parse_anei_abnormal_candidates(html: str) -> List[AbnormalCandidate]:
    operator_key = "anei"
    section_labels = ["波照間航路", "上原航路", "鳩間航路", "大原航路", "竹富航路", "小浜航路", "黒島航路"]
    lines = extract_lines(html)
    ranges = find_section_ranges(lines, section_labels)

    result: List[AbnormalCandidate] = []
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
                departure_hhmm_raw = line
                nearby_lines = block[i + 1:i + 4]
                raw_status_text = " ".join(nearby_lines)
                status = classify_status_from_symbol_or_text(raw_status_text)

                if status:
                    result.append(
                        AbnormalCandidate(
                            operator_key=operator_key,
                            section_label=section_label,
                            departure_label=current_departure_label,
                            departure_hhmm_raw=departure_hhmm_raw,
                            status=status,
                            raw_status_text=raw_status_text,
                            source_url=ANEI_URL,
                        )
                    )
                i += 1
                continue

            i += 1

    return result


def parse_ykf_abnormal_candidates(html: str, service_date: date) -> List[AbnormalCandidate]:
    operator_key = "ykf"
    section_labels = ["竹富航路", "小浜航路", "黒島航路", "西表大原航路", "西表上原航路", "鳩間航路", "上原-鳩間航路"]
    lines = extract_lines(html)
    ranges = find_section_ranges(lines, section_labels)

    _ = service_date  # 取得日基準で動かすが、ここでは時刻抽出にのみ利用

    result: List[AbnormalCandidate] = []
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
            Logger.warning(
                "ykf_departure_labels_not_found",
                operator_key=operator_key,
                section_label=section_label,
            )
            continue

        for line in block:
            pairs = pair_pattern.findall(line)
            if not pairs:
                continue

            for idx, (symbol, departure_hhmm_raw) in enumerate(pairs):
                if idx >= len(departure_labels):
                    Logger.warning(
                        "ykf_label_pair_mismatch",
                        operator_key=operator_key,
                        section_label=section_label,
                        line=line,
                        departure_labels=departure_labels,
                        pairs=pairs,
                    )
                    continue

                status = classify_status_from_symbol_or_text(symbol)
                if not status:
                    continue

                departure_label = departure_labels[idx]
                result.append(
                    AbnormalCandidate(
                        operator_key=operator_key,
                        section_label=section_label,
                        departure_label=departure_label,
                        departure_hhmm_raw=departure_hhmm_raw,
                        status=status,
                        raw_status_text=line,
                        source_url=YKF_URL,
                    )
                )

    return result


def resolve_candidates(
    candidates: List[AbnormalCandidate],
    route_lookup: Dict[Tuple[str, str, str], str],
    canonical_sailing_lookup: Dict[Tuple[str, str, str], str],
) -> Tuple[List[AbnormalSailing], List[AbnormalCandidate]]:
    resolved: List[AbnormalSailing] = []
    unresolved: List[AbnormalCandidate] = []

    for item in candidates:
        route_import_key = route_lookup.get((item.operator_key, item.section_label, item.departure_label))
        if not route_import_key:
            unresolved.append(item)
            Logger.warning(
                "route_lookup_not_found",
                operator_key=item.operator_key,
                section_label=item.section_label,
                departure_label=item.departure_label,
                departure_hhmm_raw=item.departure_hhmm_raw,
                status=item.status,
                raw_status_text=item.raw_status_text,
            )
            continue

        departure_match_key = normalize_hhmm_for_match(item.departure_hhmm_raw)
        canonical_departure_hhmm = canonical_sailing_lookup.get(
            (item.operator_key, route_import_key, departure_match_key)
        )
        if not canonical_departure_hhmm:
            unresolved.append(item)
            Logger.warning(
                "canonical_sailing_not_found",
                operator_key=item.operator_key,
                route_import_key=route_import_key,
                section_label=item.section_label,
                departure_label=item.departure_label,
                departure_hhmm_raw=item.departure_hhmm_raw,
                departure_match_key=departure_match_key,
                status=item.status,
                raw_status_text=item.raw_status_text,
            )
            continue

        resolved.append(
            AbnormalSailing(
                operator_bubble_name_en=OPERATORS[item.operator_key].bubble_operator_name_en,
                route_import_key=route_import_key,
                departure_hhmm=canonical_departure_hhmm,
                status=item.status,
                source_url=item.source_url,
                raw_status_text=item.raw_status_text,
                section_label=item.section_label,
                departure_label=item.departure_label,
            )
        )

    return resolved, unresolved


def dedupe_resolved(items: List[AbnormalSailing]) -> List[AbnormalSailing]:
    dedup: Dict[Tuple[str, str, str], AbnormalSailing] = {}
    for item in items:
        key = (item.operator_bubble_name_en, item.route_import_key, item.departure_hhmm)
        dedup[key] = item
    return list(dedup.values())


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
        config = load_config()
        now = now_jst()
        checked_at_iso = now.isoformat()
        service_date_iso = now.date().isoformat()
        service_date = now.date()

        Logger.info(
            "ferry_abnormal_status_sync_started",
            checked_at=checked_at_iso,
            service_date=service_date_iso,
            ferry_sailing_csv=config.ferry_sailing_csv,
            ferry_route_csv=config.ferry_route_csv,
            pending_keywords=PENDING_KEYWORDS,
            ykf_ssl_verify=config.ykf_ssl_verify,
            retries=config.retries,
            request_delay_sec=config.request_delay_sec,
        )

        route_lookup = build_route_lookup(config.ferry_route_csv)
        canonical_sailing_lookup = build_canonical_sailing_lookup(config.ferry_sailing_csv)

        Logger.info(
            "master_loaded",
            route_lookup_keys=len(route_lookup),
            canonical_sailing_lookup_keys=len(canonical_sailing_lookup),
        )

        session = build_session()
        anei_html = get_with_retry(
            session=session,
            url=config.anei_url,
            retries=config.retries,
            delay_sec=config.request_delay_sec,
            verify=True,
        )
        ykf_html = get_with_retry(
            session=session,
            url=config.ykf_url,
            retries=config.retries,
            delay_sec=config.request_delay_sec,
            verify=config.ykf_ssl_verify,
        )

        anei_candidates = parse_anei_abnormal_candidates(anei_html)
        ykf_candidates = parse_ykf_abnormal_candidates(ykf_html, service_date)
        all_candidates = anei_candidates + ykf_candidates

        Logger.info(
            "abnormal_candidates_parsed",
            anei_candidates=len(anei_candidates),
            ykf_candidates=len(ykf_candidates),
            total_candidates=len(all_candidates),
        )

        resolved, unresolved = resolve_candidates(
            candidates=all_candidates,
            route_lookup=route_lookup,
            canonical_sailing_lookup=canonical_sailing_lookup,
        )
        final_items = dedupe_resolved(resolved)

        Logger.info(
            "abnormal_candidates_resolved",
            resolved=len(resolved),
            unresolved=len(unresolved),
            final_items=len(final_items),
        )

        if not final_items:
            Logger.info("no_abnormal_sailings_found_nothing_to_send")
            return 0

        bubble = BubbleWorkflowClient(
            base_url=config.bubble_base_url,
            workflow_name=config.workflow_name,
            secret=config.ferry_secret,
        )

        sent = 0
        failed = 0

        for item in final_items:
            try:
                Logger.info(
                    "sending_abnormal_sailing",
                    operator=item.operator_bubble_name_en,
                    route_import_key=item.route_import_key,
                    departure_hhmm=item.departure_hhmm,
                    status=item.status,
                    section_label=item.section_label,
                    departure_label=item.departure_label,
                    raw_status_text=item.raw_status_text,
                    source_url=item.source_url,
                )

                bubble.send_status(
                    operator=item.operator_bubble_name_en,
                    status=item.status,
                    checked_at_iso=checked_at_iso,
                    service_date_iso=service_date_iso,
                    source_url=item.source_url,
                    route_import_key=item.route_import_key,
                    departure_hhmm=item.departure_hhmm,
                )
                sent += 1
            except Exception as e:
                failed += 1
                Logger.error(
                    "bubble_send_failed",
                    operator=item.operator_bubble_name_en,
                    route_import_key=item.route_import_key,
                    departure_hhmm=item.departure_hhmm,
                    status=item.status,
                    error=str(e),
                )

        Logger.info(
            "ferry_abnormal_status_sync_finished",
            sent=sent,
            failed=failed,
            unresolved=len(unresolved),
        )

        return 0 if failed == 0 else 1

    except Exception as e:
        Logger.error("fatal_error", error=str(e))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
