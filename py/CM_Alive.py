#!/usr/bin/env python3
"""Unified entrypoint for convert_cm.py and Alive.py without external imports."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections import defaultdict
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_URL = "https://zip.cm.edu.kg/all.json"
DEFAULT_OUTPUT = "all_output.txt"
DEFAULT_FORMAT_TEMPLATE = "{$.ip}:{$.port}#{$.meta.country_cn} {$.meta.asOrganization} {$.meta.city}"
PLACEHOLDER_PATTERN = re.compile(r"\{\s*(\$\.[^{}]+?)\s*\}")

DEFAULT_SOURCE_URLS = (
    "https://raw.githubusercontent.com/xihadance/CloudflareIP/main/CM.txt",
    "https://raw.githubusercontent.com/MarinaAqua/ProxyIP/main/Alive.txt",
    "https://raw.githubusercontent.com/FoolVPN-ID/Nautica/main/proxyList.txt",
)
DEFAULT_API_URL = "https://proxyip.snu.cc/batch"
DEFAULT_TARGET_COUNTRIES = ("HK", "SG", "US", "UK", "AU")
DEFAULT_LARGE_GROUP_BATCH_SIZE = 150
DEFAULT_DIRECT_REQUEST_LIMIT = 250
DEFAULT_SHARD_WORKERS = 8
DEFAULT_MAX_SUCCESS_PER_GROUP = 100
DEFAULT_BATCH_RETRIES = 2
RETRYABLE_HTTP_STATUS_CODES = {408, 429, 500, 502, 503, 504}
COUNTRY_ALIASES = {
    "HK": "HK",
    "香港": "HK",
    "HONGKONG": "HK",
    "HONG KONG": "HK",
    "SG": "SG",
    "新加坡": "SG",
    "SINGAPORE": "SG",
    "US": "US",
    "美国": "US",
    "UNITED STATES": "US",
    "USA": "US",
    "UK": "UK",
    "英国": "UK",
    "UNITED KINGDOM": "UK",
    "GB": "UK",
    "AU": "AU",
    "澳大利亚": "AU",
    "澳洲": "AU",
    "AUSTRALIA": "AU",
}
TAGGED_PROXY_PATTERN = re.compile(r"^(?P<ip>[^:\s#]+):(?P<port>\d+)\#(?P<meta>.+)$")


def fetch_json_text(url: str, timeout: float = 15.0) -> str:
    with urlopen(url, timeout=timeout) as response:
        charset = response.headers.get_content_charset() or "utf-8"
        return response.read().decode(charset, errors="replace")


def build_display_name(item: dict) -> str:
    meta = item.get("meta")
    if not isinstance(meta, dict):
        meta = {}
    parts = [
        str(meta.get("country_cn", "")).strip(),
        str(meta.get("asOrganization", "")).strip(),
        str(meta.get("city", "")).strip(),
    ]
    name = " ".join(part for part in parts if part)
    return name or "UNKNOWN"


def _tokenize_json_path(path: str) -> list[str | int]:
    if not path.startswith("$."):
        raise ValueError(f"Unsupported placeholder path: {path}")

    body = path[2:]
    tokens: list[str | int] = []
    for part in body.split("."):
        if not part:
            continue
        while True:
            bracket_start = part.find("[")
            if bracket_start == -1:
                tokens.append(part)
                break
            if bracket_start > 0:
                tokens.append(part[:bracket_start])
            bracket_end = part.find("]", bracket_start)
            if bracket_end == -1:
                raise ValueError(f"Invalid placeholder path: {path}")
            index_text = part[bracket_start + 1 : bracket_end].strip()
            tokens.append(int(index_text))
            part = part[bracket_end + 1 :]
            if not part:
                break
    return tokens


def resolve_json_path(data: object, path: str) -> str:
    current = data
    try:
        for token in _tokenize_json_path(path):
            if isinstance(token, int):
                if not isinstance(current, list):
                    return ""
                current = current[token]
                continue
            if not isinstance(current, dict):
                return ""
            current = current.get(token, "")
    except (IndexError, TypeError, ValueError):
        return ""

    if current is None:
        return ""
    if isinstance(current, (dict, list)):
        return json.dumps(current, ensure_ascii=False)
    return str(current)


def render_format_template(item: dict, format_template: str) -> str:
    return PLACEHOLDER_PATTERN.sub(
        lambda match: resolve_json_path(item, match.group(1)),
        format_template,
    ).strip()


def build_output_lines(
    raw_json: str,
    format_template: str = DEFAULT_FORMAT_TEMPLATE,
) -> list[str]:
    payload = json.loads(raw_json)
    data = payload.get("data", [])
    if not isinstance(data, list):
        raise ValueError("Invalid JSON format: 'data' must be an array")

    lines: list[str] = []
    for item in data:
        if not isinstance(item, dict):
            continue

        ip = str(item.get("ip", "")).strip()
        if not ip:
            continue

        ports = item.get("port", [])
        if not isinstance(ports, list):
            continue

        for raw_port in ports:
            try:
                port = int(raw_port)
            except (TypeError, ValueError):
                continue
            if port < 1 or port > 65535:
                continue
            render_item = dict(item)
            render_item["ip"] = ip
            render_item["port"] = port
            if "meta" not in render_item or not isinstance(render_item["meta"], dict):
                render_item["meta"] = {}
            line = render_format_template(render_item, format_template)
            lines.append(line)
    return lines


@dataclass(frozen=True)
class ProxyRow:
    ip: str
    port: str
    country: str
    org: str

    @property
    def ip_port(self) -> str:
        return f"{self.ip}:{self.port}"

    def format_with_latency(self, latency: int) -> str:
        meta = self.country if not self.org else f"{self.country} {self.org}"
        return f"{self.ip}:{self.port}#{meta} ~ {latency}"


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def read_text_file(path: str) -> str:
    with open(path, "r", encoding="utf-8") as handle:
        return handle.read()


def normalize_country_label(label: str) -> str:
    normalized = label.strip()
    if not normalized:
        return ""
    alias_key = normalized.upper()
    return COUNTRY_ALIASES.get(alias_key, COUNTRY_ALIASES.get(normalized, normalized))


def _parse_csv_row(line: str) -> ProxyRow | None:
    parts = [p.strip() for p in line.split(",")]
    if len(parts) < 3:
        return None
    ip, port, country = parts[0], parts[1], parts[2]
    if not ip or not port or not country:
        return None
    org = ",".join(parts[3:]).strip() if len(parts) > 3 else ""
    return ProxyRow(ip=ip, port=port, country=normalize_country_label(country), org=org)


def _parse_tagged_row(line: str) -> ProxyRow | None:
    match = TAGGED_PROXY_PATTERN.match(line)
    if not match:
        return None
    meta = match.group("meta").strip()
    if not meta:
        return None
    parts = meta.split(None, 1)
    country = normalize_country_label(parts[0])
    org = parts[1].strip() if len(parts) > 1 else ""
    if not country:
        return None
    return ProxyRow(
        ip=match.group("ip").strip(),
        port=match.group("port").strip(),
        country=country,
        org=org,
    )


def parse_rows(text: str) -> Iterable[ProxyRow]:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        row = _parse_csv_row(line)
        if row is None:
            row = _parse_tagged_row(line)
        if row is not None:
            yield row


def build_batch_request(api_url: str, ips: List[str]) -> Request:
    body = json.dumps({"ips": ips}, ensure_ascii=False).encode("utf-8")
    return Request(
        api_url,
        data=body,
        method="POST",
        headers={
            "content-type": "text/plain;charset=UTF-8",
            "sec-ch-ua": "\"Chromium\";v=\"146\", \"Not-A.Brand\";v=\"24\", \"Google Chrome\";v=\"146\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "referer": "https://proxyip.snu.cc/",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        },
    )


def is_retryable_batch_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in RETRYABLE_HTTP_STATUS_CODES
    return isinstance(exc, (json.JSONDecodeError, TimeoutError, URLError))


def post_batch(
    ips: List[str],
    api_url: str = DEFAULT_API_URL,
    retries: int = DEFAULT_BATCH_RETRIES,
) -> List[Dict]:
    if not ips:
        return []

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = build_batch_request(api_url, ips)
            with urlopen(req, timeout=300) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
            data = json.loads(payload)
            if isinstance(data, dict) and "data" in data:
                return data.get("data") or []
            return []
        except Exception as exc:
            if not is_retryable_batch_error(exc):
                raise
            last_error = exc
            if attempt >= retries:
                break
            time.sleep(1 + attempt)

    if len(ips) == 1:
        print(f"skip unreachable batch item {ips[0]}: {last_error}", file=sys.stderr)
        return []

    midpoint = len(ips) // 2
    left = post_batch(ips[:midpoint], api_url=api_url, retries=retries)
    right = post_batch(ips[midpoint:], api_url=api_url, retries=retries)
    return left + right


def get_batch_size(
    total_rows: int,
    direct_request_limit: int,
    large_group_batch_size: int,
) -> int:
    if total_rows <= 0:
        return 1
    if total_rows <= direct_request_limit:
        return total_rows
    return large_group_batch_size


def chunked(items: Sequence[ProxyRow], size: int) -> Iterable[List[ProxyRow]]:
    for i in range(0, len(items), size):
        yield list(items[i : i + size])


def _extract_ranked_rows(
    items: List[Dict],
    by_ip_port: Dict[str, ProxyRow],
) -> List[Tuple[ProxyRow, int]]:
    results: List[Tuple[ProxyRow, int]] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        if not item.get("valid"):
            continue
        ip = str(item.get("ip", "")).strip()
        port = str(item.get("port", "")).strip()
        if not ip or not port:
            continue
        row = by_ip_port.get(f"{ip}:{port}")
        if not row:
            continue
        latency = item.get("latency")
        try:
            latency_value = int(latency)
        except Exception:
            latency_value = 10**9
        results.append((row, latency_value))
    return results


def filter_and_rank_groups(
    groups: Dict[str, List[ProxyRow]],
    target_countries: Sequence[str],
    direct_request_limit: int,
    batch_size: int,
    max_success_per_group: int,
    shard_workers: int,
    api_url: str = DEFAULT_API_URL,
    batch_retries: int = DEFAULT_BATCH_RETRIES,
) -> Dict[str, List[Tuple[ProxyRow, int]]]:
    ranked_by_country: Dict[str, List[Tuple[ProxyRow, int]]] = {
        country: [] for country in target_countries
    }
    future_to_country: Dict[Future[List[Dict]], str] = {}
    by_country_ip_port: Dict[str, Dict[str, ProxyRow]] = {}

    normalized_workers = max(1, shard_workers)
    with ThreadPoolExecutor(max_workers=normalized_workers) as executor:
        for country in target_countries:
            rows = groups.get(country, [])
            if not rows:
                continue
            effective_batch_size = get_batch_size(
                len(rows),
                direct_request_limit=direct_request_limit,
                large_group_batch_size=batch_size,
            )
            by_country_ip_port[country] = {row.ip_port: row for row in rows}
            for shard in chunked(rows, effective_batch_size):
                ips = [row.ip_port for row in shard]
                future = executor.submit(
                    post_batch,
                    ips,
                    api_url,
                    batch_retries,
                )
                future_to_country[future] = country

        for future in as_completed(future_to_country):
            country = future_to_country[future]
            ranked_by_country[country].extend(
                _extract_ranked_rows(future.result(), by_country_ip_port[country])
            )

    for country in target_countries:
        ranked_by_country[country] = sorted(
            ranked_by_country[country],
            key=lambda item: item[1],
        )[:max_success_per_group]
    return ranked_by_country


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run convert_cm or Alive logic from a single entrypoint",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    convert_parser = subparsers.add_parser(
        "convert",
        help="Export ip:port lines from cm all.json data",
    )
    convert_parser.add_argument("--url", default=DEFAULT_URL, help="JSON source URL")
    convert_parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output file path",
    )
    convert_parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Download timeout in seconds",
    )
    convert_parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT_TEMPLATE,
        help="Output template, for example '{$.ip}:{$.port}#{$.meta.country_cn}'",
    )

    alive_parser = subparsers.add_parser(
        "alive",
        help="Fetch proxy lists, verify availability, and write ranked output",
    )
    alive_parser.add_argument(
        "output",
        nargs="?",
        default="Alive.txt",
        help="Output file path",
    )
    alive_parser.add_argument(
        "--source-url",
        action="append",
        dest="source_urls",
        default=None,
        help="Source URL, repeat to provide multiple lists",
    )
    alive_parser.add_argument(
        "--source-file",
        action="append",
        dest="source_files",
        default=None,
        help="Local source file path, repeat to provide multiple files",
    )
    alive_parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help="Batch verification API URL",
    )
    alive_parser.add_argument(
        "--target-country",
        action="append",
        dest="target_countries",
        default=None,
        help="Country code to keep, repeat to provide multiple countries",
    )
    alive_parser.add_argument(
        "--large-group-batch-size",
        type=int,
        default=DEFAULT_LARGE_GROUP_BATCH_SIZE,
        help="Shard size for large country groups",
    )
    alive_parser.add_argument(
        "--direct-request-limit",
        type=int,
        default=DEFAULT_DIRECT_REQUEST_LIMIT,
        help="Use one request when group size is at or below this limit",
    )
    alive_parser.add_argument(
        "--shard-workers",
        type=int,
        default=DEFAULT_SHARD_WORKERS,
        help="Number of concurrent shard verification workers",
    )
    alive_parser.add_argument(
        "--max-success-per-group",
        type=int,
        default=DEFAULT_MAX_SUCCESS_PER_GROUP,
        help="Maximum number of rows to keep per country after ranking",
    )
    alive_parser.add_argument(
        "--batch-retries",
        type=int,
        default=DEFAULT_BATCH_RETRIES,
        help="Retries for each batch verification request",
    )
    return parser


def run_convert(args: argparse.Namespace) -> int:
    raw_json = fetch_json_text(args.url, timeout=args.timeout)
    lines = build_output_lines(raw_json, format_template=args.format)
    with open(args.output, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines))
        if lines:
            handle.write("\n")
    print(f"已输出 {len(lines)} 条到 {args.output}")
    return 0


def run_alive(args: argparse.Namespace) -> int:
    groups: Dict[str, List[ProxyRow]] = defaultdict(list)
    source_files = list(args.source_files) if args.source_files else []
    if args.source_urls:
        source_urls = list(args.source_urls)
    elif source_files:
        source_urls = []
    else:
        source_urls = list(DEFAULT_SOURCE_URLS)
    target_countries = tuple(args.target_countries) if args.target_countries else DEFAULT_TARGET_COUNTRIES
    target_country_set = set(target_countries)

    for url in source_urls:
        text = fetch_text(url)
        for row in parse_rows(text):
            if row.country in target_country_set:
                groups[row.country].append(row)
    for path in source_files:
        text = read_text_file(path)
        for row in parse_rows(text):
            if row.country in target_country_set:
                groups[row.country].append(row)

    ranked_by_country = filter_and_rank_groups(
        groups=groups,
        target_countries=target_countries,
        direct_request_limit=max(1, args.direct_request_limit),
        batch_size=max(1, args.large_group_batch_size),
        max_success_per_group=max(1, args.max_success_per_group),
        shard_workers=max(1, args.shard_workers),
        api_url=args.api_url,
        batch_retries=max(0, args.batch_retries),
    )

    ordered_output: List[str] = []
    for country in target_countries:
        for row, latency in ranked_by_country[country]:
            ordered_output.append(row.format_with_latency(latency))

    with open(args.output, "w", encoding="utf-8") as handle:
        for line in ordered_output:
            handle.write(line + "\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        if args.command == "convert":
            return run_convert(args)
        if args.command == "alive":
            return run_alive(args)
        parser.error(f"Unsupported command: {args.command}")
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
