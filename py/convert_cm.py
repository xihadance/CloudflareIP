#!/usr/bin/env python3
"""Export ip:port lines from https://zip.cm.edu.kg/all.json."""

from __future__ import annotations

import argparse
import json
import sys
from urllib.request import urlopen


DEFAULT_URL = "https://zip.cm.edu.kg/all.json"
DEFAULT_OUTPUT = "all_output.txt"


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


def build_output_lines(raw_json: str) -> list[str]:
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

        name = build_display_name(item)
        for raw_port in ports:
            try:
                port = int(raw_port)
            except (TypeError, ValueError):
                continue
            if port < 1 or port > 65535:
                continue
            lines.append(f"{ip}:{port}#{name}")
    return lines


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export ip:port#name lines from cm all.json data",
    )
    parser.add_argument("--url", default=DEFAULT_URL, help="JSON source URL")
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        help="Output file path",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=15.0,
        help="Download timeout in seconds",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        raw_json = fetch_json_text(args.url, timeout=args.timeout)
        lines = build_output_lines(raw_json)
        with open(args.output, "w", encoding="utf-8") as handle:
            handle.write("\n".join(lines))
            if lines:
                handle.write("\n")
        print(f"已输出 {len(lines)} 条到 {args.output}")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
