#!/usr/bin/env python3
"""Export ip:port lines from https://zip.cm.edu.kg/all.json."""

from __future__ import annotations

import argparse
import json
import re
import sys
from urllib.request import urlopen


DEFAULT_URL = "https://zip.cm.edu.kg/all.json"
DEFAULT_OUTPUT = "all_output.txt"
DEFAULT_FORMAT_TEMPLATE = "{$.ip}:{$.port}#{$.meta.country_cn} {$.meta.asOrganization} {$.meta.city}"
PLACEHOLDER_PATTERN = re.compile(r"\{\s*(\$\.[^{}]+?)\s*\}")


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
    parser.add_argument(
        "--format",
        default=DEFAULT_FORMAT_TEMPLATE,
        help="Output template, for example '{$.ip}:{$.port}#{$.meta.country_cn}'",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)

    try:
        raw_json = fetch_json_text(args.url, timeout=args.timeout)
        lines = build_output_lines(raw_json, format_template=args.format)
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
