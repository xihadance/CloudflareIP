import json
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


SOURCE_URLS = (
    "https://raw.githubusercontent.com/MarinaAqua/ProxyIP/main/Alive.txt",
    "https://raw.githubusercontent.com/FoolVPN-ID/Nautica/main/proxyList.txt",
)
API_URL = "https://check.proxyip.cmliussss.net/resolve-batch"
TARGET_COUNTRIES = ("HK", "SG", "US", "JP", "UK", "AU")
RESOLVE_BATCH_LIMIT = 15
LARGE_GROUP_BATCH_SIZE = 15
DIRECT_REQUEST_LIMIT = 15
COUNTRY_WORKERS = 3
MAX_SUCCESS_PER_GROUP = 100
RETRYABLE_HTTP_STATUS_CODES = {408, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}


@dataclass(frozen=True)
class ProxyRow:
    ip: str
    port: str
    country: str
    org: str

    @property
    def ip_port(self) -> str:
        return f"{self.ip}:{self.port}"

    def format_with_latency(self, latency: int = 0) -> str:
        meta = self.country if not self.org else f"{self.country} {self.org}"
        if latency and 0 < latency < 10**9:
            return f"{self.ip}:{self.port}#{meta} ~ {latency}"
        return f"{self.ip}:{self.port}#{meta}"


def fetch_text(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return resp.read().decode("utf-8", errors="replace")


def parse_rows(text: str) -> Iterable[ProxyRow]:
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            continue
        ip, port, country = parts[0], parts[1], parts[2]
        if not ip or not port or not country:
            continue
        org = ",".join(parts[3:]).strip() if len(parts) > 3 else ""
        yield ProxyRow(ip=ip, port=port, country=country, org=org)


def build_batch_request(ips: List[str]) -> Request:
    body = json.dumps({"targets": ips, "ips": ips}, ensure_ascii=False).encode("utf-8")
    return Request(
        API_URL,
        data=body,
        method="POST",
        headers={
            "content-type": "application/json",
            "sec-ch-ua": '"Chromium";v="146", "Not-A.Brand";v="24", "Google Chrome";v="146"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "referer": "https://check.proxyip.cmliussss.net/",
            "origin": "https://check.proxyip.cmliussss.net",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
        },
    )


def is_retryable_batch_error(exc: Exception) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in RETRYABLE_HTTP_STATUS_CODES
    return isinstance(exc, (json.JSONDecodeError, TimeoutError, URLError))


def post_batch(ips: List[str], retries: int = 2) -> List[Dict]:
    if not ips:
        return []

    last_error: Exception | None = None
    for attempt in range(retries + 1):
        try:
            req = build_batch_request(ips)
            with urlopen(req, timeout=30) as resp:
                payload = resp.read().decode("utf-8", errors="replace")
            data = json.loads(payload)
            if isinstance(data, dict):
                if "results" in data and isinstance(data["results"], list):
                    normalized: List[Dict] = []
                    for item in data["results"]:
                        if not isinstance(item, dict):
                            continue
                        targets = item.get("targets")
                        input_target = str(item.get("input", "")).strip()
                        error = item.get("error")
                        if isinstance(targets, list) and targets and not error:
                            for target in targets:
                                t_str = str(target).strip()
                                if t_str.startswith("["):
                                    idx = t_str.rfind("]:")
                                    if idx != -1:
                                        ip, port = t_str[: idx + 1], t_str[idx + 2 :]
                                    else:
                                        ip, port = t_str, "443"
                                elif ":" in t_str:
                                    ip, port = t_str.rsplit(":", 1)
                                else:
                                    ip, port = t_str, "443"
                                normalized.append({
                                    "ip": ip,
                                    "port": port,
                                    "input": input_target,
                                    "valid": True,
                                    "latency": item.get("latency", 0),
                                })
                        elif item.get("valid"):
                            normalized.append(item)
                    return normalized
                if "data" in data and isinstance(data["data"], list):
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
    left = post_batch(ips[:midpoint], retries=retries)
    right = post_batch(ips[midpoint:], retries=retries)
    return left + right


def get_batch_size(total_rows: int) -> int:
    if total_rows <= 0:
        return 1
    if total_rows <= DIRECT_REQUEST_LIMIT:
        return total_rows
    return LARGE_GROUP_BATCH_SIZE


def chunked(items: List[ProxyRow], size: int) -> Iterable[List[ProxyRow]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def filter_and_rank(rows: List[ProxyRow]) -> List[Tuple[ProxyRow, int]]:
    results: List[Tuple[ProxyRow, int]] = []
    if not rows:
        return results

    batch_size = get_batch_size(len(rows))
    by_ip_port = {row.ip_port: row for row in rows}
    seen_keys: set = set()
    for batch in chunked(rows, batch_size):
        if len(results) >= MAX_SUCCESS_PER_GROUP:
            break
        ips = [row.ip_port for row in batch]
        items = post_batch(ips)
        for item in items:
            if not isinstance(item, dict):
                continue
            if not item.get("valid"):
                continue
            ip = str(item.get("ip", "")).strip()
            port = str(item.get("port", "")).strip()
            if not ip or not port:
                continue
            key = f"{ip}:{port}"
            row = by_ip_port.get(key)
            if not row:
                input_key = str(item.get("input", "")).strip()
                row = by_ip_port.get(input_key)
            if not row or row.ip_port in seen_keys:
                continue
            seen_keys.add(row.ip_port)
            latency = item.get("latency")
            try:
                latency_value = int(latency)
            except Exception:
                latency_value = 10**9
            results.append((row, latency_value))
        # Stop further batches once we already have enough successful IPs.
        if len(results) >= MAX_SUCCESS_PER_GROUP:
            break
    return sorted(results, key=lambda x: x[1])[:MAX_SUCCESS_PER_GROUP]


def get_country_workers(groups: Dict[str, List[ProxyRow]]) -> int:
    active_group_count = sum(1 for country in TARGET_COUNTRIES if groups.get(country))
    if active_group_count <= 1:
        return 1
    return min(COUNTRY_WORKERS, active_group_count)


def rank_groups_by_country(groups: Dict[str, List[ProxyRow]]) -> Dict[str, List[Tuple[ProxyRow, int]]]:
    ranked_by_country: Dict[str, List[Tuple[ProxyRow, int]]] = {
        country: [] for country in TARGET_COUNTRIES
    }
    workers = get_country_workers(groups)
    if workers == 1:
        for country in TARGET_COUNTRIES:
            ranked_by_country[country] = filter_and_rank(groups.get(country, []))
        return ranked_by_country

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_country = {
            executor.submit(filter_and_rank, groups.get(country, [])): country
            for country in TARGET_COUNTRIES
            if groups.get(country)
        }
        for future in as_completed(future_to_country):
            country = future_to_country[future]
            ranked_by_country[country] = future.result()
    return ranked_by_country


def main(output_path: str) -> int:

    groups: Dict[str, List[ProxyRow]] = defaultdict(list)
    for url in SOURCE_URLS:
        text = fetch_text(url)
        for row in parse_rows(text):
            if row.country in TARGET_COUNTRIES:
                groups[row.country].append(row)

    ranked_by_country = rank_groups_by_country(groups)
    ordered_output: List[str] = []
    for country in TARGET_COUNTRIES:
        ranked = ranked_by_country[country]
        for row, latency in ranked:
            ordered_output.append(row.format_with_latency(latency))

    with open(output_path, "w", encoding="utf-8") as f:
        for line in ordered_output:
            f.write(line + "\n")

    return 0


if __name__ == "__main__":
    output = "Alive.txt"
    if len(sys.argv) > 1 and sys.argv[1].strip():
        output = sys.argv[1].strip()
    raise SystemExit(main(output))
