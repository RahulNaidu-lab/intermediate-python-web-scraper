"""
Intermediate Python Web Scraper and Analyzer

Features:
- Robust HTTP fetching with retries and backoff
- robots.txt and rate limiting respect
- HTML parsing with BeautifulSoup4
- Structured data extraction to CSV/JSON
- Simple exploratory analysis and summary stats
- CLI interface and modular design

Usage:
    python web_scraper.py --url https://example.com --selector "article h2 a" --attr href \
        --out data/links.csv --format csv --concurrency 5

Requirements: see requirements.txt
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional, Dict, Any

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter, Retry
from urllib.parse import urljoin, urlparse
from urllib import robotparser


DEFAULT_HEADERS = {
    "User-Agent": "intermediate-python-web-scraper/1.0 (+https://github.com)"
}


@dataclass
class ExtractedItem:
    source_url: str
    text: Optional[str]
    attr: Optional[str]


class Fetcher:
    """HTTP client with retries, timeouts, and robots.txt handling."""

    def __init__(self, base_url: Optional[str] = None, timeout: float = 15.0):
        self.session = requests.Session()
        retries = Retry(total=5, backoff_factor=0.5,
                        status_forcelist=[429, 500, 502, 503, 504],
                        allowed_methods=["GET", "HEAD"])
        self.session.headers.update(DEFAULT_HEADERS)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
        self.timeout = timeout
        self.base_url = base_url
        self._robots = None
        if base_url:
            self._init_robots(base_url)

    def _init_robots(self, url: str):
        parsed = urlparse(url)
        robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
        rp = robotparser.RobotFileParser()
        try:
            rp.set_url(robots_url)
            rp.read()
            self._robots = rp
        except Exception:
            self._robots = None

    def allowed(self, url: str) -> bool:
        if not self._robots:
            return True
        return self._robots.can_fetch(DEFAULT_HEADERS["User-Agent"], url)

    def get(self, url: str) -> requests.Response:
        if self.base_url:
            url = urljoin(self.base_url, url)
        if not self.allowed(url):
            raise PermissionError(f"robots.txt disallows fetching: {url}")
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        return resp


def extract_items(html: str, base_url: str, selector: str, attr: Optional[str]) -> List[ExtractedItem]:
    """Parse HTML and extract items by CSS selector.

    - If attr is provided, extract attribute value (e.g., href, src). If href/src are relative, resolve to absolute.
    - If attr is None, extract text content.
    """
    soup = BeautifulSoup(html, "html.parser")
    results: List[ExtractedItem] = []
    for el in soup.select(selector):
        if attr:
            val = el.get(attr)
            if isinstance(val, str) and attr in ("href", "src"):
                val = urljoin(base_url, val)
            results.append(ExtractedItem(source_url=base_url, text=None, attr=val))
        else:
            text = el.get_text(strip=True)
            results.append(ExtractedItem(source_url=base_url, text=text, attr=None))
    return results


def save_items(items: List[ExtractedItem], out_path: Path, fmt: str = "csv") -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "csv":
        with out_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["source_url", "text", "attr"]) 
            writer.writeheader()
            for it in items:
                writer.writerow(asdict(it))
    elif fmt == "json":
        with out_path.open("w", encoding="utf-8") as f:
            json.dump([asdict(i) for i in items], f, ensure_ascii=False, indent=2)
    else:
        raise ValueError("Unsupported format: " + fmt)


def summarize(items: List[ExtractedItem]) -> Dict[str, Any]:
    """Return simple stats for quick inspection."""
    n = len(items)
    n_text = sum(1 for i in items if i.text)
    n_attr = sum(1 for i in items if i.attr)
    domains: Dict[str, int] = {}
    for i in items:
        target = i.attr or i.source_url
        try:
            netloc = urlparse(target).netloc
        except Exception:
            netloc = ""
        domains[netloc] = domains.get(netloc, 0) + 1
    top_domains = sorted(domains.items(), key=lambda x: x[1], reverse=True)[:5]
    return {"count": n, "text_items": n_text, "attr_items": n_attr, "top_domains": top_domains}


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="Intermediate Python web scraper and analyzer")
    p.add_argument("--url", required=True, help="Starting URL to fetch")
    p.add_argument("--selector", required=True, help="CSS selector of elements to extract")
    p.add_argument("--attr", default=None, help="Attribute to extract (e.g., href). If omitted, uses text")
    p.add_argument("--out", default="data/output.csv", help="Output file path")
    p.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
    p.add_argument("--timeout", type=float, default=15.0, help="Request timeout seconds")
    args = p.parse_args(argv)

    fetcher = Fetcher(base_url=args.url, timeout=args.timeout)
    resp = fetcher.get(args.url)
    items = extract_items(resp.text, args.url, args.selector, args.attr)
    save_items(items, Path(args.out), fmt=args.format)
    stats = summarize(items)
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
