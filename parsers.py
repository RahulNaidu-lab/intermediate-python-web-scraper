"""
Parsers Module

This module provides core web scraping functionality including:
- HTML parsing and DOM traversal
- CSS selector-based data extraction
- Data cleaning and normalization
- URL handling and validation
"""

import re
from typing import List, Dict, Any, Optional
from urllib.parse import urljoin, urlparse

try:
    from bs4 import BeautifulSoup
except ImportError:
    raise ImportError("beautifulsoup4 is required. Install with: pip install beautifulsoup4")


class HTMLParser:
    """Parse and extract data from HTML content using BeautifulSoup."""

    def __init__(self, html_content: str, base_url: str = ""):
        """
        Initialize the HTML parser.

        Args:
            html_content: The HTML content to parse
            base_url: Base URL for resolving relative links
        """
        self.html_content = html_content
        self.base_url = base_url
        self.soup = BeautifulSoup(html_content, "html.parser")

    def extract_by_selector(self, selector: str) -> List[str]:
        """
        Extract text content using CSS selector.

        Example:
            # Extract all product names from divs with class 'product-name'
            names = parser.extract_by_selector("div.product-name")
        """
        elements = self.soup.select(selector)
        return [elem.get_text(strip=True) for elem in elements]

    def extract_by_selector_attr(self, selector: str, attr: str) -> List[str]:
        """
        Extract attribute values using CSS selector.

        Example:
            # Extract all product URLs
            urls = parser.extract_by_selector_attr("a.product-link", "href")
        """
        elements = self.soup.select(selector)
        values = []
        for elem in elements:
            value = elem.get(attr)
            if value:
                values.append(value)
        return values

    def extract_table(self, table_selector: str) -> List[Dict[str, str]]:
        """
        Extract table data into list of dictionaries.

        Args:
            table_selector: CSS selector for the table element

        Returns:
            List of dictionaries where keys are headers and values are cell data
        """
        table = self.soup.select_one(table_selector)
        if not table:
            return []

        rows = table.find_all("tr")
        if not rows:
            return []

        # Extract headers from first row or thead
        headers = []
        header_row = table.find("thead")
        if header_row:
            headers = [th.get_text(strip=True) for th in header_row.find_all(["th", "td"])]
        else:
            headers = [td.get_text(strip=True) for td in rows[0].find_all(["th", "td"])]
            rows = rows[1:]

        # Extract data rows
        data = []
        for row in rows:
            cells = row.find_all("td")
            if len(cells) == len(headers):
                data.append(dict(zip(headers, [cell.get_text(strip=True) for cell in cells])))

        return data

    def resolve_url(self, relative_url: str) -> str:
        """
        Resolve relative URLs to absolute URLs.

        Example:
            # Convert relative URL to absolute
            absolute_url = parser.resolve_url("/products/item") 
            # Returns: https://example.com/products/item
        """
        if not self.base_url:
            return relative_url
        return urljoin(self.base_url, relative_url)

    def get_all_elements(self, selector: str) -> List[Any]:
        """Get all elements matching a CSS selector for advanced manipulation."""
        return self.soup.select(selector)


class DataCleaner:
    """Clean and normalize extracted data."""

    @staticmethod
    def clean_text(text: str) -> str:
        """
        Clean text by removing extra whitespace and special characters.
        """
        if not text:
            return ""
        # Remove extra whitespace
        text = re.sub(r"\s+", " ", text).strip()
        return text

    @staticmethod
    def clean_price(price_str: str) -> Optional[float]:
        """
        Extract numeric price from string.

        Example:
            # Convert "$19.99" to 19.99
            price = DataCleaner.clean_price("$19.99")
        """
        if not price_str:
            return None
        match = re.search(r"\d+(?:\.\d{2})?", price_str)
        return float(match.group()) if match else None

    @staticmethod
    def clean_list(items: List[str]) -> List[str]:
        """Remove empty strings and clean all items in a list."""
        return [DataCleaner.clean_text(item) for item in items if item]

    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """Convert all types of whitespace to single spaces."""
        return " ".join(text.split())


class URLValidator:
    """Validate and sanitize URLs."""

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """
        Check if URL is valid.

        Returns:
            True if URL has a valid scheme and netloc
        """
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False

    @staticmethod
    def get_domain(url: str) -> Optional[str]:
        """
        Extract domain from URL.

        Example:
            domain = URLValidator.get_domain("https://www.example.com/path")
            # Returns: example.com
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.replace("www.", "")
            return domain if domain else None
        except Exception:
            return None
