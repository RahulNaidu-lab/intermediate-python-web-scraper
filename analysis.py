"""
Analysis Module

This module provides data analysis and statistical functions for scraped data:
- Data aggregation and grouping
- Statistical calculations
- Trend analysis
- Data export and reporting
- Data validation and quality checks
"""

from typing import List, Dict, Any, Optional, Tuple
from collections import Counter, defaultdict
import json
import csv
from pathlib import Path


class DataAggregator:
    """Aggregate and group scraped data."""

    @staticmethod
    def group_by(data: List[Dict[str, Any]], key: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group data by a specific key.

        Example:
            data = [{"category": "electronics", "price": 100}, 
                    {"category": "books", "price": 15}]
            grouped = DataAggregator.group_by(data, "category")
            # Returns: {"electronics": [...], "books": [...]}
        """
        grouped = defaultdict(list)
        for item in data:
            if key in item:
                grouped[item[key]].append(item)
        return dict(grouped)

    @staticmethod
    def count_occurrences(data: List[Dict[str, Any]], key: str) -> Dict[str, int]:
        """
        Count occurrences of values for a specific key.

        Example:
            counts = DataAggregator.count_occurrences(data, "category")
            # Returns frequency of each category
        """
        values = [item.get(key) for item in data if key in item]
        return dict(Counter(values))

    @staticmethod
    def filter_data(data: List[Dict[str, Any]], key: str, value: Any) -> List[Dict[str, Any]]:
        """
        Filter data by key-value pair.

        Example:
            expensive = DataAggregator.filter_data(data, "price", lambda p: p > 100)
        """
        if callable(value):
            return [item for item in data if key in item and value(item[key])]
        return [item for item in data if item.get(key) == value]


class StatisticalAnalyzer:
    """Perform statistical analysis on numeric data."""

    @staticmethod
    def get_numeric_values(data: List[Dict[str, Any]], key: str) -> List[float]:
        """Extract numeric values from data for a specific key."""
        values = []
        for item in data:
            if key in item:
                try:
                    values.append(float(item[key]))
                except (ValueError, TypeError):
                    continue
        return values

    @staticmethod
    def calculate_average(values: List[float]) -> Optional[float]:
        """
        Calculate average of numeric values.

        Example:
            avg_price = StatisticalAnalyzer.calculate_average([100, 150, 200])
            # Returns: 150.0
        """
        if not values:
            return None
        return sum(values) / len(values)

    @staticmethod
    def calculate_median(values: List[float]) -> Optional[float]:
        """Calculate median of numeric values."""
        if not values:
            return None
        sorted_values = sorted(values)
        n = len(sorted_values)
        if n % 2 == 0:
            return (sorted_values[n // 2 - 1] + sorted_values[n // 2]) / 2
        return sorted_values[n // 2]

    @staticmethod
    def calculate_min_max(values: List[float]) -> Tuple[Optional[float], Optional[float]]:
        """Calculate minimum and maximum values."""
        if not values:
            return None, None
        return min(values), max(values)

    @staticmethod
    def get_statistics(data: List[Dict[str, Any]], key: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a numeric field.

        Returns:
            Dictionary with count, sum, average, median, min, max
        """
        values = StatisticalAnalyzer.get_numeric_values(data, key)
        if not values:
            return {}

        return {
            "count": len(values),
            "sum": sum(values),
            "average": StatisticalAnalyzer.calculate_average(values),
            "median": StatisticalAnalyzer.calculate_median(values),
            "min": min(values),
            "max": max(values),
        }


class DataValidator:
    """Validate and check data quality."""

    @staticmethod
    def check_required_fields(data: List[Dict[str, Any]], required_fields: List[str]) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Check if all records have required fields.

        Returns:
            Tuple of (all_valid, invalid_records)
        """
        invalid = []
        for item in data:
            if not all(field in item for field in required_fields):
                invalid.append(item)
        return len(invalid) == 0, invalid

    @staticmethod
    def remove_duplicates(data: List[Dict[str, Any]], key: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Remove duplicate records.

        Args:
            data: List of dictionaries
            key: Optional key to check for duplicates. If None, checks entire record.
        """
        if key:
            seen = set()
            unique = []
            for item in data:
                if key in item and item[key] not in seen:
                    seen.add(item[key])
                    unique.append(item)
            return unique
        else:
            # Remove completely identical records
            return [dict(t) for t in {tuple(sorted(d.items())) for d in data}]


class DataExporter:
    """Export data to various formats."""

    @staticmethod
    def to_json(data: List[Dict[str, Any]], file_path: str, pretty: bool = True) -> None:
        """
        Export data to JSON file.

        Example:
            DataExporter.to_json(data, "output/results.json")
        """
        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        with open(file_path, "w") as f:
            json.dump(data, f, indent=2 if pretty else None, default=str)

    @staticmethod
    def to_csv(data: List[Dict[str, Any]], file_path: str) -> None:
        """
        Export data to CSV file.

        Example:
            DataExporter.to_csv(data, "output/results.csv")
        """
        if not data:
            return

        Path(file_path).parent.mkdir(parents=True, exist_ok=True)
        keys = data[0].keys()
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

    @staticmethod
    def generate_report(data: List[Dict[str, Any]], stats_field: Optional[str] = None) -> Dict[str, Any]:
        """
        Generate a summary report of the data.

        Returns:
            Dictionary with data summary and statistics
        """
        report = {
            "total_records": len(data),
            "fields": list(data[0].keys()) if data else [],
        }

        if stats_field and data:
            report[f"{stats_field}_stats"] = StatisticalAnalyzer.get_statistics(data, stats_field)

        return report
