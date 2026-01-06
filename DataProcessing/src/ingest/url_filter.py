# QUANTCONNECT.COM - Democratizing Finance, Empowering Individuals.
# Lean Algorithmic Trading Engine v2.0. Copyright 2014 QuantConnect Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""URL filtering helpers for source catalog.

This module contains functions for:
- Filtering XLSX and ZIP URLs by year range
- Extracting year from URLs
- Identifying archived fruit/vegetable ZIP files
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from src.core.constants import FILE_YEAR_REGEX


def filter_source_urls(
    xlsx_urls: list[str],
    zip_urls: list[str],
    start_year: int,
    end_year: int,
    max_xlsx_downloads: int,
) -> tuple[list[str], list[str]]:
    """Filter XLSX and ZIP URLs by year range.

    XLSX files may not have years in filenames (e.g., "apples-average-retail-price.xlsx").
    The year is extracted from worksheet titles by xlsx_parser, not from filenames.
    Only ZIP archives are expected to have years in filenames (e.g., "archived-2022-data-tables-for-fruit.zip").

    Args:
        xlsx_urls: List of XLSX file URLs
        zip_urls: List of ZIP file URLs
        start_year: Start of year range (inclusive)
        end_year: End of year range (inclusive)
        max_xlsx_downloads: Maximum number of XLSX files to return (0 = no limit)

    Returns:
        Tuple of (filtered_xlsx_urls, filtered_zip_urls)
    """
    years = set(range(start_year, end_year + 1))
    filtered_zip_urls: list[str] = []
    for url in zip_urls:
        if not is_archived_fruit_and_vegetable_zip(url):
            continue
        year = try_extract_year_from_url(url)
        if year is None or year in years:
            filtered_zip_urls.append(url)
    filtered_zip_urls.sort(key=lambda item: item.lower())

    filtered_xlsx_urls = filter_urls_by_year(xlsx_urls, years)
    filtered_xlsx_urls.sort(key=lambda item: item.lower())

    if max_xlsx_downloads > 0 and len(filtered_xlsx_urls) > max_xlsx_downloads:
        filtered_xlsx_urls = filtered_xlsx_urls[:max_xlsx_downloads]

    return filtered_xlsx_urls, filtered_zip_urls


def filter_urls_by_year(urls: Sequence[str], years: set[int]) -> list[str]:
    """Filter URLs to those matching specified years.

    URLs without a year in the filename are included (year determined later from content).

    Args:
        urls: List of URLs to filter
        years: Set of years to accept

    Returns:
        Filtered list of URLs
    """
    filtered: list[str] = []
    for url in urls:
        year = try_extract_year_from_url(url)
        if year is None or year in years:
            filtered.append(url)
    return filtered


def is_archived_fruit_and_vegetable_zip(url: str) -> bool:
    """Check if URL matches archived fruit/vegetable ZIP pattern.

    Archived ZIPs have filenames like:
    - archived-2022-data-tables-for-fruit.zip
    - archived-2022-data-tables-for-vegetables.zip

    Args:
        url: URL to check

    Returns:
        True if URL matches archived pattern
    """
    file_name = extract_file_name(url)
    if not file_name:
        return False
    lower = file_name.lower()
    return lower.endswith("-for-fruit.zip") or lower.endswith("-for-vegetables.zip")


def try_extract_year_from_url(url: str) -> int | None:
    """Extract year from URL filename.

    Args:
        url: URL to extract year from

    Returns:
        Year as integer, or None if not found
    """
    file_name = extract_file_name(url)
    if not file_name:
        return None
    return try_extract_year_from_file_name(file_name)


def try_extract_year_from_file_name(file_name: str) -> int | None:
    """Extract year from filename using regex.

    Matches 4-digit years (19xx or 20xx) not adjacent to other digits.

    Args:
        file_name: Filename to extract year from

    Returns:
        Year as integer, or None if not found or outside valid range
    """
    match = FILE_YEAR_REGEX.search(file_name)
    if not match:
        return None
    try:
        parsed = int(match.group("year"))
    except ValueError:
        return None
    if parsed < 1900 or parsed > datetime.now(UTC).year + 1:
        return None
    return parsed


def extract_file_name(url: str) -> str | None:
    """Extract filename from URL path.

    Args:
        url: URL to extract filename from

    Returns:
        Filename, or None if URL is invalid or has no path
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return None
    path = parsed.path
    if not path:
        return None
    return Path(path).name
