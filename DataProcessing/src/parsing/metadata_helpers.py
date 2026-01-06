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

"""Metadata extraction helpers for XLSX parsing.

This module contains functions for:
- Extracting year from worksheet titles or sheet names
- Finding the title row before the header
- Extracting product name from title or sheet name
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime

from src.core.constants import SHEET_YEAR_REGEX


def try_get_year(text: str) -> int | None:
    """Extract 4-digit year from text.

    USDA worksheet titles typically contain the year, e.g., "Apples 2022".
    Searches from right to left to prefer the most recent year if multiple exist.

    Args:
        text: Text to search (title or sheet name)

    Returns:
        Year as integer, or None if not found or outside valid range (1900-next year)
    """
    if not text:
        return None
    matches = list(SHEET_YEAR_REGEX.finditer(text))
    if not matches:
        return None
    # USDA fruit/vegetable data starts ~2000; 1900 lower bound for historical archives
    max_year = datetime.now(UTC).year + 1
    for match in reversed(matches):
        year_text = match.group("year")
        try:
            parsed = int(year_text)
        except ValueError:
            continue
        if 1900 <= parsed <= max_year:
            return parsed
    return None


def get_title(rows: Sequence[Sequence[str | None]], header_row_index: int) -> str | None:
    """Get worksheet title from rows above the header.

    The title is typically in the first cell of the first row(s) before
    the header row. This contains the product name and year.

    Args:
        rows: All rows from the worksheet
        header_row_index: Index of the header row

    Returns:
        Title text, or None if not found
    """
    for i in range(header_row_index):
        value = rows[i][0]
        if value and value.strip():
            return value.strip()
    return None


def get_product_name(sheet_name: str, title: str | None) -> str:
    """Extract product name from title or sheet name.

    Prefers the title if available, splitting on em-dash or hyphen
    to remove the year portion (e.g., "Apples — 2022" → "Apples").

    Args:
        sheet_name: Worksheet name (fallback)
        title: Title row text (preferred)

    Returns:
        Product name
    """
    if title:
        trimmed = title.strip()
        split_title = _split_title(trimmed)
        if split_title:
            return split_title
    return sheet_name.strip()


def _split_title(title: str) -> str | None:
    """Split title on em-dash or hyphen to extract product name.

    USDA titles follow patterns like:
    - "Apples — 2022"
    - "Bananas - 2021"

    Args:
        title: Full title text

    Returns:
        Text before delimiter, or None if no delimiter found
    """
    for delimiter in ("\u2014", " - "):
        dash_index = title.find(delimiter)
        if dash_index > 0:
            return title[:dash_index].strip()
    return None
