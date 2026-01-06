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

"""Header detection and merging helpers for XLSX parsing.

This module contains functions for:
- Finding the header row in XLSX worksheets
- Validating expected header structure
- Merging split header rows (when headers span 2 rows)
"""

from __future__ import annotations

import re
from collections.abc import Sequence

from src.core.constants import EXPECTED_XLSX_COLUMN_COUNT


def find_header_row_index(rows: Sequence[Sequence[str | None]]) -> int:
    """Find the index of the header row in the worksheet.

    USDA XLSX files may have headers split across two rows. This function
    checks both single-row headers and merged two-row headers.

    Args:
        rows: All rows from the worksheet

    Returns:
        Index of the header row (0-based), or -1 if not found
    """
    for i, row in enumerate(rows):
        if is_expected_header_row(row):
            return i
        if i + 1 < len(rows):
            merged = merge_header_rows(row, rows[i + 1])
            if is_expected_header_row(merged):
                return i + 1
    return -1


def is_expected_header_row(row: Sequence[str | None]) -> bool:
    """Check if row matches expected USDA header structure.

    Expected columns:
    - [0] Form
    - [1] Average retail price
    - [2] (empty or "Average retail price unit of measure")
    - [3] Preparation yield factor
    - [4] Size of (a) cup equivalent
    - [5] (empty or "Cup equivalent unit of measure")
    - [6] Average price per cup equivalent

    Args:
        row: Row to check

    Returns:
        True if row matches expected header structure
    """
    if len(row) < EXPECTED_XLSX_COLUMN_COUNT:
        return False

    normalized = [normalize_header_cell(cell) for cell in row[:EXPECTED_XLSX_COLUMN_COUNT]]
    if normalized[0] != "form":
        return False
    if normalized[1] != "average retail price":
        return False
    if normalized[2] not in {"", "average retail price unit of measure"}:
        return False
    if normalized[3] != "preparation yield factor":
        return False
    if normalized[4] not in {"size of a cup equivalent", "size of cup equivalent"}:
        return False
    if normalized[5] not in {"", "cup equivalent unit of measure"}:
        return False
    if normalized[6] != "average price per cup equivalent":
        return False
    return True


def normalize_header_cell(value: str | None) -> str:
    """Normalize header cell text for comparison.

    Removes non-alphanumeric characters and normalizes whitespace.

    Args:
        value: Cell text or None

    Returns:
        Normalized lowercase text
    """
    if value is None:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", " ", value.strip().lower())
    return " ".join(normalized.split())


def merge_header_rows(
    primary: Sequence[str | None],
    secondary: Sequence[str | None],
) -> list[str | None]:
    """Merge two header rows into one.

    USDA XLSX files sometimes have headers split across two rows
    (e.g., "Average retail price" in row 1, "unit of measure" in row 2).

    Args:
        primary: First header row
        secondary: Second header row

    Returns:
        Merged header row with combined cell values
    """
    merged: list[str | None] = []
    for index in range(EXPECTED_XLSX_COLUMN_COUNT):
        merged.append(merge_header_cell(safe_cell(primary, index), safe_cell(secondary, index)))
    return merged


def safe_cell(row: Sequence[str | None], index: int) -> str | None:
    """Safely get cell value at index, returning None if out of bounds.

    Args:
        row: Row to access
        index: Column index

    Returns:
        Cell value or None if index is out of bounds
    """
    if index >= len(row):
        return None
    return row[index]


def merge_header_cell(primary: str | None, secondary: str | None) -> str | None:
    """Merge two header cell values.

    Combines primary and secondary text with space separator.

    Args:
        primary: Text from first row
        secondary: Text from second row

    Returns:
        Combined text, or individual text, or None if both empty
    """
    primary_text = (primary or "").strip()
    secondary_text = (secondary or "").strip()
    if primary_text and secondary_text:
        return f"{primary_text} {secondary_text}"
    if primary_text:
        return primary_text
    if secondary_text:
        return secondary_text
    return None
