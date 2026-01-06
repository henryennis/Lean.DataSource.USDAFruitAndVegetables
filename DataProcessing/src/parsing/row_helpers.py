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

"""Row classification helpers for XLSX parsing.

This module contains functions for:
- Detecting group header rows (Fresh, Canned, Juice)
- Detecting footnote rows
- Applying form context from group headers
"""

from __future__ import annotations

from collections.abc import Sequence

from src.core.constants import (
    EXPECTED_XLSX_COLUMN_COUNT,
    XLSX_ALLOWED_GROUP_HEADERS,
    XLSX_CONTACT_PREFIX_REGEX,
    XLSX_ERRATA_PREFIX_REGEX,
    XLSX_FOOTNOTE_PREFIX_REGEX,
    XLSX_SOURCE_KEYWORDS,
    XLSX_SOURCE_PREFIX_REGEX,
)
from src.model.series_code import normalize_form


def row_has_only_form_value(row: Sequence[str | None]) -> bool:
    """Check if row has data only in the form column.

    Group headers and footnotes have text only in the first column.

    Args:
        row: Row to check

    Returns:
        True if all cells after the form column are empty
    """
    for cell in row[1:EXPECTED_XLSX_COLUMN_COUNT]:
        if cell and str(cell).strip():
            return False
    return True


def row_has_any_non_form_value(row: Sequence[str | None]) -> bool:
    """Check if row has data in any column besides the form column.

    Used to detect incomplete data rows (form + partial data).

    Args:
        row: Row to check

    Returns:
        True if any cell after the form column has content
    """
    for cell in row[1:EXPECTED_XLSX_COLUMN_COUNT]:
        if cell and str(cell).strip():
            return True
    return False


def _get_form_text(row: Sequence[str | None], form_value: str) -> str | None:
    """Get form column text if it's the only non-empty cell.

    Args:
        row: Row to check
        form_value: Text from the form column

    Returns:
        Stripped form text, or None if row has other data
    """
    if not form_value:
        return None
    if not row_has_only_form_value(row):
        return None
    text = form_value.strip()
    if not text:
        return None
    return text


def is_group_header_row(row: Sequence[str | None], form_value: str) -> bool:
    """Check if row is a group header (Fresh, Canned, Juice, etc.).

    Group headers have only form column populated with allowed values.

    Args:
        row: Row to check
        form_value: Text from the form column

    Returns:
        True if row is a recognized group header
    """
    header = _get_form_text(row, form_value)
    if not header:
        return False
    normalized = normalize_form(header).strip().lower()
    return normalized in XLSX_ALLOWED_GROUP_HEADERS


def is_footnote_row(row: Sequence[str | None], form_value: str) -> bool:
    """Check if row is a footnote or source citation.

    Footnote rows have patterns like:
    - Start with digits (footnote markers)
    - Start with "Source:" or "Contact:" or "Errata:"
    - Contain "usda" or "economic research service"

    Args:
        row: Row to check
        form_value: Text from the form column

    Returns:
        True if row is a footnote/source row
    """
    text = _get_form_text(row, form_value)
    if not text:
        return False
    if XLSX_FOOTNOTE_PREFIX_REGEX.match(text):
        return True
    if XLSX_SOURCE_PREFIX_REGEX.match(text):
        return True
    if XLSX_CONTACT_PREFIX_REGEX.match(text):
        return True
    if XLSX_ERRATA_PREFIX_REGEX.match(text):
        return True
    lower = text.lower()
    for keyword in XLSX_SOURCE_KEYWORDS:
        if keyword in lower:
            return True
    return False


def apply_form_context(form: str, context: str | None) -> str:
    """Apply group context to form name.

    When a group header (e.g., "Fresh") precedes a form (e.g., "Sliced"),
    the full form becomes "Fresh, Sliced".

    Args:
        form: Form name from current row
        context: Group header context (e.g., "Fresh", "Canned")

    Returns:
        Form with context prefix if applicable
    """
    if not context:
        return form
    if context.lower() in form.lower():
        return form
    return f"{context}, {form}"
