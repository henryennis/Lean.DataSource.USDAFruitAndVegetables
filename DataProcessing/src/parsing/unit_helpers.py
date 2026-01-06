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

"""Unit parsing and decimal conversion helpers for XLSX parsing.

This module contains functions for:
- Parsing decimal values from cell text
- Parsing price units (per pound, per pint)
- Parsing cup equivalent units (fluid ounces, pints, pounds)
"""

from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation

from src.model.dataset_types import CupEquivalentUnit, PriceUnit


def try_parse_decimal(value: str | None) -> Decimal | None:
    """Parse decimal value from cell text.

    Args:
        value: Cell text or None

    Returns:
        Decimal value, or None if empty or invalid
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return Decimal(text)
    except InvalidOperation:
        return None


def normalize_unit_text(value: str | None) -> str:
    """Normalize unit text for comparison.

    Removes non-alphanumeric characters and normalizes whitespace.

    Args:
        value: Unit text or None

    Returns:
        Normalized lowercase text
    """
    if value is None:
        return ""
    normalized = re.sub(r"[^a-z0-9]+", " ", value.strip().lower())
    return " ".join(normalized.split())


def parse_price_unit(value: str | None) -> PriceUnit:
    """Parse price unit from cell text.

    USDA price units include:
    - "per pound" → PER_POUND
    - "per pint", "per pint 16 fluid ounces ..." → PER_PINT

    Args:
        value: Unit text from cell

    Returns:
        PriceUnit enum value, or UNKNOWN if not recognized
    """
    normalized = normalize_unit_text(value)
    if normalized == "per pound":
        return PriceUnit.PER_POUND
    if normalized in {
        "per pint",
        "per pint 16 fluid ounces ready to drink",
        "per pint 16 fluid ounces concentrate",
    }:
        return PriceUnit.PER_PINT
    return PriceUnit.UNKNOWN


def parse_cup_equivalent_unit(value: str | None) -> CupEquivalentUnit:
    """Parse cup equivalent unit from cell text.

    USDA cup equivalent units include:
    - "fl oz", "fluid ounces" → FLUID_OUNCES
    - "pints" → PINTS
    - "pound", "pounds" → POUNDS

    Args:
        value: Unit text from cell

    Returns:
        CupEquivalentUnit enum value, or UNKNOWN if not recognized
    """
    normalized = normalize_unit_text(value)
    if normalized in {"fl oz", "floz", "fluid ounce", "fluid ounces"}:
        return CupEquivalentUnit.FLUID_OUNCES
    if normalized == "pints":
        return CupEquivalentUnit.PINTS
    if normalized in {"pound", "pounds"}:
        return CupEquivalentUnit.POUNDS
    return CupEquivalentUnit.UNKNOWN
