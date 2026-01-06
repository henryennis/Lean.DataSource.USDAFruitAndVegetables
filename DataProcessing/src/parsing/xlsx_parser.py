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

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from src.core.constants import EXPECTED_XLSX_COLUMN_COUNT, MAX_XLSX_FILE_SIZE_BYTES
from src.model.dataset_types import CupEquivalentUnit, PriceUnit, SeriesPoint
from src.model.series_code import get_series_code, normalize_form
from src.parsing.header_helpers import find_header_row_index
from src.parsing.metadata_helpers import get_product_name, get_title, try_get_year
from src.parsing.row_helpers import apply_form_context, is_footnote_row, is_group_header_row, row_has_any_non_form_value
from src.parsing.unit_helpers import parse_cup_equivalent_unit, parse_price_unit, try_parse_decimal
from src.parsing.xlsx_reader import read_sheets


class _SeriesPointModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    series_code: str = Field(min_length=1)
    product_name: str = Field(min_length=1)
    form: str = Field(min_length=1)
    date: date
    average_retail_price: Decimal
    unit: PriceUnit
    preparation_yield_factor: Decimal
    cup_equivalent_size: Decimal
    cup_equivalent_unit: CupEquivalentUnit
    price_per_cup_equivalent: Decimal

    @field_validator(
        "average_retail_price", "preparation_yield_factor", "cup_equivalent_size", "price_per_cup_equivalent"
    )
    @classmethod
    def _ensure_decimal_values(cls, value: Decimal) -> Decimal:
        return value


def parse_xlsx(file_path: Path) -> list[SeriesPoint]:
    """Parse USDA XLSX workbook into SeriesPoints.

    Per Constitution: Fail-fast - all validation errors raise ValueError with context.

    Structure:
    1. Read all sheets from workbook (xlsx_reader.read_sheets)
    2. For each sheet:
       a. Find header row (Form, Average retail price, ...)
       b. Extract year from title or sheet name
       c. Parse data rows into SeriesPoints
       d. Handle non-data rows: group headers (Fresh, Canned, Juice) and footnotes
    3. Return accumulated points

    Helpers:
    - _find_header_row_index: Locate header in first rows (may be split across 2 rows)
    - _try_get_year: Extract 4-digit year from text
    - _parse_price_unit / _parse_cup_equivalent_unit: Map text to enum values
    - _is_group_header_row / _is_footnote_row: Identify non-data rows

    Args:
        file_path: Path to XLSX workbook

    Returns:
        List of validated SeriesPoint objects

    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If validation fails (missing year, header, units, etc.)
    """
    if not Path(file_path).exists():
        raise FileNotFoundError("XLSX file not found", file_path)

    # R-1: Validate file size before opening to prevent memory exhaustion
    file_size = Path(file_path).stat().st_size
    if file_size > MAX_XLSX_FILE_SIZE_BYTES:
        raise ValueError(
            f"XLSX file exceeds size limit ({file_size:,} bytes > {MAX_XLSX_FILE_SIZE_BYTES:,} bytes): {file_path}"
        )

    sheets = read_sheets(Path(file_path), EXPECTED_XLSX_COLUMN_COUNT)
    if not sheets:
        raise ValueError(f"XLSX file has no worksheets: {file_path}")

    points: list[SeriesPoint] = []

    for sheet_name, rows in sheets:
        if not rows:
            raise ValueError(f"XLSX worksheet has no rows: {file_path} ({sheet_name})")

        header_row_index = find_header_row_index(rows)
        if header_row_index < 0:
            raise ValueError(f"XLSX worksheet missing expected header row: {file_path} ({sheet_name})")

        title = get_title(rows, header_row_index)
        year_text = title or sheet_name
        year = try_get_year(year_text)
        if year is None:
            raise ValueError(f"XLSX worksheet missing year in title: {file_path} ({sheet_name})")

        # USDA data is annual; use Jan 1 as the observation date for the year
        row_date = date(year, 1, 1)
        product_name = get_product_name(sheet_name, title)
        if not product_name:
            raise ValueError(f"XLSX worksheet missing product name: {file_path} ({sheet_name})")

        current_group: str | None = None

        for row_index, row in enumerate(rows[header_row_index + 1 :], start=header_row_index + 1):
            row_number = row_index + 1
            form_value = (row[0] or "").strip()
            if not form_value:
                if row_has_any_non_form_value(row):
                    raise ValueError(
                        f"XLSX row missing form value but has other data: {file_path} ({sheet_name}) row {row_number}"
                    )
                continue

            average_retail_price = try_parse_decimal(row[1])
            price_per_cup_equivalent = try_parse_decimal(row[6])
            cup_equivalent_size = try_parse_decimal(row[4])
            preparation_yield_factor = try_parse_decimal(row[3])

            if (
                average_retail_price is None
                or price_per_cup_equivalent is None
                or cup_equivalent_size is None
                or preparation_yield_factor is None
            ):
                if is_group_header_row(row, form_value):
                    current_group = normalize_form(form_value)
                    continue
                if is_footnote_row(row, form_value):
                    continue
                raise ValueError(
                    f"XLSX row missing numeric fields: {file_path} ({sheet_name}) row {row_number} form '{form_value}'"
                )

            form_with_context = apply_form_context(form_value, current_group)
            unit_text = (row[2] or "").strip()
            if not unit_text:
                raise ValueError(
                    f"XLSX row missing price unit: {file_path} ({sheet_name}) row {row_number} form '{form_with_context}'"
                )
            unit = parse_price_unit(unit_text)
            if unit == PriceUnit.UNKNOWN:
                raise ValueError(
                    f"XLSX row has unknown price unit '{unit_text}': {file_path} ({sheet_name}) row {row_number} form '{form_with_context}'"
                )

            cup_unit_text = (row[5] or "").strip()
            if not cup_unit_text:
                raise ValueError(
                    f"XLSX row missing cup equivalent unit: {file_path} ({sheet_name}) row {row_number} form '{form_with_context}'"
                )
            cup_unit = parse_cup_equivalent_unit(cup_unit_text)
            if cup_unit == CupEquivalentUnit.UNKNOWN:
                raise ValueError(
                    f"XLSX row has unknown cup equivalent unit '{cup_unit_text}': {file_path} ({sheet_name}) row {row_number} form '{form_with_context}'"
                )
            series_code = get_series_code(product_name, form_with_context)
            normalized_form = normalize_form(form_with_context)
            try:
                model = _SeriesPointModel(
                    series_code=series_code,
                    product_name=product_name,
                    form=normalized_form,
                    date=row_date,
                    average_retail_price=average_retail_price,
                    unit=unit,
                    preparation_yield_factor=preparation_yield_factor,
                    cup_equivalent_size=cup_equivalent_size,
                    cup_equivalent_unit=cup_unit,
                    price_per_cup_equivalent=price_per_cup_equivalent,
                )
            except ValidationError as err:
                error_details = "; ".join([f"{e['loc'][0]}: {e['msg']}" for e in err.errors()])
                raise ValueError(
                    f"XLSX row failed validation: {file_path} ({sheet_name}) row {row_number} "
                    f"form '{form_with_context}' - {error_details}"
                ) from err
            points.append(SeriesPoint(**model.model_dump()))

    return points
