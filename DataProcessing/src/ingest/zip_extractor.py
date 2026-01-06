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

"""ZIP extraction helpers for source catalog.

This module contains functions for:
- Extracting XLSX files from ZIP archives
- Handling filename collisions with UUID suffixes
"""

from __future__ import annotations

import io
import uuid
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src.core.constants import (
    DOWNLOADER_LOG_PREFIX,
    MAX_EXTRACTED_FILE_SIZE_BYTES,
    MAX_TOTAL_EXTRACTED_BYTES,
    MAX_ZIP_COMPRESSION_RATIO,
    MAX_ZIP_FILE_SIZE_BYTES,
)
from src.core.logging_utils import Logger


@dataclass(frozen=True)
class ExtractedFile:
    """A file extracted from a ZIP archive.

    Attributes:
        path: Path to the extracted file on disk
        source_description: Description of the source (e.g., "archive.zip::file.xlsx")
    """

    path: Path
    source_description: str


def extract_xlsx_from_zip(
    zip_bytes: bytes,
    zip_source: str,
    dest_folder: Path,
    logger: Logger,
    uuid_factory: Callable[[], uuid.UUID] | None = None,
) -> list[ExtractedFile]:
    """Extract XLSX files from ZIP archive with safety limits.

    Returns list of extracted files (no mutation of external state).
    Handles filename collisions by appending UUID suffix.

    Per Constitution: Fail-fast - validates archive size, individual file sizes,
    cumulative extraction size, and compression ratios to protect against ZIP bombs.

    Args:
        zip_bytes: Raw bytes of the ZIP archive
        zip_source: Source identifier for logging (URL or filename)
        dest_folder: Directory to extract files to
        logger: Logger for trace messages
        uuid_factory: Factory for generating UUIDs (default: uuid.uuid4)

    Returns:
        List of extracted files with paths and source descriptions

    Raises:
        ValueError: If archive exceeds size limits or has suspicious compression ratio
    """
    uuid_factory = uuid_factory or uuid.uuid4
    extracted: list[ExtractedFile] = []

    # R-4: Validate archive size before opening
    archive_size = len(zip_bytes)
    if archive_size > MAX_ZIP_FILE_SIZE_BYTES:
        raise ValueError(
            f"ZIP archive too large: {zip_source} "
            f"({archive_size / (1024 * 1024):.1f} MB > "
            f"{MAX_ZIP_FILE_SIZE_BYTES / (1024 * 1024):.0f} MB limit)"
        )

    total_extracted_bytes = 0

    with zipfile.ZipFile(_io_bytes(zip_bytes)) as archive:
        # R-4: Check compression ratio for entire archive
        total_compressed = sum(e.compress_size for e in archive.infolist() if e.compress_size > 0)
        total_uncompressed = sum(e.file_size for e in archive.infolist())
        if total_compressed > 0:
            ratio = total_uncompressed / total_compressed
            if ratio > MAX_ZIP_COMPRESSION_RATIO:
                raise ValueError(
                    f"ZIP archive has suspicious compression ratio: {zip_source} "
                    f"({ratio:.0f}:1 > {MAX_ZIP_COMPRESSION_RATIO}:1 limit)"
                )

        for entry in archive.infolist():
            if not entry.filename:
                logger.trace(_with_prefix(f"Skipping empty filename entry in {zip_source}"))
                continue
            if entry.filename.endswith("/"):
                logger.trace(_with_prefix(f"Skipping directory entry: {entry.filename}"))
                continue
            if not entry.filename.lower().endswith(".xlsx"):
                logger.trace(_with_prefix(f"Skipping non-XLSX entry: {entry.filename}"))
                continue

            # R-4: Check individual file size before extraction
            if entry.file_size > MAX_EXTRACTED_FILE_SIZE_BYTES:
                raise ValueError(
                    f"ZIP entry too large: {entry.filename} in {zip_source} "
                    f"({entry.file_size / (1024 * 1024):.1f} MB > "
                    f"{MAX_EXTRACTED_FILE_SIZE_BYTES / (1024 * 1024):.0f} MB limit)"
                )

            # R-4: Check cumulative extraction size
            total_extracted_bytes += entry.file_size
            if total_extracted_bytes > MAX_TOTAL_EXTRACTED_BYTES:
                raise ValueError(
                    f"ZIP extraction exceeds total size limit: {zip_source} "
                    f"({total_extracted_bytes / (1024 * 1024):.1f} MB > "
                    f"{MAX_TOTAL_EXTRACTED_BYTES / (1024 * 1024):.0f} MB limit)"
                )

            file_name = Path(entry.filename).name
            if not file_name:
                logger.trace(_with_prefix(f"Skipping entry with empty path name: {entry.filename}"))
                continue

            # Handle filename collisions with UUID suffix
            destination = dest_folder / file_name
            if destination.exists():
                destination = dest_folder / f"{destination.stem}-{uuid_factory().hex}.xlsx"

            with archive.open(entry) as entry_stream, destination.open("wb") as output:
                output.write(entry_stream.read())

            extracted.append(
                ExtractedFile(
                    path=destination,
                    source_description=f"{zip_source}::{entry.filename}",
                )
            )

    logger.trace(_with_prefix(f"Extracted {len(extracted)} .xlsx file(s) from {zip_source}"))
    return extracted


def _with_prefix(message: str) -> str:
    """Add downloader prefix to log message."""
    return f"{DOWNLOADER_LOG_PREFIX}: {message}"


def _io_bytes(content: bytes) -> io.BytesIO:
    """Wrap bytes in BytesIO for zipfile compatibility."""
    return io.BytesIO(content)
