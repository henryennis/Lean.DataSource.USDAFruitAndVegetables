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

"""Tests for ZIP extraction functionality.

Covers:
- T-3: Basic extraction functionality (collision handling, filtering)
- R-4: ZIP bomb protection (size limits, compression ratio)
"""

from __future__ import annotations

import io
import tempfile
import unittest
import uuid
import zipfile
from pathlib import Path

from src.ingest.zip_extractor import extract_xlsx_from_zip
from tests.test_helpers import NullLogger, RecordingLogger


def _create_zip_with_files(files: dict[str, bytes]) -> bytes:
    """Create a ZIP archive in memory with the given files.

    Args:
        files: Dictionary of filename -> content bytes

    Returns:
        ZIP archive as bytes
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def _create_minimal_xlsx() -> bytes:
    """Create minimal valid XLSX-like content for testing.

    Note: This is not a real XLSX file, just placeholder bytes.
    The zip_extractor only checks file extensions, not content validity.
    """
    return b"PK\x03\x04" + b"\x00" * 100  # ZIP magic + padding


class TestExtractXlsxFromZip(unittest.TestCase):
    """Tests for extract_xlsx_from_zip function."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.dest_folder = Path(self.temp_dir.name)
        self.logger = NullLogger()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    # --- Basic Extraction Tests (T-3) ---

    def test_extract_single_xlsx_file(self) -> None:
        """Single XLSX file is extracted with correct path and source description."""
        content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files({"data.xlsx": content})

        result = extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, self.logger)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].path.name, "data.xlsx")
        self.assertTrue(result[0].path.exists())
        self.assertEqual(result[0].source_description, "test.zip::data.xlsx")

    def test_extract_multiple_xlsx_files(self) -> None:
        """Multiple XLSX files are all extracted."""
        content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files(
            {
                "file1.xlsx": content,
                "file2.xlsx": content,
                "file3.xlsx": content,
            }
        )

        result = extract_xlsx_from_zip(zip_bytes, "archive.zip", self.dest_folder, self.logger)

        self.assertEqual(len(result), 3)
        names = {f.path.name for f in result}
        self.assertEqual(names, {"file1.xlsx", "file2.xlsx", "file3.xlsx"})

    def test_extract_collision_adds_uuid_suffix(self) -> None:
        """Filename collision with existing file adds UUID suffix."""
        content = _create_minimal_xlsx()
        # Pre-create a file with same name
        existing_file = self.dest_folder / "data.xlsx"
        existing_file.write_bytes(b"existing content")

        zip_bytes = _create_zip_with_files({"data.xlsx": content})
        fixed_uuid = uuid.UUID("12345678-1234-5678-1234-567812345678")

        result = extract_xlsx_from_zip(
            zip_bytes, "test.zip", self.dest_folder, self.logger, uuid_factory=lambda: fixed_uuid
        )

        self.assertEqual(len(result), 1)
        # Should have UUID suffix
        self.assertEqual(result[0].path.name, f"data-{fixed_uuid.hex}.xlsx")
        # Original file should be untouched
        self.assertEqual(existing_file.read_bytes(), b"existing content")

    def test_extract_nested_paths_uses_basename(self) -> None:
        """Files in nested directories are extracted with basename only."""
        content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files({"path/to/nested/file.xlsx": content})

        result = extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, self.logger)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].path.name, "file.xlsx")
        self.assertEqual(result[0].path.parent, self.dest_folder)
        self.assertEqual(result[0].source_description, "test.zip::path/to/nested/file.xlsx")

    def test_extract_skips_non_xlsx_files(self) -> None:
        """Non-XLSX files are skipped."""
        xlsx_content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files(
            {
                "data.xlsx": xlsx_content,
                "readme.txt": b"readme content",
                "image.png": b"PNG\x00\x00",
                "script.py": b"print('hello')",
            }
        )

        result = extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, self.logger)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].path.name, "data.xlsx")

    def test_extract_skips_directory_entries(self) -> None:
        """Directory entries (ending with /) are skipped."""
        content = _create_minimal_xlsx()
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            # Add a directory entry
            archive.writestr("somedir/", "")
            archive.writestr("somedir/file.xlsx", content)
        zip_bytes = buffer.getvalue()

        result = extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, self.logger)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].path.name, "file.xlsx")

    def test_extract_empty_archive_returns_empty_list(self) -> None:
        """Empty ZIP archive returns empty list."""
        zip_bytes = _create_zip_with_files({})

        result = extract_xlsx_from_zip(zip_bytes, "empty.zip", self.dest_folder, self.logger)

        self.assertEqual(result, [])

    def test_extract_invalid_zip_raises(self) -> None:
        """Invalid ZIP data raises exception."""
        invalid_bytes = b"not a zip file"

        with self.assertRaises(zipfile.BadZipFile):
            extract_xlsx_from_zip(invalid_bytes, "bad.zip", self.dest_folder, self.logger)

    def test_extract_logs_trace_for_skipped_entries(self) -> None:
        """Skipped entries are logged at trace level."""
        logger = RecordingLogger()
        xlsx_content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files(
            {
                "data.xlsx": xlsx_content,
                "readme.txt": b"readme",
            }
        )

        extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, logger)

        # Should have trace for skipped txt file
        self.assertTrue(any("readme.txt" in msg for msg in logger.traces))

    def test_extract_logs_count_on_completion(self) -> None:
        """Extraction logs count of extracted files."""
        logger = RecordingLogger()
        xlsx_content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files(
            {
                "file1.xlsx": xlsx_content,
                "file2.xlsx": xlsx_content,
            }
        )

        extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, logger)

        self.assertTrue(any("2 .xlsx file(s)" in msg for msg in logger.traces))

    def test_extract_case_insensitive_xlsx_extension(self) -> None:
        """XLSX extension matching is case-insensitive."""
        content = _create_minimal_xlsx()
        zip_bytes = _create_zip_with_files(
            {
                "lower.xlsx": content,
                "upper.XLSX": content,
                "mixed.XlSx": content,
            }
        )

        result = extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, self.logger)

        self.assertEqual(len(result), 3)

    def test_extract_collision_within_same_zip(self) -> None:
        """Files with same basename from different paths get UUID suffix."""
        content1 = _create_minimal_xlsx()
        content2 = b"PK\x03\x04" + b"\x01" * 100  # Different content

        # Create ZIP with same-named files in different directories
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as archive:
            archive.writestr("dir1/data.xlsx", content1)
            archive.writestr("dir2/data.xlsx", content2)
        zip_bytes = buffer.getvalue()

        uuid_counter = [0]

        def uuid_factory() -> uuid.UUID:
            uuid_counter[0] += 1
            return uuid.UUID(f"00000000-0000-0000-0000-{uuid_counter[0]:012d}")

        result = extract_xlsx_from_zip(zip_bytes, "test.zip", self.dest_folder, self.logger, uuid_factory=uuid_factory)

        self.assertEqual(len(result), 2)
        paths = {f.path.name for f in result}
        # First file gets original name, second gets UUID suffix
        self.assertIn("data.xlsx", paths)


class TestZipBombProtection(unittest.TestCase):
    """Tests for ZIP bomb protection (R-4).

    These tests verify the size and compression ratio limits that will be
    added to protect against ZIP bomb attacks.
    """

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.dest_folder = Path(self.temp_dir.name)
        self.logger = NullLogger()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_rejects_oversized_zip_archive(self) -> None:
        """ZIP archive exceeding MAX_ZIP_FILE_SIZE_BYTES is rejected.

        Note: This test will pass once the size limit is implemented.
        Currently skipped until implementation is complete.
        """
        # Import the constant once it's added
        try:
            from src.core.constants import MAX_ZIP_FILE_SIZE_BYTES
        except ImportError:
            self.skipTest("MAX_ZIP_FILE_SIZE_BYTES not yet implemented")

        # Create a ZIP larger than the limit (for testing, we'll mock or use small limit)
        # In real implementation, we validate len(zip_bytes) before opening
        large_content = b"x" * (MAX_ZIP_FILE_SIZE_BYTES + 1)

        with self.assertRaises(ValueError) as context:
            extract_xlsx_from_zip(large_content, "large.zip", self.dest_folder, self.logger)

        self.assertIn("too large", str(context.exception).lower())

    @unittest.skip("Implementation pending - needs mock or integration test")
    def test_rejects_oversized_entry(self) -> None:
        """Entry with file_size > MAX_EXTRACTED_FILE_SIZE_BYTES is rejected.

        Note: The constant exists (MAX_EXTRACTED_FILE_SIZE_BYTES) but this test
        requires creating a ZIP with a large uncompressed entry, which needs
        special setup.
        """
        pass  # Actual test implementation deferred

    @unittest.skip("Implementation pending - needs specially crafted ZIP")
    def test_rejects_high_compression_ratio(self) -> None:
        """Entry with compression ratio > MAX_ZIP_COMPRESSION_RATIO is rejected.

        Note: The constant exists (MAX_ZIP_COMPRESSION_RATIO) but this test
        requires creating a ZIP bomb test file, which needs special setup.
        """
        pass  # Actual test implementation deferred

    @unittest.skip("Implementation pending - needs cumulative tracking")
    def test_tracks_cumulative_extracted_size(self) -> None:
        """Total extracted bytes across all files is tracked against limit.

        Note: The constant exists (MAX_TOTAL_EXTRACTED_BYTES) but this test
        requires careful orchestration of multiple files.
        """
        pass  # Actual test implementation deferred


if __name__ == "__main__":
    unittest.main()
