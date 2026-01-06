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

"""Tests for source catalog functionality (T-2).

Covers:
- SourceCatalog.resolve_source_files() entry point
- Local source loading with path traversal protection (R-5)
- Remote source downloading
- Helper functions for URL filtering and downloading
"""

from __future__ import annotations

import os
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

from src.core.config_provider import ProcessorConfig
from src.ingest.http_client import HttpError
from src.ingest.source_catalog import (
    SourceCatalog,
    SourceFilesResult,
    _validate_path_within_base,  # pyright: ignore[reportPrivateUsage]
    cleanup_temp_folder,
)
from tests.test_helpers import NullLogger, RecordingLogger


def _create_test_config(**overrides: Any) -> ProcessorConfig:
    """Create a ProcessorConfig with sensible test defaults."""
    defaults: dict[str, Any] = {
        "process_start_date": "20200101",
        "process_end_date": "20201231",
        "temp_output_directory": "/tmp/test-output",
        "xlsx_directory": "",
        "listing_url": "",
        "max_xlsx_downloads": 0,
        "normalize_cup_equivalent_unit": True,
        "http_timeout_seconds": 30,
        "max_retries": 1,
        "rate_limit_requests": 10,
        "rate_limit_seconds": 1.0,
    }
    defaults.update(overrides)
    return ProcessorConfig(**defaults)  # pyright: ignore[reportArgumentType]


class SourceFilesResultTests(unittest.TestCase):
    """Tests for SourceFilesResult dataclass."""

    def test_result_with_files(self) -> None:
        """SourceFilesResult holds source_files and temp_folder."""
        files = [(Path("/tmp/a.xlsx"), "url1")]
        folder = Path("/tmp/folder")

        result = SourceFilesResult(source_files=files, temp_folder=folder)

        self.assertEqual(result.source_files, files)
        self.assertEqual(result.temp_folder, folder)

    def test_result_with_none_files(self) -> None:
        """SourceFilesResult can have None source_files (no files found for date range)."""
        result = SourceFilesResult(source_files=None, temp_folder=None)

        self.assertIsNone(result.source_files)
        self.assertIsNone(result.temp_folder)

    def test_result_with_empty_list(self) -> None:
        """SourceFilesResult with empty list is different from None."""
        result = SourceFilesResult(source_files=[], temp_folder=None)

        self.assertEqual(result.source_files, [])
        self.assertIsNone(result.temp_folder)


class ValidatePathWithinBaseTests(unittest.TestCase):
    """Tests for _validate_path_within_base function (R-5 path traversal protection)."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.base_dir = Path(self.temp_dir.name)
        # Create some test files
        (self.base_dir / "valid.xlsx").write_bytes(b"test")
        (self.base_dir / "subdir").mkdir()
        (self.base_dir / "subdir" / "nested.xlsx").write_bytes(b"test")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_validates_file_in_base_directory(self) -> None:
        """File directly in base directory is valid."""
        path = self.base_dir / "valid.xlsx"
        result = _validate_path_within_base(path, self.base_dir)
        self.assertTrue(result)

    def test_validates_file_in_subdirectory(self) -> None:
        """File in subdirectory is valid."""
        path = self.base_dir / "subdir" / "nested.xlsx"
        result = _validate_path_within_base(path, self.base_dir)
        self.assertTrue(result)

    def test_rejects_path_outside_base(self) -> None:
        """Path outside base directory is rejected."""
        outside_path = Path("/tmp/outside.xlsx")
        result = _validate_path_within_base(outside_path, self.base_dir)
        self.assertFalse(result)

    def test_rejects_path_traversal_attempt(self) -> None:
        """Path with .. attempting to escape base is rejected."""
        traversal_path = self.base_dir / "subdir" / ".." / ".." / "etc" / "passwd"
        result = _validate_path_within_base(traversal_path, self.base_dir)
        self.assertFalse(result)

    def test_rejects_nonexistent_path(self) -> None:
        """Nonexistent path fails validation."""
        nonexistent = self.base_dir / "does_not_exist.xlsx"
        result = _validate_path_within_base(nonexistent, self.base_dir)
        self.assertFalse(result)

    @unittest.skipIf(os.name == "nt", "Symlinks require admin on Windows")
    def test_rejects_symlink_escaping_base(self) -> None:
        """Symlink pointing outside base directory is rejected."""
        # Create a symlink that points outside the base
        outside_dir = tempfile.mkdtemp()
        outside_file = Path(outside_dir) / "secret.xlsx"
        outside_file.write_bytes(b"secret")

        symlink_path = self.base_dir / "sneaky_link.xlsx"
        symlink_path.symlink_to(outside_file)

        try:
            result = _validate_path_within_base(symlink_path, self.base_dir)
            self.assertFalse(result)
        finally:
            import shutil

            shutil.rmtree(outside_dir)

    @unittest.skipIf(os.name == "nt", "Symlinks require admin on Windows")
    def test_allows_symlink_within_base(self) -> None:
        """Symlink pointing within base directory is allowed."""
        target = self.base_dir / "valid.xlsx"
        symlink_path = self.base_dir / "link_to_valid.xlsx"
        symlink_path.symlink_to(target)

        result = _validate_path_within_base(symlink_path, self.base_dir)
        self.assertTrue(result)


class CleanupTempFolderTests(unittest.TestCase):
    """Tests for cleanup_temp_folder function."""

    def test_removes_directory_and_contents(self) -> None:
        """cleanup_temp_folder removes directory and all contents."""
        temp_dir = tempfile.mkdtemp()
        test_file = Path(temp_dir) / "test.txt"
        test_file.write_text("content")

        self.assertTrue(Path(temp_dir).exists())
        cleanup_temp_folder(Path(temp_dir))
        self.assertFalse(Path(temp_dir).exists())

    def test_ignores_nonexistent_directory(self) -> None:
        """cleanup_temp_folder silently ignores nonexistent directory."""
        nonexistent = Path("/tmp/nonexistent_dir_12345")
        # Should not raise
        cleanup_temp_folder(nonexistent)


class SourceCatalogLocalSourcesTests(unittest.TestCase):
    """Tests for SourceCatalog._load_local_sources method."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir = Path(self.temp_dir.name)
        self.logger = NullLogger()
        self.http_client = MagicMock()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _create_catalog(self, xlsx_directory: str = "") -> SourceCatalog:
        """Create a SourceCatalog with the given xlsx_directory."""
        config = _create_test_config(xlsx_directory=xlsx_directory)
        return SourceCatalog(config, self.http_client, self.logger)

    def test_returns_empty_when_no_directory_configured(self) -> None:
        """Returns empty list when xlsx_directory is empty."""
        catalog = self._create_catalog(xlsx_directory="")

        result = catalog._load_local_sources()  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(result.source_files, [])
        self.assertIsNone(result.temp_folder)

    def test_returns_empty_when_directory_does_not_exist(self) -> None:
        """Returns empty list when xlsx_directory doesn't exist."""
        catalog = self._create_catalog(xlsx_directory="/nonexistent/path")

        result = catalog._load_local_sources()  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(result.source_files, [])
        self.assertIsNone(result.temp_folder)

    def test_loads_xlsx_files_from_directory(self) -> None:
        """Loads .xlsx files from local directory."""
        xlsx_file = self.local_dir / "data_2020.xlsx"
        xlsx_file.write_bytes(b"xlsx content")

        catalog = self._create_catalog(xlsx_directory=str(self.local_dir))

        result = catalog._load_local_sources()  # pyright: ignore[reportPrivateUsage]

        self.assertIsNotNone(result.source_files)
        assert result.source_files is not None  # Type narrowing for pyright
        self.assertEqual(len(result.source_files), 1)
        self.assertEqual(result.source_files[0][0], xlsx_file)
        self.assertEqual(result.source_files[0][1], "data_2020.xlsx")

    def test_raises_on_xlsx_without_year(self) -> None:
        """Raises ValueError for local XLSX file without year in filename."""
        xlsx_file = self.local_dir / "data.xlsx"  # No year
        xlsx_file.write_bytes(b"xlsx content")

        catalog = self._create_catalog(xlsx_directory=str(self.local_dir))

        with self.assertRaises(ValueError) as context:
            catalog._load_local_sources()  # pyright: ignore[reportPrivateUsage]
        self.assertIn("missing year", str(context.exception).lower())

    def test_loads_nested_xlsx_files(self) -> None:
        """Loads .xlsx files from nested subdirectories."""
        subdir = self.local_dir / "nested"
        subdir.mkdir()
        xlsx_file = subdir / "data_2020.xlsx"
        xlsx_file.write_bytes(b"xlsx content")

        catalog = self._create_catalog(xlsx_directory=str(self.local_dir))

        result = catalog._load_local_sources()  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(len(result.source_files or []), 1)

    def test_skips_non_xlsx_files(self) -> None:
        """Skips non-.xlsx files in directory."""
        (self.local_dir / "readme.txt").write_text("readme")
        (self.local_dir / "data_2020.xlsx").write_bytes(b"xlsx")

        catalog = self._create_catalog(xlsx_directory=str(self.local_dir))

        result = catalog._load_local_sources()  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(len(result.source_files or []), 1)


class SourceCatalogRemoteSourcesTests(unittest.TestCase):
    """Tests for SourceCatalog._download_remote_sources method."""

    def setUp(self) -> None:
        self.logger = RecordingLogger()
        self.http_client = MagicMock()

    def _create_catalog(
        self,
        listing_url: str = "",
        html_parser: Any = None,
    ) -> SourceCatalog:
        """Create a SourceCatalog for remote source testing."""
        config = _create_test_config(listing_url=listing_url)
        return SourceCatalog(
            config,
            self.http_client,
            self.logger,
            html_parser=html_parser,
            uuid_factory=lambda: uuid.UUID("12345678-1234-5678-1234-567812345678"),
        )

    def test_returns_empty_when_listing_fails(self) -> None:
        """Returns empty result when listing page download fails."""
        self.http_client.get_text.side_effect = RuntimeError("Network error")

        catalog = self._create_catalog()
        result = catalog._download_remote_sources(2020, 2020)  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(result.source_files, [])

    def test_returns_empty_when_no_links_found(self) -> None:
        """Returns empty result when listing has no xlsx/zip links."""
        self.http_client.get_text.return_value = "<html>No links here</html>"

        catalog = self._create_catalog()
        result = catalog._download_remote_sources(2020, 2020)  # pyright: ignore[reportPrivateUsage]

        self.assertEqual(result.source_files, [])

    def test_returns_none_when_no_files_match_year(self) -> None:
        """Returns None source_files when no files match requested year range."""
        self.http_client.get_text.return_value = "<html><a href='data_2015.xlsx'>2015</a></html>"

        # Parser returns URLs but they won't match 2020-2020
        def mock_parser(html: str, base_url: str, extensions: tuple[str, ...]) -> list[str]:
            if ".xlsx" in extensions:
                return ["http://example.com/data_2015.xlsx"]
            return []

        catalog = self._create_catalog(html_parser=mock_parser)
        result = catalog._download_remote_sources(2020, 2020)  # pyright: ignore[reportPrivateUsage]

        self.assertIsNone(result.source_files)

    def test_handles_http_error_during_download(self) -> None:
        """Continues gracefully when individual file download fails."""
        self.http_client.get_text.return_value = "<html><a href='data_2020.xlsx'>link</a></html>"
        self.http_client.get_bytes.side_effect = HttpError(404, "http://example.com", "Not Found")

        def mock_parser(html: str, base_url: str, extensions: tuple[str, ...]) -> list[str]:
            if ".xlsx" in extensions:
                return ["http://example.com/data_2020.xlsx"]
            return []

        catalog = self._create_catalog(html_parser=mock_parser)
        result = catalog._download_remote_sources(2020, 2020)  # pyright: ignore[reportPrivateUsage]

        # Should return empty list (file download failed), not None
        self.assertEqual(result.source_files, [])
        # Should have logged the error (errors are tuples of (message, exception))
        self.assertTrue(any("404" in msg for msg, _ in self.logger.errors))


class SourceCatalogResolveSourceFilesTests(unittest.TestCase):
    """Tests for SourceCatalog.resolve_source_files main entry point."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.local_dir = Path(self.temp_dir.name)
        self.logger = NullLogger()
        self.http_client = MagicMock()

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_prefers_local_sources_when_available(self) -> None:
        """Local sources take priority over remote download."""
        xlsx_file = self.local_dir / "data_2020.xlsx"
        xlsx_file.write_bytes(b"local xlsx")

        config = _create_test_config(xlsx_directory=str(self.local_dir))
        catalog = SourceCatalog(config, self.http_client, self.logger)

        result = catalog.resolve_source_files(2020, 2020)

        # Should use local file, never call http_client
        self.http_client.get_text.assert_not_called()
        self.assertEqual(len(result.source_files or []), 1)

    def test_falls_back_to_remote_when_local_empty(self) -> None:
        """Falls back to remote download when local directory is empty/missing."""
        self.http_client.get_text.return_value = "<html>no links</html>"

        config = _create_test_config(xlsx_directory="")
        catalog = SourceCatalog(config, self.http_client, self.logger)

        catalog.resolve_source_files(2020, 2020)

        # Should have attempted remote download
        self.http_client.get_text.assert_called()


if __name__ == "__main__":
    unittest.main()
