from __future__ import annotations

import shutil
import tempfile
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from src.core.config_provider import ProcessorConfig
from src.core.constants import (
    DOWNLOADER_LOG_PREFIX,
    LISTING_URLS,
)
from src.core.logging_utils import Logger
from src.ingest.html_parser import extract_links
from src.ingest.http_client import HttpClient, HttpError
from src.ingest.url_filter import (
    extract_file_name,
    filter_source_urls,
    try_extract_year_from_file_name,
)
from src.ingest.zip_extractor import extract_xlsx_from_zip


@dataclass(frozen=True)
class SourceFilesResult:
    source_files: list[tuple[Path, str]] | None
    temp_folder: Path | None


class SourceCatalog:
    def __init__(
        self,
        config: ProcessorConfig,
        http_client: HttpClient,
        logger: Logger,
        html_parser: Callable[[str, str, tuple[str, ...]], list[str]] | None = None,
        uuid_factory: Callable[[], uuid.UUID] | None = None,
    ) -> None:
        self._config = config
        self._http_client = http_client
        self._logger = logger
        self._html_parser = html_parser or extract_links
        self._uuid_factory = uuid_factory or uuid.uuid4

    def resolve_source_files(self, start_year: int, end_year: int) -> SourceFilesResult:
        """Resolve source files: local directory or download from USDA listing page."""
        local_result = self._load_local_sources()
        if local_result.source_files:
            return local_result
        if local_result.temp_folder is not None:
            cleanup_temp_folder(local_result.temp_folder)
        return self._download_remote_sources(start_year, end_year)

    def _load_local_sources(self) -> SourceFilesResult:
        local_xlsx_directory = self._config.xlsx_directory.strip()
        if not local_xlsx_directory:
            return SourceFilesResult([], None)

        directory = Path(local_xlsx_directory)
        if not directory.exists():
            return SourceFilesResult([], None)

        # R-5: Resolve base directory upfront for path traversal validation
        try:
            base_directory = directory.resolve(strict=True)
        except OSError:
            self._logger.error(_with_prefix(f"Cannot resolve local directory: {local_xlsx_directory}"))
            return SourceFilesResult([], None)

        source_files: list[tuple[Path, str]] = []
        local_zip_files: list[Path] = []
        skipped_count = 0

        for path in directory.rglob("*"):
            if not path.is_file():
                continue

            # R-5: Validate path is within base directory (reject symlinks escaping)
            if not _validate_path_within_base(path, base_directory):
                self._logger.trace(_with_prefix(f"Skipping file outside base directory: {path}"))
                skipped_count += 1
                continue

            if path.suffix.lower() == ".xlsx":
                if try_extract_year_from_file_name(path.name) is None:
                    raise ValueError(_with_prefix(f"Local XLSX filename missing year: {path.name}"))
                source_files.append((path, path.name))
                continue
            if path.suffix.lower() == ".zip":
                local_zip_files.append(path)

        if skipped_count > 0:
            self._logger.error(
                _with_prefix(f"Skipped {skipped_count} file(s) outside base directory (possible path traversal)")
            )

        temp_folder: Path | None = None
        if local_zip_files:
            temp_folder = self._create_temp_folder()
            for zip_path in local_zip_files:
                bytes_data = zip_path.read_bytes()
                extracted = extract_xlsx_from_zip(
                    bytes_data,
                    zip_path.name,
                    temp_folder,
                    self._logger,
                    self._uuid_factory,
                )
                source_files.extend((ef.path, ef.source_description) for ef in extracted)

        if source_files:
            self._logger.trace(_with_prefix(f"Using {len(source_files)} local .xlsx file(s) from {directory}"))

        return SourceFilesResult(source_files, temp_folder)

    def _download_remote_sources(self, start_year: int, end_year: int) -> SourceFilesResult:
        listing_result = _try_download_listing(self._http_client, self._logger, self._config)
        if listing_result.html is None:
            # R-3: Include tried URLs in error message for debugging
            urls_tried = ", ".join(listing_result.tried_urls) if listing_result.tried_urls else "(none)"
            self._logger.error(_with_prefix(f"Failed to download listing page. Tried: {urls_tried}"))
            return SourceFilesResult([], None)

        listing_html = listing_result.html
        listing_base_uri = listing_result.base_url
        xlsx_urls = self._extract_xlsx_urls(listing_html, listing_base_uri)
        zip_urls = self._extract_zip_urls(listing_html, listing_base_uri)

        if not xlsx_urls and not zip_urls:
            self._logger.error(_with_prefix(f"No .xlsx or .zip links found on {listing_base_uri}"))
            return SourceFilesResult([], None)

        self._logger.trace(
            _with_prefix(f"Found {len(xlsx_urls)} candidate .xlsx URL(s) and {len(zip_urls)} candidate .zip URL(s)")
        )

        filtered_xlsx_urls, filtered_zip_urls = filter_source_urls(
            xlsx_urls,
            zip_urls,
            start_year,
            end_year,
            self._config.max_xlsx_downloads,
        )

        self._logger.trace(
            _with_prefix(
                f"Selected {len(filtered_xlsx_urls)}/{len(xlsx_urls)} .xlsx URL(s) "
                f"and {len(filtered_zip_urls)}/{len(zip_urls)} archived .zip URL(s) for years {start_year}-{end_year}"
            )
        )

        if not filtered_xlsx_urls and not filtered_zip_urls:
            self._logger.trace(_with_prefix(f"No files found for requested years {start_year}-{end_year}"))
            return SourceFilesResult(None, None)

        self._logger.trace(
            _with_prefix(
                f"Downloading {len(filtered_xlsx_urls)} .xlsx file(s) and "
                f"{len(filtered_zip_urls)} archived .zip file(s) for requested years {start_year}-{end_year}"
            )
        )

        temp_folder = self._create_temp_folder()
        source_files: list[tuple[Path, str]] = []
        # R-2: Track failed downloads for summary logging
        failed_urls: list[str] = []

        for url in filtered_zip_urls:
            bytes_data = _download_bytes(self._http_client, self._logger, url)
            if bytes_data is None:
                failed_urls.append(url)
                continue
            extracted = extract_xlsx_from_zip(
                bytes_data,
                url,
                temp_folder,
                self._logger,
                self._uuid_factory,
            )
            source_files.extend((ef.path, ef.source_description) for ef in extracted)

        for url in filtered_xlsx_urls:
            file_name = extract_file_name(url) or f"usda-fruitveg-{self._uuid_factory().hex}.xlsx"
            bytes_data = _download_bytes(self._http_client, self._logger, url)
            if bytes_data is None:
                failed_urls.append(url)
                continue
            file_path = temp_folder / file_name
            file_path.write_bytes(bytes_data)
            source_files.append((file_path, url))

        # R-2: Log download failure summary
        if failed_urls:
            total_attempted = len(filtered_xlsx_urls) + len(filtered_zip_urls)
            self._logger.error(
                _with_prefix(
                    f"Download failures: {len(failed_urls)}/{total_attempted} file(s) failed. "
                    f"Failed URLs: {', '.join(failed_urls)}"
                )
            )

        return SourceFilesResult(source_files, temp_folder)

    def _extract_xlsx_urls(self, html_text: str, base_url: str) -> list[str]:
        return self._html_parser(html_text, base_url, (".xlsx",))

    def _extract_zip_urls(self, html_text: str, base_url: str) -> list[str]:
        return self._html_parser(html_text, base_url, (".zip",))

    def _create_temp_folder(self) -> Path:
        temp_root = Path(tempfile.gettempdir()) / "USDAFruitAndVegetables"
        temp_folder = temp_root / self._uuid_factory().hex
        temp_folder.mkdir(parents=True, exist_ok=True)
        return temp_folder


def cleanup_temp_folder(folder: Path) -> None:
    shutil.rmtree(folder, ignore_errors=True)


def _validate_path_within_base(path: Path, base_directory: Path) -> bool:
    """Validate that resolved path is within base directory.

    Per Constitution: Fail-fast - reject symlinks or paths that escape base directory.
    This protects against path traversal attacks where malicious symlinks or
    directory names could access files outside the intended directory.

    Args:
        path: Path to validate (may contain symlinks)
        base_directory: Expected parent directory (already resolved)

    Returns:
        True if path resolves to a location within base_directory, False otherwise
    """
    try:
        resolved = path.resolve(strict=True)
        return resolved.is_relative_to(base_directory)
    except (OSError, ValueError):
        # strict=True raises OSError if path doesn't exist
        # is_relative_to raises ValueError if not relative
        return False


def _with_prefix(message: str) -> str:
    return f"{DOWNLOADER_LOG_PREFIX}: {message}"


@dataclass(frozen=True)
class _ListingResult:
    """Result from listing page download attempt.

    Attributes:
        html: Listing page HTML content (None if all URLs failed)
        base_url: URL that succeeded (empty if all failed)
        tried_urls: List of URLs that were attempted
    """

    html: str | None
    base_url: str
    tried_urls: list[str]


def _try_download_listing(
    http_client: HttpClient,
    logger: Logger,
    config: ProcessorConfig,
) -> _ListingResult:
    """Try each listing URL until one succeeds with valid links.

    Per Constitution: Explicit over implicit - HttpError is caught and logged,
    allowing fallback to alternative URLs.

    R-3: Returns tried_urls for better error reporting when all URLs fail.
    """
    tried_urls: list[str] = []
    for url in _get_listing_urls(config):
        tried_urls.append(url)
        try:
            response = http_client.get_text(url)
            if not _has_listing_links(response):
                logger.trace(_with_prefix(f"No listing links found at {url}"))
                continue
            return _ListingResult(html=response, base_url=url, tried_urls=tried_urls)
        except HttpError as err:
            # Expected for some URLs (404 for missing resources, 402 for paywalls)
            logger.trace(_with_prefix(f"HTTP {err.status_code} at {url}"))
            continue
        except RuntimeError as err:
            # Network/transport errors
            logger.error(_with_prefix(f"Error downloading listing page {url}"), err)
    return _ListingResult(html=None, base_url="", tried_urls=tried_urls)


def _get_listing_urls(config: ProcessorConfig) -> Sequence[str]:
    override_url = config.listing_url.strip()
    if override_url:
        return [override_url]
    return list(LISTING_URLS)


def _has_listing_links(html_text: str) -> bool:
    lowered = html_text.lower()
    return ".xlsx" in lowered or ".zip" in lowered or ".csv" in lowered


def _download_bytes(http_client: HttpClient, logger: Logger, url: str) -> bytes | None:
    """Download file bytes. Returns None on HttpError (logged at caller).

    Per Constitution: Explicit over implicit - HttpError indicates specific failure,
    empty bytes indicates empty file (both valid states for caller to handle).
    """
    try:
        bytes_data = http_client.get_bytes(url)
        if not bytes_data:
            logger.error(_with_prefix(f"Empty response downloading {url}"))
            return None
        return bytes_data
    except HttpError as err:
        logger.error(_with_prefix(f"HTTP {err.status_code} downloading {url}: {err.reason}"))
        return None
