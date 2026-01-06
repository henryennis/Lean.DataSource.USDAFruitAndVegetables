from __future__ import annotations

from collections.abc import Mapping
from types import TracebackType
from typing import Any

from CLRImports import Config  # type: ignore[reportAttributeAccessIssue,reportUnknownVariableType]

from src.core.logging_utils import Logger


class NullLogger(Logger):
    """Logger that discards all messages."""

    def trace(self, message: str) -> None:
        pass

    def error(self, message: str, exc: BaseException | None = None) -> None:
        pass


class RecordingLogger(Logger):
    """Logger that records messages for test assertions.

    Usage:
        logger = RecordingLogger()
        some_function(logger)
        assert "expected message" in logger.traces
        assert any("error" in msg for msg, _ in logger.errors)
    """

    def __init__(self) -> None:
        self.traces: list[str] = []
        self.errors: list[tuple[str, BaseException | None]] = []

    def trace(self, message: str) -> None:
        self.traces.append(message)

    def error(self, message: str, exc: BaseException | None = None) -> None:
        self.errors.append((message, exc))


class ConfigOverride:
    """Context manager for config overrides in tests.

    Uses QuantConnect's Config.Set() to set values and clears them on exit.

    Usage:
        with ConfigOverride({"process-start-date": "20200101", "process-end-date": "20200201"}):
            config = load_config()
            # config uses overridden values
        # config values are cleared automatically
    """

    def __init__(self, values: Mapping[str, Any]) -> None:
        self._values = values
        self._original_values: dict[str, str] = {}

    def __enter__(self) -> ConfigOverride:
        # Store original values and set new ones
        for key, value in self._values.items():
            self._original_values[key] = str(Config.Get(key))  # type: ignore[reportUnknownMemberType]
            Config.Set(key, "" if value is None else str(value))  # type: ignore[reportUnknownMemberType]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        # Restore original values (empty string effectively clears)
        for key, original in self._original_values.items():
            Config.Set(key, original)  # type: ignore[reportUnknownMemberType]
