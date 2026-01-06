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

import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import requests

from src.ingest.http_client import HttpClient, HttpError
from src.ingest.rate_gate import RateGate
from tests.test_helpers import NullLogger


class HttpClientTests(unittest.TestCase):
    def _create_client(self) -> HttpClient:
        rate_gate = RateGate(max_requests=0, interval_seconds=0)  # bypass rate limiting
        return HttpClient(
            timeout_seconds=30,
            max_retries=1,
            rate_gate=rate_gate,
            logger=NullLogger(),
            api_key=None,
        )

    def _mock_response(self, status_code: int, text: str = "", content: bytes = b"") -> Any:
        response = MagicMock()
        response.status_code = status_code
        response.text = text
        response.content = content
        response.reason = "Test reason"
        response.raise_for_status = MagicMock()
        return response

    def test_get_text_returns_content_on_200(self) -> None:
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(200, text="Hello World")
            result = client.get_text("https://example.com")
            self.assertEqual(result, "Hello World")

    def test_get_text_raises_on_404(self) -> None:
        """Per Constitution: Fail-fast - 404 raises HttpError instead of silent empty."""
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(404)
            with self.assertRaises(HttpError) as context:
                client.get_text("https://example.com")
            self.assertEqual(context.exception.status_code, 404)
            self.assertTrue(context.exception.is_not_found())

    def test_get_text_raises_on_402(self) -> None:
        """Per Constitution: Fail-fast - 402 raises HttpError instead of silent empty."""
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(402)
            with self.assertRaises(HttpError) as context:
                client.get_text("https://example.com")
            self.assertEqual(context.exception.status_code, 402)
            self.assertTrue(context.exception.is_payment_required())

    def test_get_text_raises_on_429(self) -> None:
        """429 rate limit raises HttpError with is_rate_limited() helper."""
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(429)
            with self.assertRaises(HttpError) as context:
                client.get_text("https://example.com")
            self.assertEqual(context.exception.status_code, 429)
            self.assertTrue(context.exception.is_rate_limited())

    def test_get_bytes_returns_bytes_on_200(self) -> None:
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(200, content=b"\x00\x01\x02")
            result = client.get_bytes("https://example.com")
            self.assertEqual(result, b"\x00\x01\x02")

    def test_get_bytes_raises_on_404(self) -> None:
        """Per Constitution: Fail-fast - 404 raises HttpError instead of silent empty."""
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(404)
            with self.assertRaises(HttpError) as context:
                client.get_bytes("https://example.com")
            self.assertEqual(context.exception.status_code, 404)

    def test_get_bytes_raises_on_402(self) -> None:
        """Per Constitution: Fail-fast - 402 raises HttpError instead of silent empty."""
        client = self._create_client()
        with patch.object(client, "_request") as mock_request:
            mock_request.return_value = self._mock_response(402)
            with self.assertRaises(HttpError) as context:
                client.get_bytes("https://example.com")
            self.assertEqual(context.exception.status_code, 402)

    def test_get_text_raises_on_request_exception(self) -> None:
        client = self._create_client()
        with patch("requests.Session.get", side_effect=requests.RequestException("boom")):
            with self.assertRaises(RuntimeError):
                client.get_text("https://example.com")

    def test_get_text_raises_on_server_error(self) -> None:
        """Server errors (5xx) raise HttpError with status code."""
        client = self._create_client()
        response = self._mock_response(500)
        response.raise_for_status.side_effect = requests.RequestException("error")
        with patch.object(client, "_request", return_value=response):
            with self.assertRaises(HttpError) as context:
                client.get_text("https://example.com")
            self.assertEqual(context.exception.status_code, 500)

    def test_request_calls_rate_gate(self) -> None:
        rate_gate = MagicMock()
        client = HttpClient(
            timeout_seconds=30,
            max_retries=1,
            rate_gate=rate_gate,
            logger=NullLogger(),
            api_key=None,
        )
        with patch("requests.Session.get") as mock_get:
            mock_get.return_value = self._mock_response(200)
            client._request("https://example.com")  # pyright: ignore[reportPrivateUsage]
            rate_gate.wait_to_proceed.assert_called_once()

    def test_request_raises_runtime_error_on_timeout(self) -> None:
        """T-5: Timeout exceptions are wrapped in RuntimeError."""
        client = self._create_client()
        with patch("requests.Session.get", side_effect=requests.Timeout("timeout")):
            with self.assertRaises(RuntimeError) as context:
                client.get_text("https://example.com")
            # Verify the original Timeout is chained as __cause__
            self.assertIsInstance(context.exception.__cause__, requests.Timeout)

    def test_retry_config_includes_5xx_status_codes(self) -> None:
        """T-5: Verify retry configuration includes 5xx status codes."""
        rate_gate = RateGate(max_requests=0, interval_seconds=0)
        client = HttpClient(
            timeout_seconds=30,
            max_retries=3,
            rate_gate=rate_gate,
            logger=NullLogger(),
            api_key=None,
        )
        # Access the adapter to verify retry configuration
        adapter = client._session.get_adapter("https://")  # pyright: ignore[reportPrivateUsage]
        retry = adapter.max_retries
        # Verify 5xx codes are in status_forcelist
        self.assertIn(500, retry.status_forcelist)
        self.assertIn(502, retry.status_forcelist)
        self.assertIn(503, retry.status_forcelist)
        self.assertIn(504, retry.status_forcelist)
        self.assertIn(429, retry.status_forcelist)

    def test_api_key_header_injected_when_provided(self) -> None:
        """T-5: API key is sent as X-Api-Key header when provided."""
        rate_gate = RateGate(max_requests=0, interval_seconds=0)
        client = HttpClient(
            timeout_seconds=30,
            max_retries=1,
            rate_gate=rate_gate,
            logger=NullLogger(),
            api_key="test-api-key-12345",
        )
        # Verify the header is set on the session
        self.assertEqual(client._session.headers.get("X-Api-Key"), "test-api-key-12345")  # pyright: ignore[reportPrivateUsage]

    def test_api_key_header_not_set_when_none(self) -> None:
        """T-5: No X-Api-Key header when api_key is None."""
        client = self._create_client()  # api_key=None
        self.assertNotIn("X-Api-Key", client._session.headers)  # pyright: ignore[reportPrivateUsage]


if __name__ == "__main__":
    unittest.main()
