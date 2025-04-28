import asyncio
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import httpx
from loguru import logger
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    RetryError,
)

from src.config.settings import settings
from src.models.enums import Bookmaker, Sport

# Define common HTTP status codes that warrant a retry
RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}


class ScraperError(Exception):
    """Custom exception for scraper-related errors."""

    pass


class AuthenticationError(ScraperError):
    """Exception raised for authentication failures (401, 403)."""

    pass


class RateLimitError(ScraperError):
    """Exception raised for rate limit errors (429)."""

    pass


class BaseScraper(ABC):
    """Abstract base class for sportsbook scrapers."""

    bookmaker: Bookmaker = Bookmaker.UNKNOWN

    def __init__(self, client: Optional[httpx.AsyncClient] = None):
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),  # Set a reasonable timeout
            follow_redirects=True,
            # Consider adding user-agent randomization here
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            },
        )

    @abstractmethod
    async def fetch_odds(self, sports: List[Sport]) -> List[Dict[str, Any]]:
        """Fetch odds data for the specified sports.

        Args:
            sports: A list of Sport enums to fetch data for.

        Returns:
            A list of dictionaries, where each dictionary represents raw event/market data
            from the specific sportsbook's API.
        """
        pass

    @retry(
        stop=stop_after_attempt(4),  # PRD: Max 3 retries (4 total attempts)
        wait=wait_exponential(
            multiplier=1, min=1, max=10
        ),  # PRD: Exponential backoff (1s, 2s, 4s... capped)
        retry=retry_if_exception_type(
            (httpx.RequestError, httpx.HTTPStatusError, RateLimitError)
        ),
        reraise=True,  # Reraise the exception after max attempts
    )
    async def _make_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        content: Optional[bytes] = None,
        **kwargs,
    ) -> httpx.Response:
        """Makes an asynchronous HTTP request with retry logic."""
        log_context = {
            "method": method,
            "url": url,
            "params": params,
            "has_json": json_data is not None,
            "has_data": data is not None,
            "has_content": content is not None,
        }
        logger.debug(f"Making request", **log_context)
        try:
            response = await self.client.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_data,
                data=data,
                content=content,
                **kwargs,
            )

            # Handle specific HTTP errors after potential retries
            if response.status_code in {401, 403}:
                logger.warning(
                    f"Authentication error ({response.status_code}) for {self.bookmaker} at {url}. Check credentials/cookies."
                )
                # Don't retry auth errors further, raise specific exception
                raise AuthenticationError(
                    f"Authentication failed ({response.status_code}) for {self.bookmaker}"
                )

            if response.status_code == 429:
                # Log rate limit and raise specific exception to potentially trigger retry
                retry_after = response.headers.get("Retry-After")
                logger.warning(
                    f"Rate limit hit (429) for {self.bookmaker} at {url}. Retry-After: {retry_after}"
                )
                raise RateLimitError(f"Rate limited by {self.bookmaker}")

            # Raise general HTTPStatusError for other client/server errors (if not handled by retry)
            response.raise_for_status()  # Raises HTTPStatusError for 4xx/5xx
            logger.debug(f"Request successful: {response.status_code} for {url}")
            return response

        except httpx.HTTPStatusError as e:
            # Check if the status code is one we specifically retry on
            if e.response.status_code in RETRYABLE_STATUS_CODES:
                logger.warning(
                    f"Retrying request for {self.bookmaker} due to status {e.response.status_code}: {e}"
                )
                raise  # Re-raise to trigger tenacity retry
            else:
                # For non-retryable HTTP errors, log and wrap in ScraperError
                logger.error(
                    f"HTTP error during request for {self.bookmaker}: {e.response.status_code} - {e}"
                )
                raise ScraperError(f"HTTP error: {e.response.status_code}") from e
        except httpx.RequestError as e:
            # Network errors, timeouts etc. - these are retryable by default
            logger.warning(f"Request error for {self.bookmaker}, retrying: {e}")
            raise  # Re-raise to trigger tenacity retry
        except RetryError as e:
            # This catches the error after all retries have failed
            logger.error(
                f"Max retries exceeded for {self.bookmaker} request to {url}. Last exception: {e.cause}"
            )
            # Raise the specific final error (Auth, RateLimit) or a general ScraperError
            if isinstance(e.cause, (AuthenticationError, RateLimitError)):
                raise e.cause
            raise ScraperError(
                f"Failed request to {self.bookmaker} after multiple retries"
            ) from e.cause
        except Exception as e:
            logger.exception(
                f"Unexpected error during request for {self.bookmaker}: {e}"
            )
            raise ScraperError("Unexpected error during HTTP request") from e

    async def close(self):
        """Closes the underlying HTTP client."""
        await self.client.aclose()
        logger.info(f"Closed HTTP client for {self.bookmaker}")
