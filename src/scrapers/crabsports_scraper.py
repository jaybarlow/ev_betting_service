import asyncio
import json  # Import json
from typing import Any, Dict, List, Optional
from datetime import datetime  # Import datetime for timestamp
import os  # Import os for path joining

from loguru import logger

from src.config.settings import settings  # Added back
from src.models.enums import Bookmaker, Sport
from .base_scraper import BaseScraper, ScraperError, AuthenticationError

# URL from user input
CRABSPORTS_API_URL = "https://ws.sportsbook.crabsports.com/component/data"

# Base Headers from user input (excluding Cookie)
BASE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:137.0) Gecko/20100101 Firefox/137.0",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br, zstd",
    "Referer": "https://sportsbook.crabsports.com/",
    "Content-type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "https://sportsbook.crabsports.com",
    "DNT": "1",
    "Sec-GPC": "1",
    "Connection": "keep-alive",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Priority": "u=4",
}

# Directory to save raw responses (relative to project root)
RAW_RESPONSE_DIR = "raw_responses"


class CrabSportsScraper(BaseScraper):
    """Scraper for fetching odds from Crab Sports."""

    bookmaker: Bookmaker = Bookmaker.CRAB_SPORTS

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        current_headers = BASE_HEADERS.copy()

        # --- Reverted to use settings ---
        if not settings.crabsports_cookie:
            logger.error(
                "Crab Sports cookie is not set in environment variables."
            )  # logger restored
            raise ScraperError("Missing Crab Sports cookie configuration.")
        else:
            current_headers["Cookie"] = settings.crabsports_cookie  # Use settings
            logger.info(
                f"Using Crab Sports cookie from settings (length: {len(settings.crabsports_cookie)})."
            )  # logger restored

        self.client.headers.update(current_headers)
        logger.debug(
            f"CrabSportsScraper initialized with headers containing Cookie."
        )  # logger restored

    async def fetch_odds(self, sports: List[Sport]) -> List[Dict[str, Any]]:
        """Fetch odds data and save raw responses to files."""
        all_raw_data = []
        logger.info(f"Fetching odds for {sports} from {self.bookmaker.value}")

        # Ensure the directory for saving responses exists
        os.makedirs(RAW_RESPONSE_DIR, exist_ok=True)

        for sport in sports:
            logger.debug(f"Processing sport: {sport.value}")  # logger restored
            url_key_fragment = self._map_sport_to_url_key(sport)
            if not url_key_fragment:
                continue

            payload_dict = {  # Renamed to avoid conflict
                "context": {
                    "url_key": f"/en_us/{url_key_fragment}",
                    "clientIp": "172.58.244.54",
                    "version": "1.0.1",
                    "device": "web_vuejs_desktop",
                    "lang": "en_us",
                    "timezone": "America/New_York",
                    "url_params": {},
                },
                "components": [
                    {"tree_compo_key": "header", "params": {}},
                    {"tree_compo_key": "menu_header", "params": {}},
                    {"tree_compo_key": "menu_quick_access", "params": {}},
                    {"tree_compo_key": "menu_top_league", "params": {}},
                    {"tree_compo_key": "menu_sport", "params": {}},
                    {
                        "tree_compo_key": "prematch_event_list",
                        "params": {},
                    },
                ],
            }
            logger.warning(
                "Using potentially static clientIp in payload for Crab Sports request."
            )  # logger restored
            logger.debug(
                f"Constructed payload dict for {sport.value}"
            )  # logger restored

            try:
                payload_string = json.dumps(payload_dict)
                payload_bytes = payload_string.encode("utf-8")
            except Exception as e:
                logger.exception(
                    f"Failed to serialize payload for {sport.value}: {e}"
                )  # logger restored
                continue

            try:
                response = await self._make_request(
                    method="POST",
                    url=CRABSPORTS_API_URL,
                    headers=self.client.headers,
                    content=payload_bytes,
                )
                raw_data = response.json()

                if raw_data:
                    logger.info(
                        f"Successfully fetched raw data for {sport.value} from {self.bookmaker.value}"
                    )  # logger restored
                    all_raw_data.append(raw_data)

                    # --- Save raw response to file ---
                    try:
                        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                        filename = os.path.join(
                            RAW_RESPONSE_DIR,
                            f"crabsports_{sport.value}_{timestamp}.json",
                        )
                        with open(filename, "w", encoding="utf-8") as f:
                            json.dump(raw_data, f, indent=2, ensure_ascii=False)
                        logger.info(
                            f"Saved raw response for {sport.value} to {filename}"
                        )
                    except Exception as e:
                        logger.exception(
                            f"Failed to save raw response for {sport.value} to file: {e}"
                        )
                    # --- End save response ---

                else:
                    logger.warning(
                        f"No data returned in response body for {sport.value} from {self.bookmaker.value}"
                    )  # logger restored

            except AuthenticationError as e:
                logger.error(
                    f"Authentication failed for {self.bookmaker.value}. Cookie might be expired: {e}"
                )  # logger restored
                raise
            except ScraperError as e:
                logger.error(
                    f"Failed to fetch odds for {sport.value} from {self.bookmaker.value}: {e}"
                )  # logger restored
                continue
            except Exception as e:
                logger.exception(
                    f"Unexpected error fetching odds for {sport.value} from {self.bookmaker.value}: {e}"
                )  # logger restored
                continue

        logger.info(
            f"Finished fetching from {self.bookmaker.value}. Returning {len(all_raw_data)} raw data sets."
        )  # logger restored
        return all_raw_data

    def _map_sport_to_url_key(self, sport: Sport) -> Optional[str]:
        """Maps Sport enum to the url_key fragment used by Crab Sports API."""
        mapping = {
            Sport.NBA: "basketball/united-states/nba",
            Sport.NHL: "hockey/united-states/nhl",
            Sport.MLB: "baseball/united-states/mlb",
        }
        if sport not in mapping:
            logger.warning(
                f"Unsupported sport for Crab Sports URL key mapping: {sport.value}"
            )  # logger restored
            return None
        logger.debug(
            f"Mapped {sport.value} to URL key fragment: {mapping[sport]}"
        )  # logger restored
        return mapping[sport]

    # Removing dummy data method as we are making real requests now
    # def _get_dummy_data(self, sport: Sport) -> Optional[List[Dict[str, Any]]]:
    #     ...
