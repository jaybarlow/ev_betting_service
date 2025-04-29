# src/scrapers/pinnacle_scraper.py

import asyncio
from typing import Any, Dict, List, Optional

from loguru import logger

from src.config.settings import settings
from src.models.enums import Bookmaker, Sport
from .base_scraper import BaseScraper, ScraperError, AuthenticationError

# Pinnacle API endpoint
PINNACLE_API_BASE_URL = "https://guest.api.arcadia.pinnacle.com/0.1"

# Mapping from our Sport enum to Pinnacle API league IDs
# From user code: NBA: 487, MLB: 246, NHL: 1456
# Add others as needed (NFL: 889, NCAAF: 880, WNBA: 578, MLS: 2663)
SPORT_TO_LEAGUE_ID = {
    Sport.NBA: 487,
    Sport.MLB: 246,
    Sport.NHL: 1456,
    Sport.NFL: 889,
    Sport.NCAAF: 880,
    Sport.WNBA: 578,
    Sport.MLS: 2663,
}


class PinnacleScraper(BaseScraper):
    """Scraper for fetching odds from Pinnacle."""

    bookmaker: Bookmaker = Bookmaker.PINNACLE

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not settings.PINNACLE_API_KEY:
            logger.error("Pinnacle API Key is not set in environment variables.")
            raise ScraperError("Missing Pinnacle API Key configuration.")

        # Set the API key header for all requests made by this scraper instance
        self.client.headers.update({"X-API-Key": settings.PINNACLE_API_KEY})
        logger.info("PinnacleScraper initialized with API Key header.")

    async def fetch_odds(self, sports: List[Sport]) -> List[Dict[str, Any]]:
        """Fetch raw odds data for specified sports from Pinnacle API."""
        all_raw_data = []
        logger.info(f"Fetching odds for {sports} from {self.bookmaker.value}")

        target_league_ids = [
            league_id
            for sport in sports
            if (league_id := SPORT_TO_LEAGUE_ID.get(sport))
        ]

        if not target_league_ids:
            logger.warning(f"No Pinnacle league IDs found for target sports: {sports}")
            return []

        game_tasks = []
        market_tasks = []

        for league_id in target_league_ids:
            matchups_url = f"{PINNACLE_API_BASE_URL}/leagues/{league_id}/matchups"
            markets_url = (
                f"{PINNACLE_API_BASE_URL}/leagues/{league_id}/markets/straight"
            )

            # Use self._make_request which handles errors and uses the client's base headers
            game_tasks.append(self._make_request(method="GET", url=matchups_url))
            market_tasks.append(self._make_request(method="GET", url=markets_url))

        try:
            game_responses = await asyncio.gather(*game_tasks, return_exceptions=True)
            market_responses = await asyncio.gather(
                *market_tasks, return_exceptions=True
            )

            # Process successful responses, log errors
            for i, league_id in enumerate(target_league_ids):
                game_resp = game_responses[i]
                market_resp = market_responses[i]

                league_raw_data = {}
                has_error = False

                if isinstance(game_resp, Exception):
                    logger.error(
                        f"Error fetching Pinnacle matchups for league {league_id}: {game_resp}"
                    )
                    has_error = True
                else:
                    try:
                        league_raw_data["matchups"] = game_resp.json()
                        logger.info(
                            f"Successfully fetched Pinnacle matchups for league {league_id}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error parsing Pinnacle matchups JSON for league {league_id}: {e}"
                        )
                        logger.debug(
                            f"Raw matchup response content: {getattr(game_resp, 'text', 'N/A')}"
                        )
                        has_error = True

                if isinstance(market_resp, Exception):
                    logger.error(
                        f"Error fetching Pinnacle markets for league {league_id}: {market_resp}"
                    )
                    has_error = True
                else:
                    try:
                        league_raw_data["markets"] = market_resp.json()
                        logger.info(
                            f"Successfully fetched Pinnacle markets for league {league_id}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error parsing Pinnacle markets JSON for league {league_id}: {e}"
                        )
                        logger.debug(
                            f"Raw market response content: {getattr(market_resp, 'text', 'N/A')}"
                        )
                        has_error = True

                # Only add if we got both matchups and markets successfully
                if (
                    not has_error
                    and "matchups" in league_raw_data
                    and "markets" in league_raw_data
                ):
                    # Add league ID for context during normalization
                    league_raw_data["league_id"] = league_id
                    all_raw_data.append(league_raw_data)
                else:
                    logger.warning(
                        f"Skipping data for league {league_id} due to fetch/parse errors."
                    )

        except Exception as e:
            # Catch potential errors in asyncio.gather itself
            logger.exception(
                f"Unexpected error during concurrent Pinnacle API calls: {e}"
            )
            # Depending on severity, you might want to raise ScraperError here

        logger.info(
            f"Finished fetching from {self.bookmaker.value}. Returning data for {len(all_raw_data)} leagues."
        )
        return all_raw_data  # List of dicts, each dict contains 'matchups' and 'markets' for a league_id

    # Note: The parsing functions from the original code (parse_market_values, etc.)
    # will need to be adapted and moved into the Normalizer's _normalize_pinnacle_data method.
    # The `american_to_decimal` function will also be needed there.
