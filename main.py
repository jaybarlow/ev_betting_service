import sys
import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Any

# import os # No longer needed

# --- Restore Settings/Logging ---
from src.logging.setup import setup_logging
from src.config.settings import settings

setup_logging()

from loguru import logger

# --- End Restore ---

# Remove test-specific imports if not needed elsewhere
from src.models.enums import Sport, Bookmaker
from src.scrapers.crabsports_scraper import CrabSportsScraper
from src.scrapers.base_scraper import ScraperError, AuthenticationError
from src.normalization.normalizer import Normalizer

# Remove test function
# async def test_crabsports():
#    ...

# Restore placeholder Rich panels if desired, or remove
from rich import print, panel


async def run_scrape_cycle():
    """Runs a single scrape cycle for configured scrapers."""
    logger.info("Starting scrape cycle...")
    # --- Instantiate Scrapers ---
    # TODO: Add other scrapers (Pinnacle, TBD) here
    scrapers = [CrabSportsScraper()]  # Add more scrapers to this list
    target_sports = [Sport.NBA, Sport.MLB, Sport.NHL]  # Define sports to scrape

    all_raw_results = {}

    async def run_scraper(scraper):
        try:
            logger.info(f"Running scraper: {scraper.bookmaker.value}")
            # Fetch odds for all target sports for this scraper
            raw_data_list = await scraper.fetch_odds(target_sports)
            all_raw_results[scraper.bookmaker] = raw_data_list
            logger.info(f"Successfully completed scrape for {scraper.bookmaker.value}")
        except AuthenticationError as e:
            logger.critical(
                f"{scraper.bookmaker.value} Authentication Error: {e} - Check credentials/cookie!"
            )
            # Optionally stop the whole cycle on auth error, or just skip this book
        except ScraperError as e:
            logger.error(f"{scraper.bookmaker.value} Scraper Error during fetch: {e}")
        except Exception as e:
            logger.exception(
                f"Unexpected error running scraper {scraper.bookmaker.value}: {e}"
            )
        finally:
            await scraper.close()

    # Run scrapers concurrently
    await asyncio.gather(*(run_scraper(s) for s in scrapers))

    logger.info(
        f"Scrape cycle finished. Collected data keys: {list(all_raw_results.keys())}"
    )
    # TODO: Pass all_raw_results to the normalization step
    # normalizer = Normalizer()
    # normalized_data = normalizer.normalize(all_raw_results)
    # ... etc ...


def main() -> None:
    """Main entry point for the application."""
    logger.info("Starting EV Betting Service - Normalization Test")

    # Define path to the raw response file
    raw_data_dir = Path("raw_responses")
    # Use the specific Crab Sports MLB file
    file_path = raw_data_dir / "crabsports_MLB_20250428_134329.json"

    if not file_path.exists():
        logger.error(f"Raw response file not found: {file_path}")
        return

    # Load the raw data
    try:
        with open(file_path, "r") as f:
            raw_response_data = json.load(f)
        logger.info(f"Successfully loaded raw data from {file_path}")
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON from {file_path}")
        return
    except Exception as e:
        logger.error(f"Error reading file {file_path}: {e}")
        return

    # Prepare data for the normalizer
    # The normalizer expects Dict[Bookmaker, List[Dict[str, Any]]]
    # Since Crab Sports scraper returns one response per sport,
    # and we loaded one file directly, we wrap it in a list.
    raw_data_for_norm: Dict[Bookmaker, List[Dict[str, Any]]] = {
        Bookmaker.CRAB_SPORTS: [raw_response_data]
    }

    # Instantiate the normalizer
    normalizer = Normalizer()

    # Normalize the data
    try:
        normalized_games = normalizer.normalize(raw_data_for_norm)
        logger.success(f"Normalization completed. Found {len(normalized_games)} games.")

        # Print details of all games for verification
        if not normalized_games:
            logger.warning("No games were normalized.")
        else:
            for i, game in enumerate(normalized_games):
                logger.info(f"--- Normalized Game {i + 1}/{len(normalized_games)} --- ")
                logger.info(f"ID: {game.game_id}")
                logger.info(f"Bookmaker: {game.bookmaker.value}")
                logger.info(f"Sport: {game.sport.value}")
                logger.info(f"League: {game.league.value}")
                logger.info(f"Start Time (UTC): {game.start_time_utc}")
                logger.info(
                    f"Home Team: {game.home_team.raw_name} (ID: {game.home_team.team_id}, Canonical: {game.home_team.canonical_name})"
                )
                logger.info(
                    f"Away Team: {game.away_team.raw_name} (ID: {game.away_team.team_id}, Canonical: {game.away_team.canonical_name})"
                )
                logger.info(f"Raw Event ID: {game.raw_event_id}")
                logger.info(f"Number of Markets: {len(game.markets)}")

                if game.markets:
                    # Log details for the first market of this game
                    # If you need all markets, add another loop here
                    market = game.markets[0]
                    logger.info("  --- First Market --- ")  # Indicate it's the first
                    logger.info(f"  Market ID: {market.market_id}")
                    logger.info(f"  Market Type: {market.market_type.value}")
                    logger.info(f"  Market Line: {market.line}")
                    logger.info(f"  Market Period: {market.period.value}")
                    logger.info(f"  Raw Market Name: {market.raw_market_name}")
                    logger.info(f"  Number of Odds: {len(market.odds)}")

                    if market.odds:
                        # Log details for the first odds of this market
                        # If you need all odds, add another loop here
                        odds_item = market.odds[0]
                        logger.info(
                            "    --- First Odds --- "
                        )  # Indicate it's the first
                        logger.info(
                            f"    Market ID: {odds_item.market_id}"
                        )  # Should match parent market ID
                        logger.info(f"    Bookmaker: {odds_item.bookmaker.value}")
                        logger.info(f"    Decimal Price: {odds_item.decimal_odds}")
                        logger.info(f"    Timestamp: {odds_item.timestamp_collected}")
                logger.info(
                    f"--- End Normalized Game {i + 1}/{len(normalized_games)} --- "
                )

    except Exception as e:
        logger.exception("An error occurred during normalization.")


if __name__ == "__main__":
    # Logging is handled by setup_logging called above
    try:
        main()
    except SystemExit as e:
        # logger should already be configured, but use print as fallback if critical exit before logger setup
        print(f"CRITICAL: Application exited prematurely during setup: {e}")
        logger.critical(f"Application exited prematurely: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception("An unexpected error occurred in main.")
        sys.exit(1)
