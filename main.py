import sys
import asyncio
import json
import os
from pathlib import Path
from typing import Dict, List, Any

# --- Restore Settings/Logging ---
from src.logging.setup import setup_logging
from src.config.settings import settings

setup_logging()

from loguru import logger

# --- End Restore ---

# Data Models and Core Logic Imports
from src.models.enums import Sport, Bookmaker
from src.scrapers.crabsports_scraper import CrabSportsScraper
from src.scrapers.pinnacle_scraper import PinnacleScraper
from src.scrapers.base_scraper import ScraperError, AuthenticationError
from src.normalization.normalizer import Normalizer

# --- Supabase Client Function Imports ---
from src.storage.supabase_client import (
    initialize_supabase,
    save_normalized_data,
)

# --- EV Calculator Imports ---
from src.calculation.ev_calculator import (
    fetch_relevant_data,
    calculate_ev,
    EVBet,
)

from rich import print
from rich.panel import Panel


# Restore the original run_scrape_cycle
async def run_scrape_cycle():
    """Runs a single scrape cycle for configured scrapers."""
    logger.info("Starting scrape cycle...")
    # --- Instantiate Scrapers ---
    scrapers = [CrabSportsScraper(), PinnacleScraper()]
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
            if scraper:
                await scraper.close()

    # Run scrapers concurrently
    await asyncio.gather(*(run_scraper(s) for s in scrapers))

    logger.info(
        f"Scrape cycle finished. Collected data keys: {list(all_raw_results.keys())}"
    )
    # Return the collected results
    return all_raw_results


async def main() -> None:
    """Main entry point for the application."""
    logger.info("Starting EV Betting Service - Live Scrape, Normalize, and Store")

    supabase_client = None
    try:
        supabase_client = await initialize_supabase()
        if not supabase_client:
            logger.critical("Failed to initialize Supabase client. Exiting.")
            return

        logger.info("Initiating live data scraping...")
        raw_data_for_norm = await run_scrape_cycle()

        if not raw_data_for_norm:
            logger.error("Scraping cycle returned no data. Exiting.")
            return

        logger.info(
            f"Scraping complete. Received data for bookmakers: {list(raw_data_for_norm.keys())}"
        )

        normalizer = Normalizer()
        normalized_games = []
        save_success = False

        normalized_games = normalizer.normalize(raw_data_for_norm)
        logger.success(f"Normalization completed. Found {len(normalized_games)} games.")

        if normalized_games:
            logger.info("Attempting to save normalized data to Supabase...")
            save_success = await save_normalized_data(normalized_games)
            if save_success:
                logger.success("Data successfully saved to Supabase.")
            else:
                logger.error("Failed to save data to Supabase.")
        else:
            logger.warning("No normalized games to save to Supabase.")

        if save_success:
            logger.info("Fetching relevant game data from Supabase to save...")
            relevant_games_data = await fetch_relevant_data(supabase_client)

            if relevant_games_data is not None:
                if relevant_games_data:
                    logger.success(
                        f"Successfully fetched {len(relevant_games_data)} games with market/odds data."
                    )

                    output_filename = "fetched_data.json"
                    try:
                        # Use context manager for file writing
                        with open(output_filename, "w", encoding="utf-8") as f:
                            json.dump(
                                relevant_games_data, f, indent=4, ensure_ascii=False
                            )
                        logger.success(
                            f"Successfully saved fetched data to {output_filename}"
                        )
                    except IOError as e:
                        logger.error(
                            f"Failed to write fetched data to {output_filename}: {e}"
                        )
                    except TypeError as e:
                        logger.error(
                            f"Data structure not JSON serializable when writing to {output_filename}: {e}"
                        )

                else:
                    logger.info(
                        "No relevant games found within the timeframe. No data saved."
                    )
            else:
                logger.error("Skipping data saving due to fetch error.")
        else:
            logger.warning("Skipping data saving (data save failed/skipped).")

        # --- Optional logging of normalized data (can be removed or reduced) ---
        if normalized_games:
            logger.info(
                f"--- Logging summary of first few normalized games ({min(3, len(normalized_games))}) ---"
            )
            for i, game in enumerate(normalized_games[:3]):  # Log only first 3
                logger.info(
                    f" Game {i+1}: {game.game_id} ({game.bookmaker.value}) - {len(game.markets)} markets"
                )
        else:
            logger.warning("No games were normalized.")

    except Exception as e:
        logger.exception("An error occurred during main execution loop.")
    finally:
        # --- Close Supabase Client implicitly (as before) ---
        if supabase_client:
            logger.info("Supabase client cleanup will be handled implicitly on exit.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Execution interrupted by user (KeyboardInterrupt).")
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Unhandled exception in main execution: {e}")
        sys.exit(1)
