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

# Data Models and Core Logic Imports
from src.models.enums import Sport, Bookmaker
from src.scrapers.crabsports_scraper import CrabSportsScraper
from src.scrapers.base_scraper import ScraperError, AuthenticationError
from src.normalization.normalizer import Normalizer

# --- Supabase Client Function Imports ---
from src.storage.supabase_client import (
    initialize_supabase,
    save_normalized_data,
    # Removed direct import of the global supabase variable
)

# --- EV Calculator Imports ---
from src.calculation.ev_calculator import (
    fetch_relevant_data,
    calculate_ev,
    EVBet,  # Import EVBet if needed for type hinting or direct use
)

# Remove test function
# async def test_crabsports():
#    ...

# Restore placeholder Rich panels if desired, or remove
from rich import print
from rich.panel import Panel


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
    # Return the collected results
    return all_raw_results


async def main() -> None:
    """Main entry point for the application."""
    logger.info("Starting EV Betting Service - Live Scrape, Normalize, and Store")

    supabase_client = None  # Initialize client variable

    try:
        # --- Initialize Supabase Client ---
        supabase_client = await initialize_supabase()
        if not supabase_client:
            logger.critical("Failed to initialize Supabase client. Exiting.")
            # No need to close client here as it's None
            return

        # --- Run Scrape Cycle ---
        logger.info("Initiating live data scraping...")
        raw_data_for_norm = await run_scrape_cycle()

        if not raw_data_for_norm:
            logger.error("Scraping cycle returned no data. Exiting.")
            # No further processing needed
            return

        logger.info(
            f"Scraping complete. Received data for bookmakers: {list(raw_data_for_norm.keys())}"
        )

        # --- Normalization ---
        normalizer = Normalizer()
        normalized_games = []
        save_success = False

        normalized_games = normalizer.normalize(raw_data_for_norm)
        logger.success(f"Normalization completed. Found {len(normalized_games)} games.")

        # --- Save to Supabase ---
        if normalized_games:
            logger.info("Attempting to save normalized data to Supabase...")
            # save_normalized_data uses the internal client via get_supabase_client()
            save_success = await save_normalized_data(normalized_games)
            if save_success:
                logger.success("Data successfully saved to Supabase.")
            else:
                logger.error("Failed to save data to Supabase.")
        else:
            logger.warning("No normalized games to save to Supabase.")

        # --- Data Fetching for Inspection ---
        if save_success:
            logger.info("Fetching relevant game data from Supabase to save...")
            # Pass the initialized client explicitly
            relevant_games_data = await fetch_relevant_data(supabase_client)

            if relevant_games_data is not None:
                if relevant_games_data:
                    logger.success(
                        f"Successfully fetched {len(relevant_games_data)} games with market/odds data."
                    )

                    # --- Save fetched data to JSON ---
                    output_filename = "fetched_data.json"
                    try:
                        with open(output_filename, "w") as f:
                            json.dump(
                                relevant_games_data, f, indent=4
                            )  # Indent for readability
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
                        logger.warning(
                            "You might need to convert complex types (like datetime) to strings before saving."
                        )
                    # --- End Save to JSON ---

                    # --- Removed EV CALCULATION AND DISPLAY ---
                    # ...(removed/commented code remains removed/commented)...

                else:
                    logger.info(
                        "No relevant games found within the timeframe. No data saved."
                    )
            else:
                logger.error("Skipping data saving due to fetch error.")
        else:
            logger.warning("Skipping data saving (data save failed/skipped).")

        # --- Logging Normalized Data (Optional) ---
        if not normalized_games:
            logger.warning("No games were normalized.")
        else:
            # Loop and log details (existing code)
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
        logger.exception(
            "An error occurred during main execution loop (scrape, normalize, save, or EV calc)."
        )
    finally:
        # --- Close Supabase Client ---
        if supabase_client:
            # Reverting: Removing explicit close/aclose as both failed.
            # Assuming implicit cleanup by underlying httpx client on exit.
            logger.info("Supabase client cleanup will be handled implicitly on exit.")
            pass
            # try:
            #      await supabase_client.aclose()
            #      logger.success("Supabase async client closed.")
            # except AttributeError:
            #      logger.error(f"Failed to close Supabase client: Method 'aclose' not found on {type(supabase_client)}. Cleanup might be handled implicitly.")
            # except Exception as close_err:
            #      logger.error(f"Error closing Supabase client: {close_err}")
        else:
            logger.debug("Supabase client was not initialized, no closing needed.")


if __name__ == "__main__":
    # Logging is handled by setup_logging called above
    try:
        asyncio.run(main())  # Use asyncio.run for the async main
    except SystemExit as e:
        # logger should already be configured, but use print as fallback if critical exit before logger setup
        print(f"CRITICAL: Application exited prematurely during setup: {e}")
        logger.critical(f"Application exited prematurely: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception("An unexpected error occurred in main.")
        sys.exit(1)
