from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from loguru import logger
from supabase import AsyncClient
from postgrest import APIResponse
from postgrest.exceptions import APIError

# Placeholder for data structure if needed, or we can use Dict[str, Any]
# from src.models.game import Game # etc. if we want full Pydantic models


async def fetch_relevant_data(
    supabase_client: AsyncClient, hours_ahead: int = 24
) -> Optional[List[Dict[str, Any]]]:
    """
    Fetches games starting within the specified timeframe, along with their
    associated markets and odds, from Supabase.

    Args:
        supabase_client: An initialized async Supabase client instance.
        hours_ahead: The number of hours into the future to fetch games for.

    Returns:
        A list of dictionaries representing games and their nested data,
        or None if an error occurs or the client is not available.
    """
    if not supabase_client:
        logger.error("Supabase client not provided to fetch_relevant_data.")
        return None

    now = datetime.now(timezone.utc)
    future_time = now + timedelta(hours=hours_ahead)
    now_iso = now.isoformat()
    future_iso = future_time.isoformat()

    logger.info(
        f"Fetching games starting between {now_iso} and {future_iso} from Supabase..."
    )

    try:
        # Query games table, filter by start time, and fetch related markets and odds
        # The string syntax 'markets(*, odds(*))' tells Supabase to join these tables.
        response: APIResponse = (
            await supabase_client.table("games")
            .select(
                "*, markets(*, odds(*))"
            )  # Select all game fields, all market fields, all odds fields
            .gte("start_time_utc", now_iso)  # Greater than or equal to now
            .lte("start_time_utc", future_iso)  # Less than or equal to future time
            .execute()
        )

        if response.data:
            logger.success(f"Successfully fetched {len(response.data)} relevant games.")
            return response.data
        else:
            # It's not an error if no games are found in the timeframe
            logger.info("No games found starting in the specified timeframe.")
            return []

    except APIError as e:
        logger.error(f"Supabase API error fetching data: {e.message}")
        logger.debug(f"Full APIError details: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred fetching data: {e}")
        logger.exception("Traceback:")
        return None


# --- Next Steps ---
# 1. Define how to calculate "fair odds" -> Using placeholder for now
# 2. Implement the EV calculation logic using the fetched data
# 3. Integrate this module into main.py


# Define a structure for EV results (can be expanded)
class EVBet:
    def __init__(
        self,
        game_info: str,
        market_info: str,
        bookmaker: str,
        decimal_odds: float,
        fair_odds: float,
        ev_percentage: float,
        stake: Optional[float] = None,  # Future use: Kelly Criterion?
    ):
        self.game_info = game_info
        self.market_info = market_info
        self.bookmaker = bookmaker
        self.decimal_odds = decimal_odds
        self.fair_odds = fair_odds
        self.ev_percentage = ev_percentage
        self.stake = stake

    def __repr__(self):
        return f"EVBet(game='{self.game_info}', market='{self.market_info}', \
            book='{self.bookmaker}', odds={self.decimal_odds:.2f}, \
            fair={self.fair_odds:.2f}, EV={self.ev_percentage:.2%})"


async def calculate_ev(games_data: List[Dict[str, Any]]) -> List[EVBet]:
    """
    Calculates the Expected Value (EV) for odds based on fetched game data.

    Args:
        games_data: A list of game dictionaries, including nested markets and odds,
                    fetched from Supabase by fetch_relevant_data.

    Returns:
        A list of EVBet objects representing ALL calculated EV results.
        (Note: With the current placeholder logic, EV will be near-zero).
    """
    # Change list name to reflect it holds all results now
    all_calculated_bets: List[EVBet] = []

    if not games_data:
        logger.info("No game data provided for EV calculation.")
        return all_calculated_bets

    logger.info(f"Calculating EV for {len(games_data)} games...")

    for game in games_data:
        game_desc = f"{game.get('sport', 'Unknown Sport')} - {game.get('league', 'Unknown League')} - {game.get('home_team_id', '?')} vs {game.get('away_team_id', '?')} @ {game.get('start_time_utc', '?')}"

        if not game.get("markets"):
            continue

        for market in game["markets"]:
            market_desc = f"{market.get('market_type', '?')} ({market.get('period', '?')}) Line: {market.get('line', 'N/A')}"
            market_odds = market.get("odds")

            if not market_odds or len(market_odds) < 1:
                continue

            # --- Placeholder Fair Odds Logic ---
            fair_odds_reference = market_odds[0]
            try:
                fair_decimal_odds = float(fair_odds_reference["decimal_odds"])
                if fair_decimal_odds <= 1.0:
                    logger.warning(
                        f"Invalid reference decimal odds ({fair_decimal_odds}) for market {market_desc} in game {game_desc}. Skipping market."
                    )
                    continue
                placeholder_fair_odds = fair_decimal_odds
            except (ValueError, TypeError, KeyError) as e:
                logger.warning(
                    f"Could not process reference odds for market {market_desc} in game {game_desc}: {e}. Skipping market."
                )
                continue
            # --- End Placeholder ---

            for odds_item in market_odds:
                try:
                    bookmaker_odds = float(odds_item["decimal_odds"])
                    bookmaker = odds_item.get("bookmaker", "Unknown Bookmaker")

                    if bookmaker_odds <= 1.0:
                        continue

                    ev_percentage = (bookmaker_odds / placeholder_fair_odds) - 1

                    logger.debug(
                        f"Game: {game_desc}, Market: {market_desc}, Book: {bookmaker}, Odds: {bookmaker_odds:.2f}, Fair: {placeholder_fair_odds:.2f}, EV: {ev_percentage:.2%}"
                    )

                    # Create EVBet object for EVERY calculated odd, regardless of EV
                    bet = EVBet(
                        game_info=game_desc,
                        market_info=market_desc,
                        bookmaker=bookmaker,
                        decimal_odds=bookmaker_odds,
                        fair_odds=placeholder_fair_odds,
                        ev_percentage=ev_percentage,
                    )
                    # Append all calculated bets
                    all_calculated_bets.append(bet)

                    # Remove the positive EV check for now
                    # if ev_percentage > 0.001:
                    #    positive_ev_bets.append(bet)
                    #    logger.info(f"Found potential +EV bet: {bet}")

                except (ValueError, TypeError, KeyError) as e:
                    logger.warning(
                        f"Could not process odds item {odds_item} for market {market_desc} in game {game_desc}: {e}. Skipping item."
                    )
                    continue

    # Update log message to reflect total calculated bets
    logger.success(
        f"EV Calculation complete. Calculated EV for {len(all_calculated_bets)} odds records (using placeholder fair odds)."
    )
    # Return the full list
    return all_calculated_bets
