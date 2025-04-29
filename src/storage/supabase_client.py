# src/storage/supabase_client.py
import sys
from typing import List, Dict, Any, Optional, Tuple
from decimal import Decimal

from loguru import logger
from models.enums import Bookmaker, MarketSide
from supabase import create_client, create_async_client, Client, AsyncClient
from postgrest import APIResponse
from postgrest.exceptions import APIError

# Assume settings are configured with SUPABASE_URL and SUPABASE_KEY
# We might need to add these to src/config/settings.py
from src.config.settings import settings

# Import our data models
from src.models.team import Team
from src.models.game import Game
from src.models.market import Market
from src.models.odds import Odds

# Module-level storage for the async client instance
_async_supabase_client: Optional[AsyncClient] = None


async def initialize_supabase() -> Optional[AsyncClient]:
    """Initializes the global ASYNC Supabase client and returns it."""
    global _async_supabase_client
    if _async_supabase_client:
        logger.debug("Async Supabase client already initialized.")
        return _async_supabase_client

    if not settings.SUPABASE_URL or not settings.SUPABASE_KEY:
        logger.critical("Supabase URL or Key not configured in settings.")
        raise SystemExit("Supabase configuration missing.")

    logger.debug(
        f"Attempting to initialize Async Supabase client with URL: {settings.SUPABASE_URL}"
    )
    key_snippet = (
        f"{settings.SUPABASE_KEY[:5]}...{settings.SUPABASE_KEY[-5:]}"
        if settings.SUPABASE_KEY
        else "None"
    )
    logger.debug(f"Using Supabase Key (snippet): {key_snippet}")

    try:
        # Explicitly await the creation of the async client
        client: AsyncClient = await create_async_client(
            settings.SUPABASE_URL, settings.SUPABASE_KEY
        )
        _async_supabase_client = client
        logger.success("Async Supabase client initialized successfully.")
        return client
    except Exception as e:
        logger.exception(f"Failed to initialize Async Supabase client: {e}")
        return None


async def get_supabase_client() -> Optional[AsyncClient]:
    """Returns the initialized ASYNC Supabase client instance."""
    # This function might not be strictly necessary anymore if initialize_supabase
    # is always called first and its result is used, but keep for now.
    global _async_supabase_client
    if not _async_supabase_client:
        logger.warning("Async Supabase client accessed before initialization.")
        return None
    return _async_supabase_client


async def _handle_upsert(table_name: str, data: List[Dict[str, Any]]) -> bool:
    """Handles the upsert operation for a given table using the ASYNC client."""
    # Directly use the module-level client, assuming it's initialized
    # This avoids potential issues with awaiting the getter function result.
    global _async_supabase_client
    client = _async_supabase_client

    if not client:
        # This should ideally not happen if initialize_supabase was called successfully
        logger.error(
            "Async Supabase client not available for upsert (module-level access)."
        )
        return False

    if not data:
        logger.debug(f"No data provided for upsert to table {table_name}. Skipping.")
        return True

    try:
        # Await the execute() call for the async client
        response: APIResponse = await client.table(table_name).upsert(data).execute()

        # Check response status - PostgREST upsert typically returns 200 or 201 on success
        # The python client might raise APIError for >= 400, but explicit check is safer.
        # Note: The actual structure might vary slightly; consult Postgrest-py docs if needed.
        # For now, assume APIError is raised on failure.

        logger.success(
            f"Successfully upserted {len(data)} records to {table_name} (async)."
        )
        return True
    except APIError as e:
        logger.error(f"Error during async upsert to {table_name}: {e.message}")
        logger.debug(f"Full APIError details: {e}")
        return False
    except Exception as e:
        logger.error(
            f"An unexpected error occurred during async upsert to {table_name}: {e}"
        )
        logger.exception("Traceback:")
        return False


# --- Individual Save Functions ---
# No changes needed here as they call _handle_upsert
async def save_teams(teams: List[Team]) -> bool:
    """Upserts a list of Team objects into the Supabase 'teams' table."""
    team_data = [t.model_dump(exclude_none=True) for t in teams]
    return await _handle_upsert("teams", team_data)


async def save_games(games: List[Game]) -> bool:
    """Upserts a list of Game objects into the Supabase 'games' table."""
    game_data = []
    for g in games:
        data = {
            "game_id": g.game_id,
            "sport": g.sport.value,
            "league": g.league.value,
            "start_time_utc": g.start_time_utc.isoformat(),
            "home_team_id": g.home_team.team_id,
            "away_team_id": g.away_team.team_id,
        }
        if g.last_updated_utc:
            data["last_updated_utc"] = g.last_updated_utc.isoformat()
        if g.raw_event_id:
            data["raw_event_id"] = g.raw_event_id
        game_data.append(data)
    return await _handle_upsert("games", game_data)


async def save_markets(markets: List[Market]) -> bool:
    """Upserts a list of Market objects into the Supabase 'markets' table."""
    market_data = []
    for m in markets:
        data = {
            "market_id": m.market_id,
            "game_id": m.game_id,
            "market_type": m.market_type.value,
            "period": m.period.value,
            "raw_market_name": m.raw_market_name,
        }
        if m.line is not None:
            data["line"] = float(m.line)
        market_data.append(data)
    return await _handle_upsert("markets", market_data)


async def save_odds(odds_list: List[Odds]) -> bool:
    """Upserts a list of Odds objects into the Supabase 'odds' table."""
    odds_data = []
    for o in odds_list:
        data = {
            "market_id": o.market_id,
            "bookmaker": o.bookmaker.value,
            "side": o.side.value,
            "decimal_odds": float(o.decimal_odds),
            "timestamp_collected": o.timestamp_collected.isoformat(),
        }
        if o.points is not None:
            data["points"] = float(o.points)
        if o.line is not None:
            data["line"] = float(o.line)
        if o.american_odds is not None:
            data["american_odds"] = o.american_odds
        odds_data.append(data)
    return await _handle_upsert("odds", odds_data)


async def save_normalized_data(normalized_games: List[Game]) -> bool:
    """Saves all normalized data (games, markets, odds, teams) to Supabase,
    handling potential duplicate games from different sources by merging markets."""

    if not normalized_games:
        logger.warning("No normalized games provided to save.")
        return True

    # --- Robust Game Deduplication and Merging ---
    canonical_games: Dict[Tuple, Game] = {}
    all_teams: Dict[str, Team] = {}
    all_markets: Dict[str, Market] = {}
    all_odds_raw: List[Odds] = []

    def get_fuzzy_game_key(game: Game) -> Optional[Tuple]:
        """Generates a key to identify potentially duplicate games."""
        if not game.home_team or not game.away_team or not game.start_time_utc:
            logger.warning(
                f"Game {game.game_id} missing team or start time for fuzzy key."
            )
            return None

        # Sort canonical names to handle potential home/away swaps between sources
        team_names_tuple = tuple(
            sorted((game.home_team.canonical_name, game.away_team.canonical_name))
        )
        game_date = game.start_time_utc.date()  # Match on date initially

        return (game.sport.value, game.league.value, team_names_tuple, game_date)

    logger.info(
        f"Starting deduplication and merging for {len(normalized_games)} input games."
    )
    merged_market_count = 0
    discarded_game_count = 0
    processed_market_ids_in_canonical = (
        set()
    )  # Keep track of market IDs added to canonical games

    for game in normalized_games:
        # Always collect teams
        if game.home_team and game.home_team.team_id not in all_teams:
            all_teams[game.home_team.team_id] = game.home_team
        if game.away_team and game.away_team.team_id not in all_teams:
            all_teams[game.away_team.team_id] = game.away_team

        fuzzy_key = get_fuzzy_game_key(game)
        if not fuzzy_key:
            logger.warning(
                f"Skipping game {game.game_id} due to inability to generate fuzzy key."
            )
            discarded_game_count += 1
            continue

        if fuzzy_key not in canonical_games:
            # This is the first time we've seen this game (based on the fuzzy key)
            canonical_games[fuzzy_key] = game
            logger.debug(
                f"Identified game {game.game_id} as canonical for key {fuzzy_key}"
            )
            # Collect its markets and odds
            for market in game.markets:
                if market.market_id not in all_markets:
                    all_markets[market.market_id] = market
                    processed_market_ids_in_canonical.add(market.market_id)
                    all_odds_raw.extend(market.odds)
                # Handle case where market might exist from a previous duplicate? Unlikely but safe.
                elif market.market_id not in processed_market_ids_in_canonical:
                    # If market exists in all_markets but wasn't processed as part of a canonical game yet
                    all_odds_raw.extend(market.odds)
                    processed_market_ids_in_canonical.add(market.market_id)

        else:
            # This game is a duplicate. Update its markets' game_id and collect entities.
            canonical_game = canonical_games[fuzzy_key]
            logger.debug(
                f"Game {game.game_id} is a duplicate of {canonical_game.game_id} (key {fuzzy_key}). Merging markets."
            )
            discarded_game_count += 1

            if game.markets:
                for market_to_merge in game.markets:
                    # ** CRITICAL STEP: Update game_id before collecting **
                    original_market_game_id = market_to_merge.game_id
                    market_to_merge.game_id = canonical_game.game_id
                    logger.debug(
                        f"Updated market {market_to_merge.market_id} game_id from {original_market_game_id} to {canonical_game.game_id}"
                    )

                    if market_to_merge.market_id not in all_markets:
                        all_markets[market_to_merge.market_id] = market_to_merge
                        processed_market_ids_in_canonical.add(market_to_merge.market_id)
                        all_odds_raw.extend(market_to_merge.odds)
                        merged_market_count += 1
                    elif (
                        market_to_merge.market_id
                        not in processed_market_ids_in_canonical
                    ):
                        # Market already exists, but we haven't added its odds yet (edge case)
                        all_odds_raw.extend(market_to_merge.odds)
                        processed_market_ids_in_canonical.add(market_to_merge.market_id)
                        merged_market_count += (
                            1  # Count as merge even if only odds added
                        )

    # The list of games to save is now the values from the canonical_games map
    games_to_save = list(canonical_games.values())

    logger.info(
        f"Finished deduplication. Saving {len(games_to_save)} canonical games."
        f" Merged/processed {merged_market_count} markets from {discarded_game_count} duplicate game objects."
    )

    # --- Extract unique entities (post-merge) ---\n    # Entities were already collected above, just log counts
    logger.info(
        f"Total unique entities collected: {len(all_teams)} teams, "
        f"{len(all_markets)} markets, {len(all_odds_raw)} raw odds instances."
    )

    # --- Filter odds (using the established primary key logic) ---\n    # (Same logic as before)
    latest_odds_map: Dict[Tuple[str, Bookmaker, MarketSide], Odds] = {}
    for odds_item in all_odds_raw:
        key = (odds_item.market_id, odds_item.bookmaker, odds_item.side)
        if (
            key not in latest_odds_map
            or odds_item.timestamp_collected > latest_odds_map[key].timestamp_collected
        ):
            latest_odds_map[key] = odds_item

    unique_latest_odds = list(latest_odds_map.values())
    logger.info(
        f"Filtered down to {len(unique_latest_odds)} unique latest odds records to save."
    )

    # --- Save entities ---
    success_teams = await save_teams(list(all_teams.values()))
    if not success_teams:
        logger.error("Failed to save teams.")
        return False

    # Save the canonical list of games (which now contain merged markets)
    success_games = await save_games(games_to_save)
    if not success_games:
        logger.error("Failed to save canonical games.")
        return False

    # Save all unique markets collected from all games (canonical and duplicates)
    success_markets = await save_markets(list(all_markets.values()))
    if not success_markets:
        logger.error("Failed to save unique markets.")
        return False

    # Save the filtered latest odds
    success_odds = await save_odds(unique_latest_odds)
    if not success_odds:
        logger.error("Failed to save unique latest odds.")
        return False

    logger.success(
        "Successfully saved all deduplicated and merged data components to Supabase."
    )
    return True


# --- Example Usage (commented out) ---
# async def example():
#     client = await initialize_supabase()
#     if client:
#         # ... operations using client ...
#         await client.close() # Close async client when done
