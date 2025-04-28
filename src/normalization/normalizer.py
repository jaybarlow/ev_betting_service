from typing import Dict, List, Tuple, Any, Optional
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
import json
import re

from loguru import logger

from src.models.enums import Bookmaker, Sport, League, MarketType, MarketSide, Period


from src.models.game import Game, Team
from src.models.market import Market
from src.models.odds import Odds
from src.utils.misc_utils import generate_canonical_id

# Type alias for the output of normalization
NormalizedData = Tuple[List[Game], List[Market], List[Odds]]


class NormalizationError(Exception):
    """Custom exception for data normalization errors."""

    pass


class Normalizer:
    """Handles normalization of raw data from different bookmakers."""

    def __init__(self):
        # TODO: Initialize any necessary mapping tables (e.g., team aliases)
        self.team_aliases: Dict[str, str] = {}
        self.market_aliases: Dict[str, MarketType] = {
            # Example aliases - needs population based on actual data
            "moneyline": MarketType.MONEYLINE,
            "money line": MarketType.MONEYLINE,
            "ml": MarketType.MONEYLINE,
            "point spread": MarketType.SPREAD,
            "spread": MarketType.SPREAD,
            "run line": MarketType.SPREAD,
            "puck line": MarketType.SPREAD,
            "handicap": MarketType.SPREAD,
            "total points": MarketType.TOTAL,
            "total": MarketType.TOTAL,
            "over/under": MarketType.TOTAL,
            "over under": MarketType.TOTAL,
        }
        logger.info("Normalizer initialized.")

    def normalize(
        self, raw_data_by_book: Dict[Bookmaker, List[Dict[str, Any]]]
    ) -> List[Game]:
        """Normalizes raw data from multiple bookmakers into a list of Game objects.

        Args:
                        raw_data_by_book: A dictionary where keys are Bookmaker enums
                                          and values are lists of raw data objects.

        Returns:
                        A list of normalized Game objects, with nested Market and Odds objects.
        """
        all_normalized_games: List[Game] = []  # Changed variable name for clarity

        logger.info(
            f"Starting normalization for bookmakers: {list(raw_data_by_book.keys())}"
        )

        for bookmaker, raw_responses in raw_data_by_book.items():
            if not raw_responses:
                logger.warning(
                    f"No raw data received for {bookmaker.value}, skipping normalization."
                )
                continue

            logger.debug(
                f"Normalizing {len(raw_responses)} raw response(s) for {bookmaker.value}"
            )
            try:
                normalized_games_for_book: List[Game] = []
                if bookmaker == Bookmaker.CRAB_SPORTS:
                    # Crab Sports scraper returns a list of responses (one per sport)
                    for raw_response_dict in raw_responses:
                        normalized_games_for_book.extend(
                            self._normalize_crabsports_data(raw_response_dict)
                        )
                # TODO: Add handlers for other bookmakers (Pinnacle, TBD)
                # elif bookmaker == Bookmaker.PINNACLE:
                #     normalized_games_for_book = self._normalize_pinnacle_data(raw_data)
                else:
                    logger.warning(
                        f"Normalization not implemented for bookmaker: {bookmaker.value}"
                    )

                all_normalized_games.extend(normalized_games_for_book)

            except Exception as e:
                logger.exception(f"Error normalizing data for {bookmaker.value}: {e}")
                # Optionally continue to next bookmaker or raise
                continue

        # TODO: Implement game deduplication/merging logic across bookmakers if needed
        # This might involve processing `all_normalized_games` after initial loop.
        logger.info(
            f"Normalization complete. Produced {len(all_normalized_games)} normalized Game objects."
        )
        return all_normalized_games

    def _normalize_crabsports_data(self, raw_response: Dict[str, Any]) -> List[Game]:
        """Normalizes the raw dictionary structure from Crab Sports API response into Game objects."""
        normalized_games: List[Game] = []  # Renamed from 'games' for clarity
        bookmaker = Bookmaker.CRAB_SPORTS

        logger.debug(
            f"Parsing Crab Sports raw response. Top keys: {list(raw_response.keys())}"
        )

        events_component = None
        if "components" in raw_response and isinstance(
            raw_response["components"], list
        ):
            for component in raw_response["components"]:
                if (
                    isinstance(component, dict)
                    and component.get("tree_compo_key") == "prematch_event_list"
                ):
                    events_component = component
                    break

        if not events_component:
            logger.warning(
                "Could not find 'prematch_event_list' component in Crab Sports response."
            )
            return []

        raw_events_data = events_component.get("data")
        if not raw_events_data or not isinstance(raw_events_data, dict):
            logger.warning(
                "Found 'prematch_event_list' component, but 'data' field is missing or not a dictionary."
            )
            return []

        # --- Determine the actual path to the events list ---
        # Inspecting the previous JSON snippet, events seem nested under competitions
        raw_events = []
        if "competitions" in raw_events_data and isinstance(
            raw_events_data["competitions"], list
        ):
            for competition in raw_events_data["competitions"]:
                if (
                    isinstance(competition, dict)
                    and "events" in competition
                    and isinstance(competition["events"], list)
                ):
                    raw_events.extend(competition["events"])
        elif "events" in raw_events_data and isinstance(
            raw_events_data["events"], list
        ):
            # Fallback if structure is simpler than expected
            raw_events = raw_events_data["events"]
        else:
            logger.warning(
                "Could not find 'events' list within component data (checked 'data.competitions[].events' and 'data.events')."
            )
            return []

        logger.debug(
            f"Found {len(raw_events)} potential raw events in Crab Sports component."
        )

        for raw_event in raw_events:
            if not isinstance(raw_event, dict):
                logger.warning(
                    f"Skipping non-dictionary item in raw_events list: {type(raw_event)}"
                )
                continue

            try:
                # --- Extract Game Info ---
                # Using observed keys from the JSON snippet provided
                event_id = str(raw_event.get("id", "UNKNOWN_ID"))
                sport_info = raw_event.get("sport", {})
                sport_name = (
                    sport_info.get("label") if isinstance(sport_info, dict) else None
                )
                competition_info = raw_event.get("competition", {})
                league_name = (
                    competition_info.get("label")
                    if isinstance(competition_info, dict)
                    else None
                )
                start_time_str = raw_event.get("start")  # Use 'start' key

                # Participants are under 'actors' key
                actors = raw_event.get("actors", [])
                home_team_name = "Unknown Home"
                away_team_name = "Unknown Away"

                # Use 'type' and 'label' from actors list
                if isinstance(actors, list) and len(actors) >= 2:
                    for actor in actors:
                        if isinstance(actor, dict):
                            actor_type = actor.get("type")
                            actor_label = actor.get("label")
                            if actor_type == "home":
                                home_team_name = actor_label or home_team_name
                            elif actor_type == "away":
                                away_team_name = actor_label or away_team_name

                if (
                    home_team_name == "Unknown Home"
                    or away_team_name == "Unknown Away"
                    or not home_team_name
                    or not away_team_name
                ):
                    # Log participant data for debugging if extraction failed
                    logger.warning(
                        f"Could not reliably determine home/away teams for event {event_id}. Actors data received: {json.dumps(actors)}. Skipping event."
                    )
                    continue

                # --- Map & Parse Game Info ---
                sport = self._map_sport(sport_name)
                league = self._map_league(league_name, sport)
                start_time = self._parse_datetime(start_time_str)

                if not sport or not league or not start_time:
                    logger.warning(
                        f"Skipping event {event_id} due to missing/unmappable core info (Sport: {sport_name}, League: {league_name}, Start: {start_time_str})"
                    )
                    continue

                # --- Generate IDs and Create Objects ---
                home_team_id = generate_canonical_id(home_team_name)
                away_team_id = generate_canonical_id(away_team_name)
                game_id = generate_canonical_id(
                    f"{sport.value}_{league.value}_{away_team_id}_at_{home_team_id}_{start_time.strftime('%Y%m%d')}"
                )

                home_team = Team(
                    team_id=home_team_id,
                    raw_name=home_team_name,
                    canonical_name=self._get_canonical_team_name(home_team_name),
                )
                away_team = Team(
                    team_id=away_team_id,
                    raw_name=away_team_name,
                    canonical_name=self._get_canonical_team_name(away_team_name),
                )

                game = Game(
                    game_id=game_id,
                    bookmaker=bookmaker,
                    sport=sport,
                    league=league,
                    start_time_utc=start_time,
                    home_team=home_team,
                    away_team=away_team,
                    raw_event_id=event_id,
                    markets=[],
                )
                logger.debug(
                    f"Processing Game: {game.game_id} ({away_team.raw_name} @ {home_team.raw_name})"
                )

                # --- Extract Markets & Odds ---
                # Markets are under 'markets' -> 'bets' -> 'selections'
                raw_market_list = raw_event.get("markets", [])  # Top level is 'markets'
                if not isinstance(raw_market_list, list):
                    logger.warning(
                        f"Expected 'markets' for event {event_id} to be a list, got {type(raw_market_list)}. Skipping markets."
                    )
                    raw_market_list = []

                for (
                    raw_market_container
                ) in raw_market_list:  # This container might hold multiple 'bets'
                    if not isinstance(raw_market_container, dict):
                        continue

                    # The actual market info seems to be in the 'bets' list within the container
                    raw_bets = raw_market_container.get("bets", [])
                    if not isinstance(raw_bets, list) or not raw_bets:
                        # Skip if 'bets' is not a list or is empty
                        continue

                    # Assuming the first bet in the list holds the primary market info for this container
                    raw_bet = raw_bets[0]
                    if not isinstance(raw_bet, dict):
                        continue

                    try:
                        # Market name is the 'label' of the bet
                        market_name_raw = raw_bet.get("label")
                        if not market_name_raw:
                            logger.debug(
                                f"Skipping market in event {event_id} due to missing label in bet: {raw_bet}"
                            )
                            continue

                        # Try to extract line number primarily from the market name label (e.g., "Over/Under 9.5 runs")
                        # Will pass the full name to _map_market_type and _map_market_side for parsing
                        market_type = self._map_market_type(market_name_raw)

                        if not market_type:
                            logger.debug(
                                f"Skipping unknown market type: '{market_name_raw}' for event {event_id}",
                            )
                            continue

                        # Line is derived later from outcomes/label parsing
                        market_line: Optional[Decimal] = None

                        processed_outcomes: List[
                            Tuple[MarketSide, Decimal, str, Optional[Decimal]]
                        ] = []
                        # Outcomes are under 'selections' key within the bet
                        raw_selections = raw_bet.get("selections", [])
                        if not isinstance(raw_selections, list):
                            logger.warning(
                                f"Expected 'selections' for market '{market_name_raw}' (Event {event_id}) to be a list, got {type(raw_selections)}. Skipping outcomes for this market."
                            )
                            continue

                        for raw_selection in raw_selections:
                            if not isinstance(raw_selection, dict):
                                logger.warning(
                                    f"Skipping non-dictionary item in selections list for market '{market_name_raw}' (Event {event_id}): {type(raw_selection)}"
                                )
                                continue

                            try:
                                # Outcome name is the 'label' of the selection
                                outcome_label_raw = raw_selection.get("label")
                                # Price is under 'odds' key
                                price_decimal_val = raw_selection.get("odds")

                                if not outcome_label_raw or price_decimal_val is None:
                                    logger.debug(
                                        f"Skipping outcome in '{market_name_raw}' due to missing label or price."
                                    )
                                    continue

                                # Price seems to be directly a decimal/float in JSON
                                price_decimal = self._parse_decimal(price_decimal_val)

                                # Extract potential line from the outcome label (e.g., "Mets (-1.5)", "Over 9.5")
                                outcome_line = self._extract_line_from_label(
                                    outcome_label_raw
                                )

                                market_side = self._map_market_side(
                                    outcome_label_raw,
                                    market_type,
                                    home_team_name,
                                    away_team_name,
                                    outcome_line,  # Pass potential line for context
                                )

                                if not market_side or price_decimal is None:
                                    logger.debug(
                                        f"Skipping outcome '{outcome_label_raw}' for market '{market_name_raw}' (Event {event_id}) due to unmappable side or invalid price ('{price_decimal_val}')",
                                    )
                                    continue

                                # For Spread/Total markets, the first valid *outcome* line sets the market line
                                if (
                                    market_line is None
                                    and outcome_line is not None
                                    and market_type
                                    in [MarketType.SPREAD, MarketType.TOTAL]
                                ):
                                    market_line = outcome_line

                                processed_outcomes.append(
                                    (
                                        market_side,
                                        price_decimal,
                                        outcome_label_raw,
                                        outcome_line,
                                    )
                                )
                            except Exception as outcome_exc:
                                logger.error(
                                    f"Error processing selection (outcome) for market '{market_name_raw}' (Event {event_id}): {outcome_exc}"
                                )
                                logger.debug(
                                    f"Problematic selection data: {raw_selection}"
                                )

                        # Generate market ID using the derived line
                        market_id = generate_canonical_id(
                            f"{game_id}_{market_type.value}_{market_line or 'base'}"
                        )

                        market = Market(
                            market_id=market_id,
                            game_id=game_id,
                            market_type=market_type,
                            line=market_line,
                            period=Period.FULL_GAME,
                            raw_market_name=market_name_raw,
                            odds=[],
                        )
                        logger.debug(
                            f"Processing Market: {market.market_id} ({market.market_type.value}, Line: {market.line})",
                        )

                        for side, price, raw_label, _ in processed_outcomes:
                            odds = Odds(
                                market_id=market_id,
                                bookmaker=bookmaker,
                                decimal_odds=price,
                                timestamp_collected=datetime.now(timezone.utc),
                            )
                            logger.debug(
                                f"Type of market object before appending odds: {type(market)}"
                            )
                            market.odds.append(odds)
                            logger.debug(
                                f"Added Odds: MarketID={market_id}, Price={odds.decimal_odds}"
                            )

                        logger.debug(
                            f"Type of market object before checking odds: {type(market)}"
                        )
                        if market.odds:
                            logger.debug(
                                f"Type of game object before appending market: {type(game)}"
                            )
                            game.markets.append(market)
                        else:
                            logger.debug(
                                f"Market '{market.raw_market_name}' (Event {event_id}) has no valid outcomes, discarding."
                            )

                    except Exception as market_exc:
                        logger.error(
                            f"Error processing bet/market container for event {event_id}: {market_exc}"
                        )
                        logger.debug(
                            f"Problematic market container data: {raw_market_container}"
                        )

                logger.debug(
                    f"Type of game object before checking markets: {type(game)}"
                )
                if game.markets:
                    normalized_games.append(game)
                else:
                    logger.debug(
                        f"Game {game.game_id} has no valid markets, discarding."
                    )

            except Exception as event_exc:
                logger.error(f"Error processing event: {event_exc}")
                event_id_debug = (
                    raw_event.get("id", "MISSING_ID")
                    if isinstance(raw_event, dict)
                    else "INVALID_EVENT_TYPE"
                )
                logger.debug(
                    f"Problematic event data (ID: {event_id_debug}): {raw_event}"
                )

        logger.info(
            f"Finished normalization for this Crab Sports response. Produced {len(normalized_games)} Game objects."
        )
        return normalized_games

    # --- Helper methods for mapping and parsing ---

    def _map_sport(self, raw_sport_name: Optional[str]) -> Optional[Sport]:
        if not raw_sport_name:
            return None
        raw_lower = raw_sport_name.lower().strip()
        if "basketball" in raw_lower:
            return Sport.NBA
        if "hockey" in raw_lower or "ice hockey" in raw_lower:
            return Sport.NHL
        if "baseball" in raw_lower:
            return Sport.MLB
        if "soccer" in raw_lower:
            return Sport.SOCCER
        if "tennis" in raw_lower:
            return Sport.TENNIS
        logger.debug(f"Could not map sport: {raw_sport_name}")
        return None

    def _map_league(
        self, raw_league_name: Optional[str], sport: Optional[Sport]
    ) -> Optional[League]:
        if not raw_league_name:
            return None
        raw_lower = raw_league_name.lower().strip()
        if sport == Sport.NBA and (
            "nba" in raw_lower or "national basketball association" in raw_lower
        ):
            return League.NBA
        if sport == Sport.NHL and (
            "nhl" in raw_lower or "national hockey league" in raw_lower
        ):
            return League.NHL
        if sport == Sport.MLB and (
            "mlb" in raw_lower or "major league baseball" in raw_lower
        ):
            return League.MLB
        if sport == Sport.SOCCER:
            if "premier league" in raw_lower:
                return League.EPL
            if "champions league" in raw_lower:
                return League.CHAMPIONS_LEAGUE
            if "la liga" in raw_lower:
                return League.LA_LIGA
            if "serie a" in raw_lower:
                return League.SERIE_A
            if "bundesliga" in raw_lower:
                return League.BUNDESLIGA
            if "ligue 1" in raw_lower:
                return League.LIGUE_1
            if "mls" in raw_lower or "major league soccer" in raw_lower:
                return League.MLS
        if sport == Sport.TENNIS:
            if "atp" in raw_lower:
                return League.ATP
            if "wta" in raw_lower:
                return League.WTA
        logger.debug(f"Could not map league: {raw_league_name}")
        return None

    def _map_market_type(self, raw_market_name: Optional[str]) -> Optional[MarketType]:
        if not raw_market_name:
            return None
        raw_lower = raw_market_name.lower().strip()
        # Check direct aliases first
        mapped_type = self.market_aliases.get(raw_lower)
        if mapped_type:
            return mapped_type

        # Handle names from Crab Sports JSON structure (bets[0]['label'])
        if "moneyline" in raw_lower:
            return MarketType.MONEYLINE
        if "spread" in raw_lower:
            return MarketType.SPREAD
        if "run line" in raw_lower:
            return MarketType.SPREAD  # Alias for baseball
        if "over/under" in raw_lower or "total" in raw_lower:
            return MarketType.TOTAL
        if raw_lower == "winner 2-way":
            return MarketType.MONEYLINE  # Another potential alias

        logger.debug(f"Market type not recognized: '{raw_market_name}'")
        return None

    def _extract_line_from_label(self, label: Optional[str]) -> Optional[Decimal]:
        """Attempts to extract a numeric line value (like 1.5 or 9.5) from a label."""
        if not label:
            return None
        # Regex to find numbers (int or float, positive or negative) possibly at the end or near keywords
        # Handles cases like "Team (-1.5)", "Over 9.5", "Under 110.5", "Spread (7.5)"
        match = re.search(r"([+-]?\d*\.?\d+)\)?$", label.strip())
        if match:
            try:
                return Decimal(match.group(1))
            except InvalidOperation:
                pass  # Fall through if number parsing fails

        # Maybe look for numbers after Over/Under specifically
        if label.lower().startswith("over") or label.lower().startswith("under"):
            match = re.search(r"([+-]?\d*\.?\d+)", label)
            if match:
                try:
                    return Decimal(match.group(1))
                except InvalidOperation:
                    pass

        # logger.log("SPAM", f"Could not extract line from label: {label}") # Too noisy
        return None

    def _map_market_side(
        self,
        raw_outcome_label: str,
        market_type: MarketType,
        home_team: str,
        away_team: str,
        line: Optional[Decimal],  # Line extracted from this outcome's label
    ) -> Optional[MarketSide]:
        """Determines the MarketSide based on outcome label, market type, and team names."""
        label_lower = raw_outcome_label.lower().strip()
        home_lower = home_team.lower().strip()
        away_lower = away_team.lower().strip()

        # For Moneyline and Spread, check if label *starts* with team name
        if market_type in [MarketType.MONEYLINE, MarketType.SPREAD]:
            if label_lower.startswith(home_lower):
                return MarketSide.HOME
            if label_lower.startswith(away_lower):
                return MarketSide.AWAY
            # Fallback check for exact match (Moneyline case where label is just team name)
            if label_lower == home_lower:
                return MarketSide.HOME
            if label_lower == away_lower:
                return MarketSide.AWAY

        elif market_type == MarketType.TOTAL:
            # Check if label starts with over/under
            if label_lower.startswith("over"):
                return MarketSide.OVER
            if label_lower.startswith("under"):
                return MarketSide.UNDER
            if label_lower == "o":
                return MarketSide.OVER
            if label_lower == "u":
                return MarketSide.UNDER

        logger.debug(
            f"Could not map market side for outcome '{raw_outcome_label}' in market {market_type.value}"
        )
        return None

    def _parse_datetime(self, datetime_str: Optional[str]) -> Optional[datetime]:
        if not datetime_str:
            return None
        try:
            # Crab Sports uses ISO 8601 format with timezone offset like "2025-04-28T22:05:00.000+02:00"
            dt = datetime.fromisoformat(datetime_str)
            # Convert to UTC
            return dt.astimezone(timezone.utc)
        except ValueError:
            logger.warning(
                f"Could not parse datetime string with ISO format: {datetime_str}"
            )
            return None

    def _parse_decimal(self, value: Any) -> Optional[Decimal]:
        if value is None:
            return None
        # Crab Sports provides decimal odds directly as float/int in the 'odds' key
        try:
            # Convert directly to Decimal
            return Decimal(value)
        except (InvalidOperation, ValueError, TypeError) as e:
            logger.warning(f"Could not parse value '{value}' as Decimal: {e}")
            return None

    def _get_canonical_team_name(self, raw_name: str) -> str:
        raw_name_clean = raw_name.lower().strip()
        return self.team_aliases.get(raw_name_clean, raw_name_clean)

    # TODO: Add methods for _normalize_pinnacle_data, etc.
