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
from src.utils.calcs import american_to_decimal  # <-- Import converter

# Type alias for the output of normalization
NormalizedData = Tuple[List[Game], List[Market], List[Odds]]


class NormalizationError(Exception):
    """Custom exception for data normalization errors."""

    pass


class Normalizer:
    """Handles normalization of raw data from different bookmakers."""

    def __init__(self):
        # Initialize team aliases mapping
        # Key: lowercased raw name, Value: desired canonical name
        self.team_aliases: Dict[str, str] = {
            # NBA Examples
            "knicks": "new york knicks",
            "nyk": "new york knicks",
            "pistons": "detroit pistons",
            "det": "detroit pistons",
            "nets": "brooklyn nets",
            "bkn": "brooklyn nets",
            "celtics": "boston celtics",
            "bos": "boston celtics",
            "lakers": "los angeles lakers",
            "lal": "los angeles lakers",
            "clippers": "los angeles clippers",
            "lac": "los angeles clippers",
            # Add more aliases for all relevant teams/sports
        }
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
        logger.info(
            f"Normalizer initialized with {len(self.team_aliases)} team aliases."
        )

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
                elif bookmaker == Bookmaker.PINNACLE:
                    # Pinnacle scraper returns a list of dicts, each with 'matchups' and 'markets' for a league
                    for raw_league_data in raw_responses:
                        normalized_games_for_book.extend(
                            self._normalize_pinnacle_data(raw_league_data)
                        )
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

                        for side, price, raw_label, outcome_line in processed_outcomes:
                            # Extract specific points for this side if it's a spread market
                            points_value: Optional[Decimal] = None
                            # Extract specific line for this side if it's a total market
                            line_value: Optional[Decimal] = None

                            if market_type == MarketType.SPREAD:
                                points_value = self._extract_points_from_outcome_label(
                                    raw_label
                                )
                            elif market_type == MarketType.TOTAL:
                                line_value = self._extract_line_from_label(raw_label)

                            odds = Odds(
                                market_id=market_id,
                                bookmaker=bookmaker,
                                side=side,
                                points=points_value,
                                line=line_value,
                                decimal_odds=price,
                                timestamp_collected=datetime.now(timezone.utc),
                            )

                            logger.debug(
                                f"Type of market object before appending odds: {type(market)}"
                            )
                            market.odds.append(odds)

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

    def _normalize_pinnacle_data(self, raw_league_data: Dict[str, Any]) -> List[Game]:
        """Normalizes the raw dictionary structure from Pinnacle API response for a specific league."""
        normalized_games: List[Game] = []
        bookmaker = Bookmaker.PINNACLE

        raw_matchups = raw_league_data.get("matchups", [])
        raw_markets = raw_league_data.get("markets", [])
        league_id_api = raw_league_data.get(
            "league_id", "UNKNOWN"
        )  # Get league ID for context

        if not raw_matchups or not raw_markets:
            logger.warning(
                f"Missing 'matchups' or 'markets' data for Pinnacle league {league_id_api}. Skipping."
            )
            return []

        logger.debug(
            f"Normalizing {len(raw_matchups)} matchups and {len(raw_markets)} markets for Pinnacle league {league_id_api}"
        )

        # Create a lookup for markets by matchupId for faster access
        markets_by_matchup: Dict[int, List[Dict[str, Any]]] = {}
        for market_item in raw_markets:
            matchup_id = market_item.get("matchupId")
            if matchup_id:
                if matchup_id not in markets_by_matchup:
                    markets_by_matchup[matchup_id] = []
                markets_by_matchup[matchup_id].append(market_item)

        # Process matchups (games)
        for raw_matchup in raw_matchups:
            if not isinstance(raw_matchup, dict):
                continue

            # --- Game Info Extraction ---
            try:
                # Pinnacle uses 'parentId' for matchup ID in some cases, fallback to 'id'
                # The original github code logic seemed complex here, simplifying to use parentId if present.
                # Revisit if this doesn't capture all games.
                pin_matchup_id = raw_matchup.get("parentId")
                if not pin_matchup_id:
                    pin_matchup_id = raw_matchup.get("id")

                if not pin_matchup_id:
                    logger.warning(
                        f"Skipping Pinnacle matchup due to missing id/parentId: {raw_matchup}"
                    )
                    continue

                league_info = raw_matchup.get("league", {})
                league_name = league_info.get("name")
                sport_name = league_info.get("sport", {}).get("name")

                # Time is nested under periods[0]
                periods = raw_matchup.get("periods", [{}])
                start_time_str = periods[0].get("cutoffAt") if periods else None

                # --- Extract Participants (Teams) ---
                # Participants might be directly in the matchup or nested in 'parent'
                participants = raw_matchup.get("participants")
                if not participants:
                    parent_data = raw_matchup.get("parent")  # Get parent, might be None
                    # Only try to get participants if parent_data is a dictionary
                    if isinstance(parent_data, dict):
                        participants = parent_data.get("participants")

                if not participants or len(participants) < 2:
                    logger.warning(
                        f"Could not determine teams for Pinnacle matchup {pin_matchup_id}. Data: {participants}. Skipping."
                    )
                    continue

                home_team_name = None
                away_team_name = None
                if isinstance(participants, list) and len(participants) == 2:
                    # Assuming [0] is home, [1] is away IF alignment is not neutral
                    # Need to check 'alignment'
                    if (
                        participants[0].get("alignment") == "home"
                        and participants[1].get("alignment") == "away"
                    ):
                        home_team_name = participants[0].get("name")
                        away_team_name = participants[1].get("name")
                    elif (
                        participants[0].get("alignment") == "away"
                        and participants[1].get("alignment") == "home"
                    ):
                        home_team_name = participants[1].get("name")
                        away_team_name = participants[0].get("name")
                    else:  # Fallback if alignment isn't clear, rely on order (less reliable)
                        logger.debug(
                            f"Using participant order fallback for Pinnacle teams in matchup {pin_matchup_id}"
                        )
                        home_team_name = participants[0].get("name")
                        away_team_name = participants[1].get("name")

                if not home_team_name or not away_team_name:
                    logger.warning(
                        f"Could not determine teams for Pinnacle matchup {pin_matchup_id}. Data: {participants}. Skipping."
                    )
                    continue

                # --- Map & Parse Game Info ---
                sport = self._map_sport(sport_name)
                league = self._map_league(league_name, sport)
                start_time = self._parse_datetime(start_time_str)

                if not sport or not league or not start_time:
                    logger.warning(
                        f"Skipping Pinnacle matchup {pin_matchup_id} due to missing/unmappable core info (Sport: {sport_name}, League: {league_name}, Start: {start_time_str})"
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
                    raw_event_id=str(pin_matchup_id),
                    markets=[],
                )
                logger.debug(
                    f"Processing Pinnacle Game: {game.game_id} ({away_team.raw_name} @ {home_team.raw_name}) - API ID: {pin_matchup_id}"
                )

                # --- Process Markets for this Game ---
                game_markets = markets_by_matchup.get(pin_matchup_id, [])
                for raw_market in game_markets:
                    if not isinstance(raw_market, dict):
                        continue

                    try:
                        market_type_raw = raw_market.get("type")
                        period_raw = raw_market.get("period")  # 0 for Full Game
                        prices = raw_market.get("prices", [])

                        # We only care about full game (period 0) for now
                        if period_raw != 0:
                            continue

                        # Map market type
                        market_type = self._map_pinnacle_market_type(market_type_raw)
                        if not market_type:
                            logger.debug(
                                f"Skipping unknown Pinnacle market type '{market_type_raw}' for matchup {pin_matchup_id}"
                            )
                            continue

                        # Expect exactly 2 prices (outcomes) for these markets
                        if not isinstance(prices, list) or len(prices) != 2:
                            logger.debug(
                                f"Skipping Pinnacle market '{market_type_raw}' for matchup {pin_matchup_id} due to unexpected prices structure: {prices}"
                            )
                            continue

                        # Assign prices based on expected structure (assuming home/away or over/under)
                        # Need to handle based on market type
                        odds_data_list = []
                        market_line = None  # Overall line for the market
                        raw_market_name = f"{market_type.value} - Period {period_raw}"  # Simple raw name for now

                        if market_type == MarketType.MONEYLINE:
                            # Usually price[0] = Home, price[1] = Away (confirm with 'designation' if needed)
                            price_home = prices[0]
                            price_away = prices[1]
                            odds_home = american_to_decimal(price_home.get("price"))
                            odds_away = american_to_decimal(price_away.get("price"))
                            if odds_home and odds_away:
                                odds_data_list.append(
                                    (MarketSide.HOME, odds_home, None, None)
                                )
                                odds_data_list.append(
                                    (MarketSide.AWAY, odds_away, None, None)
                                )
                                raw_market_name = "Moneyline"  # More specific

                        elif market_type == MarketType.SPREAD:
                            # price[0] might be home or away, check 'points' sign or potentially 'designation'
                            # Assuming price[0] corresponds to home team's perspective for now
                            price1 = prices[0]
                            price2 = prices[1]
                            points1 = self._parse_decimal(price1.get("points"))
                            points2 = self._parse_decimal(price2.get("points"))
                            odds1 = american_to_decimal(price1.get("price"))
                            odds2 = american_to_decimal(price2.get("price"))

                            if points1 is not None and odds1:
                                # Assume points positive => home is +points, away is -points (or vice versa)
                                # The side association depends on which participant price[0] refers to.
                                # For now, assume price[0] is HOME side spread.
                                odds_data_list.append(
                                    (MarketSide.HOME, odds1, points1, None)
                                )
                                raw_market_name = (
                                    f"Spread ({points1})"  # Add line to raw name
                                )
                                market_line = abs(points1)  # Base line for the market
                            if points2 is not None and odds2:
                                odds_data_list.append(
                                    (MarketSide.AWAY, odds2, points2, None)
                                )
                                # Ensure market_line is set if only points2 was valid
                                if market_line is None:
                                    market_line = abs(points2)
                                if (
                                    raw_market_name
                                    == f"{market_type.value} - Period {period_raw}"
                                ):
                                    raw_market_name = f"Spread ({points2})"

                        elif market_type == MarketType.TOTAL:
                            # price[0] is Over, price[1] is Under based on 'designation'
                            price_over = None
                            price_under = None
                            for price in prices:
                                if price.get("designation") == "over":
                                    price_over = price
                                if price.get("designation") == "under":
                                    price_under = price

                            if price_over and price_under:
                                total_line = self._parse_decimal(
                                    price_over.get("points")
                                )  # Line is in 'points'
                                odds_over = american_to_decimal(price_over.get("price"))
                                odds_under = american_to_decimal(
                                    price_under.get("price")
                                )

                                if total_line is not None and odds_over and odds_under:
                                    odds_data_list.append(
                                        (MarketSide.OVER, odds_over, None, total_line)
                                    )
                                    odds_data_list.append(
                                        (MarketSide.UNDER, odds_under, None, total_line)
                                    )
                                    market_line = total_line
                                    raw_market_name = f"Total ({total_line})"

                        if not odds_data_list:
                            logger.debug(
                                f"No valid odds extracted for Pinnacle market '{market_type_raw}' for matchup {pin_matchup_id}"
                            )
                            continue

                        # --- Create Market and Odds --- #
                        market_id = generate_canonical_id(
                            f"{game_id}_{market_type.value}_{market_line or 'base'}"
                        )
                        market = Market(
                            market_id=market_id,
                            game_id=game_id,
                            market_type=market_type,
                            line=market_line,
                            period=Period.FULL_GAME,  # Period 0 maps to Full Game
                            raw_market_name=raw_market_name,  # Use generated name
                            odds=[],
                        )

                        for side, price, points_val, line_val in odds_data_list:
                            odds = Odds(
                                market_id=market_id,
                                bookmaker=bookmaker,
                                side=side,
                                points=points_val,
                                line=line_val,
                                decimal_odds=price,
                                timestamp_collected=datetime.now(timezone.utc),
                            )
                            market.odds.append(odds)

                        if market.odds:
                            game.markets.append(market)
                        else:
                            logger.debug(
                                f"Market '{market.raw_market_name}' (Matchup {pin_matchup_id}) has no valid outcomes, discarding."
                            )

                    except Exception as market_exc:
                        logger.exception(
                            f"Error processing Pinnacle market '{raw_market.get('type', 'UNKNOWN')}' for matchup {pin_matchup_id}: {market_exc}"
                        )
                    logger.debug(f"Problematic market data: {raw_market}")

                # --- Finish Game Processing --- #
                if game.markets:
                    normalized_games.append(game)
                else:
                    logger.debug(
                        f"Pinnacle Game {game.game_id} (API ID: {pin_matchup_id}) has no valid markets, discarding."
                    )

            except Exception as game_exc:
                logger.exception(
                    f"Error processing Pinnacle matchup {raw_matchup.get('id', 'UNKNOWN')}: {game_exc}"
                )
                logger.debug(f"Problematic matchup data: {raw_matchup}")

        logger.info(
            f"Finished normalization for Pinnacle league {league_id_api}. Produced {len(normalized_games)} Game objects."
        )
        return normalized_games

    def _map_pinnacle_market_type(
        self, raw_market_name: Optional[str]
    ) -> Optional[MarketType]:
        """Maps Pinnacle market type string to our enum."""
        # Simple mapping based on observed values
        if not raw_market_name:
            return None
        raw_lower = raw_market_name.lower().strip()
        if raw_lower == "moneyline":
            return MarketType.MONEYLINE
        if raw_lower == "spread":
            return MarketType.SPREAD
        if raw_lower == "total":
            return MarketType.TOTAL
        # Add other mappings if needed (e.g., 'team_total')
        logger.debug(f"Unknown Pinnacle market type encountered: {raw_market_name}")
        return None

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

    def _extract_points_from_outcome_label(
        self, label: Optional[str]
    ) -> Optional[Decimal]:
        """Attempts to extract spread points (like -1.5 or +7.5) from an outcome label."""
        if not label:
            return None
        # Regex to find a signed number (int or float) within parentheses
        # Handles \"Team Name (-1.5)\", \"Team Name (+7.0)\", \"Team Name (3.5)\"
        # Using raw string r\"...\" to avoid potential issues with backslash escaping
        match = re.search(r"\(([+-]?\d*\.?\d+)\)", label)
        if match:
            try:
                return Decimal(match.group(1))
            except (InvalidOperation, ValueError, TypeError):
                logger.warning(
                    f"Found potential points in label '{label}' but failed to parse: {match.group(1)}"
                )
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
