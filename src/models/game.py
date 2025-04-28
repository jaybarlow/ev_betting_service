from datetime import datetime
from typing import Optional, List
import uuid

from pydantic import BaseModel, Field, computed_field

from .enums import Sport, League, Bookmaker
from .market import Market
from .team import Team


class Game(BaseModel):
    """Represents a single game or sporting event."""

    sport: Sport
    league: League
    start_time_utc: datetime
    home_team: Team
    away_team: Team
    bookmaker: Bookmaker
    raw_event_id: str
    last_updated_utc: datetime = Field(default_factory=datetime.utcnow)

    # Canonical game identifier generated during normalization
    # Based on league, canonical teams, and rounded start time (as per PRD 5.2)
    game_id: Optional[str] = None

    # Add the missing markets field
    markets: List[Market] = []

    @computed_field  # type: ignore[misc]
    @property
    def description(self) -> str:
        """A human-readable description of the game."""
        home = self.home_team.canonical_name or self.home_team.raw_name
        away = self.away_team.canonical_name or self.away_team.raw_name
        return f"{self.league}: {away} @ {home} ({self.start_time_utc.strftime('%Y-%m-%d %H:%M')} UTC)"

    def __hash__(self):
        # Use game_id if available, otherwise fallback to a combination of fields
        if self.game_id:
            return hash(self.game_id)
        # Fallback hash based on critical identifying info (immutable parts)
        return hash(
            (
                self.sport,
                self.league,
                self.home_team.raw_name,
                self.away_team.raw_name,
                # Round start time to nearest 5 minutes for hashing consistency (PRD 5.2)
                round(self.start_time_utc.timestamp() / 300),
            )
        )

    def __eq__(self, other):
        if not isinstance(other, Game):
            return NotImplemented
        if self.game_id and other.game_id:
            return self.game_id == other.game_id
        # Fallback equality check if game_id isn't set on both
        return (
            self.sport == other.sport
            and self.league == other.league
            and self.home_team.raw_name == other.home_team.raw_name
            and self.away_team.raw_name == other.away_team.raw_name
            and abs((self.start_time_utc - other.start_time_utc).total_seconds())
            <= 300  # Allow 5-minute tolerance (PRD 5.2)
        )
