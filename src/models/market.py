from datetime import datetime
from decimal import Decimal
from typing import Optional, List

from pydantic import BaseModel, Field

from .enums import MarketType, MarketSide, Period
from .odds import Odds


class Market(BaseModel):
    """Represents a specific betting market within a game."""

    game_id: str  # FK to the canonical Game.game_id
    market_type: MarketType
    period: Period = Period.FULL_GAME
    # Optional line/handicap, quantized to nearest 0.25 as per PRD 5.2
    line: Optional[Decimal] = None
    # Optional side identifier (e.g., HOME for Spread, OVER for Total)
    raw_market_name: Optional[str] = None
    last_updated_utc: datetime = Field(default_factory=datetime.utcnow)

    # Canonical market identifier generated during normalization
    # Based on game_id, type, period, line (as per PRD 5.2)
    market_id: Optional[str] = None

    # List to hold associated Odds objects
    odds: List[Odds] = []

    def __hash__(self):
        # Use market_id if available, otherwise fallback
        if self.market_id:
            return hash(self.market_id)
        # Fallback hash based on identifying info
        return hash(
            (
                self.game_id,
                self.market_type,
                self.period,
                # Use rounded line for hashing consistency
                round(self.line * 4) / 4 if self.line is not None else None,
            )
        )

    def __eq__(self, other):
        if not isinstance(other, Market):
            return NotImplemented
        if self.market_id and other.market_id:
            return self.market_id == other.market_id
        # Fallback equality
        line_eq = (self.line is None and other.line is None) or (
            self.line is not None
            and other.line is not None
            and abs(self.line - other.line)
            < Decimal("0.01")  # Tolerance for Decimal comparison
        )
        return (
            self.game_id == other.game_id
            and self.market_type == other.market_type
            and self.period == other.period
            and line_eq
        )
