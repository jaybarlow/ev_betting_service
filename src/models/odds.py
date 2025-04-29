from datetime import datetime
from decimal import Decimal
from typing import Optional, Any

from pydantic import BaseModel, Field, field_validator, computed_field

from .enums import Bookmaker, MarketSide


class Odds(BaseModel):
    """Represents the odds for a specific side of a market from a specific bookmaker."""

    market_id: str = Field(..., description="Foreign key linking to the Market object.")
    bookmaker: Bookmaker = Field(..., description="The bookmaker offering these odds.")
    side: MarketSide = Field(
        ...,
        description="Which side of the market this odds represents (e.g., Home, Away, Over, Under).",
    )
    points: Optional[Decimal] = Field(
        None,
        description="The point spread value associated with this side, if applicable (e.g., -7.5 for Home team).",
    )
    line: Optional[Decimal] = Field(
        None,
        description="The total line value associated with this side, if applicable (e.g., 210.5 for Over/Under).",
    )
    decimal_odds: Decimal = Field(..., description="The odds in decimal format.", gt=1)
    american_odds: Optional[int] = None  # Can be calculated/provided
    timestamp_collected: datetime = Field(default_factory=datetime.utcnow)

    @computed_field  # type: ignore[misc]
    @property
    def implied_probability(self) -> Decimal:
        """Calculate the implied probability from decimal odds."""
        if self.decimal_odds <= 0:
            return Decimal(0)
        return Decimal(1) / self.decimal_odds

    def model_post_init(self, __context: Any) -> None:
        # Calculate American odds if not provided
        if self.american_odds is None and self.decimal_odds > 0:
            if self.decimal_odds >= 2.0:
                self.american_odds = int((self.decimal_odds - 1) * 100)
            else:
                self.american_odds = int(-100 / (self.decimal_odds - 1))

    def __hash__(self):
        # Odds are mutable, hash based on identifier and book
        return hash((self.market_id, self.bookmaker))

    def __eq__(self, other):
        # Equality based on identifier and book, odds values can differ
        if not isinstance(other, Odds):
            return NotImplemented
        return self.market_id == other.market_id and self.bookmaker == other.bookmaker
