from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import BaseModel, Field

from .enums import Bookmaker


class OddsComparison(BaseModel):
    """Stores the result of comparing odds between a target book and a sharp book."""

    # Identifiers
    comparison_id: Optional[str] = (
        None  # Unique ID for this comparison instance (e.g., UUID)
    )
    game_id: str  # FK to Game.game_id
    market_id: str  # FK to Market.market_id

    # Bookmakers involved
    target_book: Bookmaker
    sharp_book: Bookmaker = Bookmaker.PINNACLE  # Default sharp book for MVP

    # Odds data
    target_odds_decimal: Decimal
    sharp_odds_decimal: Decimal  # Sharp odds for the *same* outcome
    # Might also include odds for the other side of the market from sharp book
    sharp_odds_decimal_other_side: Optional[Decimal] = None

    # Calculated values
    sharp_prob_no_vig: Optional[Decimal] = None  # Vig-free probability from sharp book
    ev_percent: Optional[Decimal] = None  # Expected Value percentage
    kelly_fraction: Optional[Decimal] = None  # Kelly Criterion fraction

    # Metadata
    timestamp_compared: datetime = Field(default_factory=datetime.utcnow)
    timestamp_target_odds_collected: datetime
    timestamp_sharp_odds_collected: datetime

    # Optional field for database primary key if needed
    # db_id: Optional[int] = None

    class Config:
        # Allow ORM mode if interacting with SQLAlchemy or similar
        orm_mode = True
