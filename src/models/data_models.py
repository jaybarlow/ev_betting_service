from typing import Optional
from datetime import datetime
from pydantic import BaseModel, ConfigDict
from src.models.enums import Bookmaker

class OddsData(BaseModel):
    """Represents a single odds point for a specific market from a bookmaker."""

    model_config = ConfigDict(frozen=True)  # Make instances immutable

    market_id: str  # Foreign key linking back to MarketData
    bookmaker: Bookmaker
    decimal_odds: float
    american_odds: int
    timestamp_collected: datetime
    outcome_description: Optional[str] = None  # Added field for Over/Under/Team desc.

    # Potential future fields:
    # probability: Optional[float] = None
    # is_best_odds: Optional[bool] = None
