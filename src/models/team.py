# src/models/team.py
from typing import Optional
from pydantic import BaseModel


class Team(BaseModel):
    """Represents a team with raw and canonical names/IDs."""

    raw_name: str
    canonical_name: Optional[str] = None  # Determined during normalization
    team_id: Optional[str] = None  # Canonical team identifier
