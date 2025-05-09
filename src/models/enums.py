from enum import Enum


class Sport(str, Enum):
    NBA = "NBA"
    NHL = "NHL"
    MLB = "MLB"
    NFL = "NFL"
    NCAAF = "NCAAF"
    WNBA = "WNBA"
    MLS = "MLS"
    SOCCER = "Soccer"
    TENNIS = "Tennis"
    # Add more sports as needed


class League(str, Enum):
    NBA = "NBA"
    NHL = "NHL"
    MLB = "MLB"
    # Add specific league variations if necessary (e.g., NCAA)


class MarketType(str, Enum):
    MONEYLINE = "MONEYLINE"
    SPREAD = "SPREAD"  # Covers Point Spread, Run Line, Puck Line
    TOTAL = "TOTAL"  # Covers Over/Under
    # Add more market types later (e.g., PLAYER_PROP_POINTS)


class MarketSide(str, Enum):
    HOME = "HOME"
    AWAY = "AWAY"
    OVER = "OVER"
    UNDER = "UNDER"
    # Add other potential sides if needed


class Period(str, Enum):
    FULL_GAME = "FULL_GAME"
    FIRST_HALF = "FIRST_HALF"
    # Add more periods as needed


class Bookmaker(str, Enum):
    PINNACLE = "PINNACLE"
    CRAB_SPORTS = "CRAB_SPORTS"
    UNKNOWN = "UNKNOWN"
    TBD_BOOK = "TBD_BOOK"  # Placeholder
    # Add other bookmakers as needed
