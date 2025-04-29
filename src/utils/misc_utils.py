# src/utils/misc_utils.py
import re
import hashlib


def generate_canonical_id(*args: str) -> str:
    """Generates a consistent, URL-safe ID from one or more strings."""
    combined = "_".join(str(arg).lower() for arg in args if arg)
    # Remove non-alphanumeric characters (except underscore)
    safe_string = re.sub(r"[^\w]+", "", combined.replace(" ", "_"))
    # Use a hash for uniqueness if the string gets too long or complex,
    # but short, clean strings can be used directly.
    if len(safe_string) > 100:  # Increased length limit to 100
        return hashlib.sha1(safe_string.encode()).hexdigest()[:16]  # Short hash
    return safe_string
