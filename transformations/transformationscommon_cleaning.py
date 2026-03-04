"""
Common cleaning utilities.
"""

def normalize_city_name(city: str) -> str:
    return city.strip().lower().replace(" ", "_")


def ensure_numeric(value, default=0):
    try:
        return float(value)
    except (ValueError, TypeError):
        return default
