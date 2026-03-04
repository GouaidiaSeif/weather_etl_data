from dataclasses import dataclass
from typing import List


@dataclass
class Town:
    """Attributes:
        name: Town name 
        name_fr: Town name
        lat
        lon
        population
        department
    """
    name: str
    name_fr: str
    lat: float
    lon: float
    population: int
    department: str


FRENCH_TOWNS: List[Town] = [
    Town(
        name="paris",
        name_fr="Paris",
        lat=48.8566,
        lon=2.3522,
        population=2_165_000,
        department="Paris"
    ),
    Town(
        name="marseille",
        name_fr="Marseille",
        lat=43.2965,
        lon=5.3698,
        population=861_000,
        department="Bouches-du-Rhône"
    ),
    Town(
        name="lyon",
        name_fr="Lyon",
        lat=45.7640,
        lon=4.8357,
        population=515_000,
        department="Rhône"
    ),
    Town(
        name="toulouse",
        name_fr="Toulouse",
        lat=43.6047,
        lon=1.4442,
        population=493_000,
        department="Haute-Garonne"
    ),
    Town(
        name="nice",
        name_fr="Nice",
        lat=43.7102,
        lon=7.2620,
        population=343_000,
        department="Alpes-Maritimes"
    ),
    Town(
        name="nantes",
        name_fr="Nantes",
        lat=47.2184,
        lon=-1.5536,
        population=309_000,
        department="Loire-Atlantique"
    ),
    Town(
        name="montpellier",
        name_fr="Montpellier",
        lat=43.6108,
        lon=3.8767,
        population=285_000,
        department="Hérault"
    ),
    Town(
        name="strasbourg",
        name_fr="Strasbourg",
        lat=48.5734,
        lon=7.7521,
        population=280_000,
        department="Bas-Rhin"
    ),
    Town(
        name="bordeaux",
        name_fr="Bordeaux",
        lat=44.8378,
        lon=-0.5792,
        population=254_000,
        department="Gironde"
    ),
    Town(
        name="lille",
        name_fr="Lille",
        lat=50.6292,
        lon=3.0573,
        population=233_000,
        department="Nord"
    ),
]


TOWNS_BY_NAME = {town.name: town for town in FRENCH_TOWNS}


def get_town_by_name(name: str) -> Town:
    """Get a town by name.
    Args:
        name: Town name (case-insensitive)  
    Returns:
        Town: Town instance  
    Raises:
        KeyError: If town not found
    """
    return TOWNS_BY_NAME[name.lower()]


def get_towns_by_department(department: str) -> List[Town]:
    """Get all towns in a department.
    Args:
        department: Department name   
    Returns:
        List[Town]: List of towns in the department
    """
    return [town for town in FRENCH_TOWNS if town.department.lower() == department.lower()]
