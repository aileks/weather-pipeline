from pathlib import Path

from weather_pipeline.cities import load_cities

SEED = Path(__file__).parents[1] / "dbt/seeds/cities.csv"


def test_seed_loads_eight_cities_in_stable_order():
    cities = load_cities(SEED)

    assert [city.city_id for city in cities] == [
        "new-york",
        "london",
        "tokyo",
        "sydney",
        "sao-paulo",
        "cairo",
        "mumbai",
        "reykjavik",
    ]


def test_every_city_carries_request_coordinates_and_context():
    cities = load_cities(SEED)

    for city in cities:
        assert -90 <= city.latitude <= 90
        assert -180 <= city.longitude <= 180
        assert "/" in city.timezone
        assert city.climate_zone
