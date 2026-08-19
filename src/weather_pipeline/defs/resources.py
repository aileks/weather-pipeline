"""Resources shared by Dagster assets."""

import httpx
from dagster import ConfigurableResource

from weather_pipeline.settings import Settings

REQUEST_TIMEOUT_SECONDS = 30.0


class OpenMeteoHttpResource(ConfigurableResource):
    """Provides the httpx client used to call the Open-Meteo archive API.

    Tests inject a client with a mock transport through the same seam.
    """

    base_url: str = ""

    def get_client(self) -> httpx.Client:
        base_url = self.base_url or Settings.from_env().open_meteo_base_url
        return httpx.Client(base_url=base_url, timeout=REQUEST_TIMEOUT_SECONDS)
