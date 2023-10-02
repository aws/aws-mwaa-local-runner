import functools
import os

from airflow.models import Variable


class Config:
    def get(self, config_key, default=None):
        try:
            config = Variable.get(config_key)
        except KeyError:
            config = os.getenv(config_key)

        if config is None and default is not None:
            config = default

        return config

    @functools.lru_cache(maxsize=16)
    def cached_get(self, config_key, default=None):
        return self.get(config_key, default)
