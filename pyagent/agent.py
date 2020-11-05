import logging
from pyagent.utils.core import LOGGING_LEVEL

__all__ = ['Agent']


class Agent():
    def __init__(self):
        self.logger = logging.getLogger("Insight")
        self.logger.setLevel(LOGGING_LEVEL)