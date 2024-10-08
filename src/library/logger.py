import logging
import sys

from constants.config import config

loggers: dict[str, logging.Logger | None] = {}


def getLogger(name="unknown"):
    global loggers

    if loggers.get(name):
        return loggers.get(name)
    else:
        level = logging.INFO
        if config["LOG_LEVEL"] == "debug":
            level = logging.DEBUG
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.setLevel(level)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        loggers[name] = logger

    return logger
