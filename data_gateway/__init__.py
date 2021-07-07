import os

from . import exceptions


__all__ = ("exceptions",)


os.environ["USE_OCTUE_LOG_HANDLER"] = "1"
