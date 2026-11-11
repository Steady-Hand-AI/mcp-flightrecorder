"""An SDK-independent capture core. Optional integrations live in their own modules."""
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())

from ._recorder import Recorder
from ._types import RecorderError
from ._values import MISSING

__version__ = "0.5.23"
__all__ = ["Recorder", "RecorderError", "MISSING"]
