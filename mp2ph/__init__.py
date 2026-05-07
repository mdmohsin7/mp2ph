"""Stream Mixpanel raw-export JSONL into PostHog as a historical migration."""

from .transform import transform_event, MIXPANEL_INSERT_ID_NAMESPACE

__version__ = "0.1.0"
__all__ = ["transform_event", "MIXPANEL_INSERT_ID_NAMESPACE"]
