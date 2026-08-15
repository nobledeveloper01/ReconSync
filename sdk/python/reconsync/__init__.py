"""The ReconSync client library.

Standard library only, so adding it to a payment service brings in nothing to
review. Two things live here: reporting transaction legs, and verifying the
webhook that may come back.
"""

from reconsync.client import (
    MAX_BULK_EVENTS,
    ApiError,
    Client,
    CreditStatus,
    Reporter,
    ReporterStats,
)
from reconsync.webhook import (
    DEFAULT_TOLERANCE_SECONDS,
    DELIVERY_HEADER,
    DRILL_HEADER,
    EVENT_HEADER,
    SIGNATURE_HEADER,
    SignatureError,
    Webhook,
    parse_webhook,
    verify_signature,
)

__all__ = [
    "ApiError",
    "Client",
    "CreditStatus",
    "MAX_BULK_EVENTS",
    "Reporter",
    "ReporterStats",
    "DEFAULT_TOLERANCE_SECONDS",
    "DELIVERY_HEADER",
    "DRILL_HEADER",
    "EVENT_HEADER",
    "SIGNATURE_HEADER",
    "SignatureError",
    "Webhook",
    "parse_webhook",
    "verify_signature",
]

__version__ = "0.1.0"
