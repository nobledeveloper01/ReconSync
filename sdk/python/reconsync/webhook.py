"""Verifying a ReconSync webhook, which is the one thing every receiver must
get right.

The payload advises reversing a customer's money. A handler that acts on an
unverified body will act on anything anyone posts to that URL, and the whole
design — advisory payloads, no credentials in the webhook — rests on the
receiver checking who sent it.
"""

from __future__ import annotations

import hmac
import json
import time
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Literal

SIGNATURE_HEADER = "X-ReconSync-Signature"
EVENT_HEADER = "X-ReconSync-Event"
DELIVERY_HEADER = "X-ReconSync-Delivery"
DRILL_HEADER = "X-ReconSync-Drill"

#: How far a signature's timestamp may be from now.
DEFAULT_TOLERANCE_SECONDS = 300

Reason = Literal["malformed", "mismatch", "expired"]


class SignatureError(Exception):
    """The signature could not be trusted. Do not act on the payload."""

    def __init__(self, reason: Reason, message: str) -> None:
        super().__init__(message)
        self.reason: Reason = reason


def verify_signature(
    secret: str,
    header: str | None,
    body: bytes | str,
    *,
    now: float | None = None,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
) -> None:
    """Raise :class:`SignatureError` unless the signature is valid.

    ``body`` must be the **raw bytes as received**. Passing a re-serialised
    object is the most common way this fails in production: ``json.loads``
    followed by ``json.dumps`` changes separators and can reorder keys, and the
    signature covers the exact bytes that were sent. In Flask that means
    ``request.get_data()``, not ``request.get_json()``; in Django,
    ``request.body``.
    """
    timestamp, provided = _parse_header(header)

    if tolerance_seconds > 0:
        age = abs((time.time() if now is None else now) - timestamp)
        if age > tolerance_seconds:
            # Bounded replay: a request captured off the wire stops working
            # shortly after it was made.
            raise SignatureError(
                "expired",
                f"signature timestamp is {age:.0f}s away, outside the {tolerance_seconds}s tolerance",
            )

    raw = body.encode("utf-8") if isinstance(body, str) else body
    expected = hmac.new(
        secret.encode("utf-8"), f"{timestamp}.".encode("utf-8") + raw, sha256
    ).hexdigest()

    # compare_digest, not ==: a byte-by-byte comparison leaks how much of the
    # signature was correct, which is enough to forge one a byte at a time.
    if not hmac.compare_digest(expected, provided):
        raise SignatureError("mismatch", "signature does not match")


def _parse_header(header: str | None) -> tuple[int, str]:
    if not header:
        raise SignatureError("malformed", f"missing {SIGNATURE_HEADER} header")

    timestamp = 0
    signature = ""
    for part in header.split(","):
        key, sep, value = part.strip().partition("=")
        if not sep:
            raise SignatureError("malformed", f"cannot parse {SIGNATURE_HEADER}")
        if key == "t":
            try:
                timestamp = int(value)
            except ValueError as exc:
                raise SignatureError("malformed", "timestamp is not a number") from exc
        elif key == "v1":
            signature = value

    if not timestamp or not signature:
        raise SignatureError("malformed", f"{SIGNATURE_HEADER} is missing t or v1")
    return timestamp, signature


@dataclass(frozen=True)
class Webhook:
    """A verified webhook.

    The payload is kept as a plain dict as well as the fields worth naming,
    because events come in more than one shape and forcing every one through a
    single class would invent zero values for fields that do not apply.
    """

    event: str
    occurred_at: str
    data: dict[str, Any]
    raw: dict[str, Any]

    @property
    def transaction_id(self) -> str | None:
        return self.data.get("transaction_id")

    @property
    def advisory(self) -> bool:
        """Always true on a real event. Check your own ledger before paying."""
        return bool(self.data.get("advisory", False))

    @property
    def confidence(self) -> float:
        """0 to 1. Set your own bar rather than treating every verdict alike."""
        return float(self.data.get("confidence", 0.0))

    @property
    def is_drill(self) -> bool:
        """A fire drill. Acknowledge it and do nothing else."""
        return bool(self.data.get("drill", False))

    @property
    def amount_to_reverse_minor(self) -> int:
        """What is actually outstanding.

        Reverse this, never ``amount_minor``. When part of the money already
        reached the destination, refunding the full amount pays the customer
        twice for the part that arrived.
        """
        outstanding = self.data.get("outstanding_minor")
        if outstanding is not None:
            return int(outstanding)
        return int(self.data.get("amount_minor", 0))


def parse_webhook(
    secret: str,
    header: str | None,
    body: bytes | str,
    *,
    now: float | None = None,
    tolerance_seconds: int = DEFAULT_TOLERANCE_SECONDS,
) -> Webhook:
    """Verify and parse, in the order that keeps you safe."""
    verify_signature(secret, header, body, now=now, tolerance_seconds=tolerance_seconds)

    payload = json.loads(body)
    return Webhook(
        event=payload.get("event", ""),
        occurred_at=payload.get("occurred_at", ""),
        data=payload.get("data", {}),
        raw=payload,
    )
