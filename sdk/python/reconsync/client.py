"""The client, and the reporter that keeps it off your payment path."""

from __future__ import annotations

import json
import queue
import random
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Literal

CreditStatus = Literal["success", "failed", "unknown"]

#: The server's ceiling on one bulk call.
MAX_BULK_EVENTS = 1000


class ApiError(Exception):
    """A refusal from the server, with the code and field it named."""

    def __init__(
        self,
        status_code: int,
        code: str,
        message: str,
        field_name: str | None = None,
        request_id: str | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.field_name = field_name
        self.request_id = request_id

    @property
    def retryable(self) -> bool:
        """Whether sending the same request again could succeed."""
        return self.status_code == 429 or self.status_code >= 500


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


class Client:
    """Reports transaction legs to ReconSync.

    The default timeout is deliberately short. This call sits beside a money
    movement, and a reconciliation service having a slow day must never become
    the reason a customer's transfer hangs.
    """

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 5.0,
        max_attempts: int = 3,
        user_agent: str = "reconsync-python",
    ) -> None:
        base_url = base_url.strip().rstrip("/")
        if not base_url:
            raise ValueError("reconsync: base URL is required")
        if not base_url.startswith(("http://", "https://")):
            raise ValueError(
                f"reconsync: base URL must start with http:// or https://, got {base_url!r}"
            )
        if not api_key:
            raise ValueError("reconsync: api key is required")

        self._base_url = base_url
        self._api_key = api_key
        self._timeout = timeout
        self._max_attempts = max(1, max_attempts)
        self._user_agent = user_agent

    def report_debit(
        self,
        *,
        transaction_id: str,
        transaction_type: str,
        amount_minor: int,
        currency: str,
        customer_ref: str,
        provider: str | None = None,
        debit_at: str | None = None,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
        expected_credit_minor: int | None = None,
        backfill: bool = False,
    ) -> dict[str, Any]:
        """Register an outgoing leg and return the window it was granted."""
        if not transaction_id:
            raise ValueError("reconsync: transaction_id is required")

        body: dict[str, Any] = {
            "transaction_id": transaction_id,
            # Derived rather than random, so a retry after a network timeout
            # cannot register the same debit twice.
            "idempotency_key": idempotency_key or f"debit-{transaction_id}",
            "transaction_type": transaction_type,
            "amount_minor": amount_minor,
            "currency": currency,
            "customer_ref": customer_ref,
            "debit_at": debit_at or _now_iso(),
        }
        if provider:
            body["provider"] = provider
        if metadata:
            body["metadata"] = metadata
        if expected_credit_minor:
            body["expected_credit_minor"] = expected_credit_minor
        if backfill:
            body["backfill"] = True

        return self._post("/v1/events/debit", body)

    def report_credit(
        self,
        *,
        transaction_id: str,
        status: CreditStatus,
        credit_at: str | None = None,
        provider_reference: str | None = None,
        idempotency_key: str | None = None,
        amount_minor: int | None = None,
        currency: str | None = None,
    ) -> None:
        """Record the verdict: success, failed, or the honest unknown."""
        if not transaction_id:
            raise ValueError("reconsync: transaction_id is required")
        if status not in ("success", "failed", "unknown"):
            raise ValueError("reconsync: status must be success, failed or unknown")

        body: dict[str, Any] = {
            "transaction_id": transaction_id,
            # Keyed on the verdict too: a transaction can legitimately go
            # unknown and then succeed, and those are two distinct events.
            "idempotency_key": idempotency_key or f"credit-{transaction_id}-{status}",
            "status": status,
            "credit_at": credit_at or _now_iso(),
        }
        if provider_reference:
            body["provider_reference"] = provider_reference
        if amount_minor is not None:
            body["amount_minor"] = amount_minor
        if currency:
            body["currency"] = currency

        self._post("/v1/events/credit", body)

    def report_reversal_completed(
        self, transaction_id: str, completed_at: str | None = None
    ) -> None:
        """Close the loop after you have reversed.

        Without this the transaction stays outstanding on the compliance report
        forever: ReconSync advised the reversal but never saw it happen.
        """
        self._post(
            "/v1/events/reversal-completed",
            {"transaction_id": transaction_id, "completed_at": completed_at or _now_iso()},
        )

    def bulk(self, events: list[dict[str, Any]]) -> dict[str, Any]:
        """Submit up to MAX_BULK_EVENTS at once, for backfill.

        Partial acceptance is the norm: valid events are taken and invalid ones
        listed by index, so one malformed row in a historical export does not
        reject the other nine hundred.
        """
        if not events:
            return {"accepted": 0}
        if len(events) > MAX_BULK_EVENTS:
            raise ValueError(
                f"reconsync: {len(events)} events exceeds the maximum of {MAX_BULK_EVENTS} per call"
            )
        return self._post("/v1/events/bulk", {"events": events})

    # --- transport ---

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        raw = json.dumps(body).encode("utf-8")
        last_error: Exception | None = None

        for attempt in range(1, self._max_attempts + 1):
            if attempt > 1:
                # Exponential with jitter: without jitter a fleet that all timed
                # out on the same server retries in lockstep and keeps it down.
                base = 0.1 * (2 ** (attempt - 2))
                time.sleep(base + random.random() * base * 0.25)

            try:
                return self._attempt(path, raw)
            except ApiError as exc:
                last_error = exc
                if not exc.retryable:
                    # A 400 says the request is wrong. Sending it again more
                    # slowly will not make it right.
                    raise
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                last_error = exc

        assert last_error is not None
        raise last_error

    def _attempt(self, path: str, raw: bytes) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self._base_url}{path}",
            data=raw,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "User-Agent": self._user_agent,
            },
        )

        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as response:
                payload = response.read()
                return json.loads(payload) if payload else {}
        except urllib.error.HTTPError as exc:
            raise _to_api_error(exc) from None


def _to_api_error(exc: urllib.error.HTTPError) -> ApiError:
    code = f"http_{exc.code}"
    message = exc.reason if isinstance(exc.reason, str) else f"HTTP {exc.code}"
    field_name = None
    request_id = exc.headers.get("X-Request-Id") if exc.headers else None

    try:
        # A proxy returning HTML is a real case, so a body that will not parse
        # leaves the status-derived message rather than masking what happened.
        parsed = json.loads(exc.read())
        error = parsed.get("error") or {}
        if error.get("code"):
            code = error["code"]
            message = error.get("message", message)
            field_name = error.get("field")
            request_id = error.get("request_id", request_id)
    except Exception:  # noqa: BLE001 — any parse failure means "no structured error"
        pass

    return ApiError(exc.code, code, message, field_name, request_id)


@dataclass
class ReporterStats:
    sent: int = 0
    failed: int = 0
    dropped: int = 0
    queued: int = 0


class Reporter:
    """Reports transactions without ever standing in the way of one.

    The naive integration — ``client.report_debit(...)`` inside the transfer —
    makes a reconciliation service having a bad afternoon into a reason the
    customer's payment fails. That is strictly worse than not reconciling it.
    So these methods return immediately and a full buffer drops the report.

    It does not drop silently. A dropped debit is a transaction ReconSync will
    never see and so can never detect the failure of, which is exactly the blind
    spot the server models as an ingest gap. Wire ``on_drop`` to an alert.
    """

    def __init__(
        self,
        client: Client,
        *,
        buffer_size: int = 1024,
        workers: int = 2,
        on_drop: Callable[[str, str], None] | None = None,
        on_error: Callable[[str, str, Exception], None] | None = None,
    ) -> None:
        self._client = client
        self._queue: queue.Queue[tuple[str, str, Callable[[], None]] | None] = queue.Queue(
            maxsize=buffer_size
        )
        self._on_drop = on_drop
        self._on_error = on_error
        self._stats = ReporterStats()
        self._lock = threading.Lock()
        self._closed = False

        self._threads = [
            threading.Thread(target=self._run, daemon=True, name=f"reconsync-reporter-{i}")
            for i in range(max(1, workers))
        ]
        for thread in self._threads:
            thread.start()

    def report_debit(self, **kwargs: Any) -> bool:
        transaction_id = str(kwargs.get("transaction_id", ""))
        return self._enqueue(
            "debit", transaction_id, lambda: self._client.report_debit(**kwargs)
        )

    def report_credit(self, **kwargs: Any) -> bool:
        transaction_id = str(kwargs.get("transaction_id", ""))
        return self._enqueue(
            "credit", transaction_id, lambda: self._client.report_credit(**kwargs)
        )

    def report_reversal_completed(
        self, transaction_id: str, completed_at: str | None = None
    ) -> bool:
        return self._enqueue(
            "reversal",
            transaction_id,
            lambda: self._client.report_reversal_completed(transaction_id, completed_at),
        )

    def stats(self) -> ReporterStats:
        with self._lock:
            return ReporterStats(
                sent=self._stats.sent,
                failed=self._stats.failed,
                dropped=self._stats.dropped,
                queued=self._queue.qsize(),
            )

    def close(self, timeout: float = 5.0) -> None:
        """Stop accepting reports and wait for the queue to drain.

        Raises :class:`TimeoutError` if anything is still queued. Silence there
        would mean a gap in the record at every rolling restart, which nobody
        would ever notice.
        """
        with self._lock:
            already = self._closed
            self._closed = True

        if not already:
            for _ in self._threads:
                self._queue.put(None)

        deadline = time.monotonic() + timeout
        for thread in self._threads:
            thread.join(timeout=max(0.0, deadline - time.monotonic()))

        remaining = self._queue.qsize()
        if remaining:
            raise TimeoutError(f"reconsync: closed with {remaining} reports still queued")

    def _enqueue(self, kind: str, transaction_id: str, task: Callable[[], Any]) -> bool:
        with self._lock:
            if self._closed:
                self._drop(kind, transaction_id)
                return False

        try:
            # Non-blocking on purpose: waiting here would apply backpressure
            # from a reconciliation service onto a payment path.
            self._queue.put_nowait((kind, transaction_id, task))
            return True
        except queue.Full:
            with self._lock:
                self._drop(kind, transaction_id)
            return False

    def _drop(self, kind: str, transaction_id: str) -> None:
        self._stats.dropped += 1
        if self._on_drop is not None:
            self._on_drop(kind, transaction_id)

    def _run(self) -> None:
        while True:
            item = self._queue.get()
            if item is None:
                return

            kind, transaction_id, task = item
            try:
                task()
                with self._lock:
                    self._stats.sent += 1
            except Exception as exc:  # noqa: BLE001 — a reporter must never raise into a worker
                with self._lock:
                    self._stats.failed += 1
                if self._on_error is not None:
                    self._on_error(kind, transaction_id, exc)
