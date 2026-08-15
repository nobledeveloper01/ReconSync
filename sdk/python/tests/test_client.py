"""The client and reporter, against a throwaway HTTP server."""

from __future__ import annotations

import json
import pathlib
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from reconsync import ApiError, Client, Reporter  # noqa: E402


class _Stub:
    """A server that answers however the test tells it to."""

    def __init__(self, respond):
        self.requests: list[dict] = []
        self.lock = threading.Lock()
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):  # noqa: N802 — the base class names it
                length = int(self.headers.get("Content-Length", 0))
                body = json.loads(self.rfile.read(length) or b"{}")
                with stub.lock:
                    stub.requests.append({"path": self.path, "body": body})
                    n = len(stub.requests)
                respond(self, n)

            def log_message(self, *args):
                pass

        self.server = HTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self) -> str:
        host, port = self.server.server_address
        return f"http://{host}:{port}"

    def close(self):
        self.server.shutdown()
        self.server.server_close()


@pytest.fixture
def stub():
    made: list[_Stub] = []

    def make(respond):
        s = _Stub(respond)
        made.append(s)
        return s

    yield make
    for s in made:
        s.close()


def test_refuses_a_configuration_it_cannot_use():
    with pytest.raises(ValueError):
        Client("", "k")
    with pytest.raises(ValueError):
        Client("reconsync.internal", "k")
    with pytest.raises(ValueError):
        Client("https://reconsync.internal", "")


def test_a_retried_debit_carries_the_same_idempotency_key(stub):
    # A retry that changed the key would register the same debit twice, and two
    # debits for one transfer is the double-count this product exists to prevent.
    def respond(handler, n):
        if n < 3:
            handler.send_response(503)
            handler.end_headers()
            return
        payload = json.dumps(
            {"status": "accepted", "transaction_id": "TX1", "window_seconds": 300}
        ).encode()
        handler.send_response(202)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    s = stub(respond)
    client = Client(s.url, "rs_test_key")
    accepted = client.report_debit(
        transaction_id="TX1",
        transaction_type="transfer",
        amount_minor=5000,
        currency="NGN",
        customer_ref="cust_1",
    )

    assert accepted["window_seconds"] == 300
    assert len(s.requests) == 3
    keys = {r["body"]["idempotency_key"] for r in s.requests}
    assert len(keys) == 1, "a retry changed the idempotency key"
    assert next(iter(keys)), "no idempotency key was sent"


def test_a_client_error_is_not_retried_and_names_the_field(stub):
    def respond(handler, n):
        payload = json.dumps(
            {"error": {"code": "invalid_request", "message": "is required", "field": "currency"}}
        ).encode()
        handler.send_response(400)
        handler.send_header("Content-Type", "application/json")
        handler.send_header("Content-Length", str(len(payload)))
        handler.end_headers()
        handler.wfile.write(payload)

    s = stub(respond)
    client = Client(s.url, "rs_test_key")

    with pytest.raises(ApiError) as caught:
        client.report_debit(
            transaction_id="TX1",
            transaction_type="transfer",
            amount_minor=1,
            currency="",
            customer_ref="c",
        )

    assert caught.value.field_name == "currency"
    assert not caught.value.retryable
    assert len(s.requests) == 1, "a 400 was retried"


def test_the_reporter_never_blocks_the_payment_path(stub):
    # The reason the Reporter exists: reporting must not be able to slow down or
    # fail the money movement it sits beside.
    release = threading.Event()

    def respond(handler, n):
        release.wait(timeout=10)  # the server is wedged, as it would be in an outage
        handler.send_response(202)
        handler.end_headers()

    s = stub(respond)
    dropped: list[str] = []
    reporter = Reporter(
        Client(s.url, "rs_test_key"),
        buffer_size=4,
        workers=1,
        on_drop=lambda kind, txn: dropped.append(txn),
    )

    started = time.monotonic()
    for i in range(500):
        reporter.report_debit(
            transaction_id=f"TX{i}",
            transaction_type="transfer",
            amount_minor=1000,
            currency="NGN",
            customer_ref="c",
        )
    elapsed = time.monotonic() - started

    assert elapsed < 1.0, f"500 reports took {elapsed:.2f}s against a wedged server"
    assert dropped, "nothing was dropped, so something must have waited"
    # And the drops are counted rather than silent: a hole in ReconSync's view
    # of your traffic looks exactly like a quiet day unless something says so.
    assert reporter.stats().dropped > 0

    release.set()


def test_closing_refuses_further_reports(stub):
    def respond(handler, n):
        handler.send_response(202)
        handler.end_headers()

    s = stub(respond)
    reporter = Reporter(Client(s.url, "rs_test_key"))
    reporter.report_debit(
        transaction_id="TX1",
        transaction_type="transfer",
        amount_minor=1,
        currency="NGN",
        customer_ref="c",
    )
    reporter.close(timeout=5.0)

    assert (
        reporter.report_debit(
            transaction_id="TX2",
            transaction_type="transfer",
            amount_minor=1,
            currency="NGN",
            customer_ref="c",
        )
        is False
    )


def test_bulk_refuses_more_than_the_server_accepts(stub):
    def respond(handler, n):
        raise AssertionError("the request reached the server; it should have been refused locally")

    s = stub(respond)
    client = Client(s.url, "rs_test_key")

    from reconsync import MAX_BULK_EVENTS

    with pytest.raises(ValueError):
        client.bulk([{"type": "debit"}] * (MAX_BULK_EVENTS + 1))
