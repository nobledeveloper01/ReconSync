"""The Python client, checked against signatures the Go server actually made."""

from __future__ import annotations

import json
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from reconsync import SignatureError, parse_webhook, verify_signature  # noqa: E402

FIXTURES = json.loads(
    (pathlib.Path(__file__).resolve().parents[2] / "fixtures" / "signatures.json").read_text()
)
VALID = [f for f in FIXTURES if f["valid"]]
INVALID = [f for f in FIXTURES if not f["valid"]]


# This is the test that matters most here. An implementation can be
# self-consistent — signing and verifying its own output happily — and still
# reject every signature the server actually sends. Only a fixture from the
# other side catches that.
@pytest.mark.parametrize("fixture", VALID, ids=lambda f: f["name"])
def test_verifies_signatures_the_server_produced(fixture):
    verify_signature(
        fixture["secret"],
        fixture["header"],
        fixture["body"].encode("utf-8"),
        now=fixture["timestamp"],
    )


@pytest.mark.parametrize("fixture", INVALID, ids=lambda f: f["name"])
def test_rejects_what_the_server_would_reject(fixture):
    with pytest.raises(SignatureError):
        verify_signature(
            fixture["secret"],
            fixture["header"],
            fixture["body"].encode("utf-8"),
            now=fixture["timestamp"],
        )


def test_rejects_a_signature_outside_the_tolerance():
    fixture = VALID[0]

    # Replay of a captured request is bounded in both directions: a receiver's
    # clock can be behind as easily as ahead.
    for offset in (600, -600):
        with pytest.raises(SignatureError) as caught:
            verify_signature(
                fixture["secret"],
                fixture["header"],
                fixture["body"],
                now=fixture["timestamp"] + offset,
            )
        assert caught.value.reason == "expired"

    verify_signature(
        fixture["secret"], fixture["header"], fixture["body"], now=fixture["timestamp"] + 60
    )


def test_a_reserialised_body_does_not_verify():
    # The most common way a real integration breaks: verifying a body that was
    # parsed and dumped again. Same meaning, different bytes, and the signature
    # covers the bytes. Use request.get_data(), not request.get_json().
    fixture = next(f for f in FIXTURES if f["name"] == "spaced")
    round_tripped = json.dumps(json.loads(fixture["body"]))

    assert round_tripped != fixture["body"], "this fixture must not survive a round trip"
    verify_signature(fixture["secret"], fixture["header"], fixture["body"], now=fixture["timestamp"])

    with pytest.raises(SignatureError):
        verify_signature(
            fixture["secret"], fixture["header"], round_tripped, now=fixture["timestamp"]
        )


def test_multibyte_characters_survive_the_hash():
    # A body measured in characters rather than bytes hashes the wrong length,
    # and every Nigerian name with a diacritic would fail in production only.
    fixture = next(f for f in FIXTURES if f["name"] == "unicode")
    verify_signature(
        fixture["secret"],
        fixture["header"],
        fixture["body"].encode("utf-8"),
        now=fixture["timestamp"],
    )


def test_parse_webhook_verifies_before_it_parses():
    fixture = next(f for f in FIXTURES if f["name"] == "reversal")
    hook = parse_webhook(
        fixture["secret"], fixture["header"], fixture["body"], now=fixture["timestamp"]
    )

    assert hook.event == "reversal.triggered"
    assert hook.advisory is True
    assert hook.transaction_id == "TX-1"

    # A bad signature must not yield a parsed object a handler could act on.
    with pytest.raises(SignatureError):
        parse_webhook(fixture["secret"], "t=1,v1=deadbeef", fixture["body"], now=fixture["timestamp"])


def test_the_amount_to_reverse_is_what_is_outstanding():
    # Refunding amount_minor when part of the money already arrived pays the
    # customer twice for the part that arrived.
    from reconsync import Webhook

    partial = Webhook(
        event="reversal.triggered",
        occurred_at="",
        data={"amount_minor": 10_000, "credited_minor": 2_000, "outstanding_minor": 8_000},
        raw={},
    )
    assert partial.amount_to_reverse_minor == 8_000

    whole = Webhook(
        event="reversal.triggered", occurred_at="", data={"amount_minor": 10_000}, raw={}
    )
    assert whole.amount_to_reverse_minor == 10_000


def test_a_rotated_payload_verifies_with_either_secret():
    # Rotation, from the receiving side. A payload signed with two secrets must
    # verify with either one alone, or rotating breaks every receiver that has
    # not been redeployed yet — which is what rotation is meant to avoid.
    fixture = next(f for f in FIXTURES if f["name"] == "rotating")
    assert fixture["header"].count("v1=") == 2, "the fixture carries one signature"

    for secret in fixture["verify_with"]:
        verify_signature(secret, fixture["header"], fixture["body"], now=fixture["timestamp"])

    # Only those two. A second signature must not become a second way in.
    for secret in fixture["reject_with"]:
        with pytest.raises(SignatureError):
            verify_signature(secret, fixture["header"], fixture["body"], now=fixture["timestamp"])


def test_holding_several_secrets_verifies_a_single_secret_signature():
    # And from the sending side: a receiver holding both while the sender is
    # still on one. The two halves let each side change on its own schedule.
    fixture = next(f for f in FIXTURES if f["name"] == "reversal")

    verify_signature(
        ["whsec_something_old", fixture["secret"]],
        fixture["header"],
        fixture["body"],
        now=fixture["timestamp"],
    )

    with pytest.raises(SignatureError):
        verify_signature(
            ["whsec_something_old", "whsec_also_wrong"],
            fixture["header"],
            fixture["body"],
            now=fixture["timestamp"],
        )
