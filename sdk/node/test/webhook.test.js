import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { verifySignature, parseWebhook, SignatureError } from "../dist/index.js";

const fixtures = JSON.parse(
  readFileSync(fileURLToPath(new URL("../../fixtures/signatures.json", import.meta.url)), "utf8"),
);

// The fixtures are produced by the Go server that does the real signing.
//
// This is the test that matters most in this package: an implementation can be
// self-consistent — signing and verifying its own output happily — and still
// reject every signature the server actually sends. Only a fixture from the
// other side catches that.
test("verifies signatures produced by the server", () => {
  for (const f of fixtures.filter((f) => f.valid)) {
    verifySignature(f.secret, f.header, Buffer.from(f.body, "utf8"), {
      nowSeconds: f.timestamp,
    });
  }
});

test("rejects what the server would consider invalid", () => {
  for (const f of fixtures.filter((f) => !f.valid)) {
    assert.throws(
      () =>
        verifySignature(f.secret, f.header, Buffer.from(f.body, "utf8"), {
          nowSeconds: f.timestamp,
        }),
      SignatureError,
      `${f.name} was accepted (${f.why})`,
    );
  }
});

test("rejects a signature outside the tolerance", () => {
  const f = fixtures.find((f) => f.valid);

  // Replay of a captured request is bounded: the same bytes that verify now
  // stop verifying once they are old enough.
  assert.throws(
    () => verifySignature(f.secret, f.header, f.body, { nowSeconds: f.timestamp + 600 }),
    (err) => err instanceof SignatureError && err.reason === "expired",
  );

  // Both directions, because a receiver's clock can be behind as easily as ahead.
  assert.throws(
    () => verifySignature(f.secret, f.header, f.body, { nowSeconds: f.timestamp - 600 }),
    (err) => err instanceof SignatureError && err.reason === "expired",
  );

  // Inside it, the same bytes verify.
  verifySignature(f.secret, f.header, f.body, { nowSeconds: f.timestamp + 60 });
});

// The most common way a real integration breaks: parsing the body and
// re-serialising it before verifying. The signature covers the exact bytes.
test("a re-serialised body does not verify", () => {
  // This fixture carries the insignificant whitespace a proxy or a
  // pretty-printer leaves behind. Parsing and re-serialising produces the same
  // meaning and different bytes, and the signature covers the bytes — which is
  // why a receiver must keep the raw body. In Express that means
  // express.raw({ type: "application/json" }), not express.json().
  const f = fixtures.find((f) => f.name === "spaced");
  const round = JSON.stringify(JSON.parse(f.body));

  assert.notEqual(round, f.body, "this fixture must not survive a round trip, or it proves nothing");
  verifySignature(f.secret, f.header, f.body, { nowSeconds: f.timestamp });
  assert.throws(
    () => verifySignature(f.secret, f.header, round, { nowSeconds: f.timestamp }),
    SignatureError,
  );
});

test("multi-byte characters survive the hash", () => {
  const f = fixtures.find((f) => f.name === "unicode");
  // A body measured in characters rather than bytes hashes the wrong length,
  // and every Nigerian name with a diacritic would fail in production only.
  verifySignature(f.secret, f.header, Buffer.from(f.body, "utf8"), { nowSeconds: f.timestamp });
});

test("parseWebhook verifies before it parses", () => {
  const f = fixtures.find((f) => f.name === "reversal");
  const envelope = parseWebhook(f.secret, f.header, f.body, { nowSeconds: f.timestamp });

  assert.equal(envelope.event, "reversal.triggered");
  assert.equal(envelope.data.advisory, true);

  // A bad signature must not yield a parsed object a handler could act on.
  assert.throws(() => parseWebhook(f.secret, "t=1,v1=deadbeef", f.body, { nowSeconds: f.timestamp }));
});
