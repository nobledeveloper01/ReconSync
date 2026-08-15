import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";

import { ReconSyncClient, Reporter, ApiError } from "../dist/index.js";

/** Starts a throwaway server and returns a client pointed at it. */
async function stub(handler) {
  const requests = [];
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      requests.push({ url: req.url, body: body ? JSON.parse(body) : null, headers: req.headers });
      handler(req, res, requests.length);
    });
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();

  return {
    client: new ReconSyncClient(`http://127.0.0.1:${port}`, "rs_test_key"),
    requests,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

test("refuses a configuration it cannot use", () => {
  assert.throws(() => new ReconSyncClient("", "k"));
  assert.throws(() => new ReconSyncClient("reconsync.internal", "k"));
  assert.throws(() => new ReconSyncClient("https://reconsync.internal", ""));
});

// A retry that changed the key would register the same debit twice, and two
// debits for one transfer is the double-count this product exists to prevent.
test("a retried debit carries the same idempotency key", async () => {
  const s = await stub((req, res, n) => {
    if (n < 3) {
      res.writeHead(503).end();
      return;
    }
    res.writeHead(202, { "content-type": "application/json" });
    res.end(JSON.stringify({ status: "accepted", transaction_id: "TX1", window_seconds: 300 }));
  });

  try {
    const accepted = await s.client.reportDebit({
      transaction_id: "TX1",
      transaction_type: "transfer",
      amount_minor: 5000,
      currency: "NGN",
      customer_ref: "cust_1",
    });
    assert.equal(accepted.window_seconds, 300);

    assert.equal(s.requests.length, 3);
    const keys = new Set(s.requests.map((r) => r.body.idempotency_key));
    assert.equal(keys.size, 1, "a retry changed the idempotency key");
    assert.ok([...keys][0], "no idempotency key was sent at all");
  } finally {
    await s.close();
  }
});

test("a client error is not retried, and names the field", async () => {
  const s = await stub((req, res) => {
    res.writeHead(400, { "content-type": "application/json" });
    res.end(
      JSON.stringify({ error: { code: "invalid_request", message: "is required", field: "currency" } }),
    );
  });

  try {
    await assert.rejects(
      () => s.client.reportDebit({ transaction_id: "TX1", transaction_type: "transfer", amount_minor: 1, currency: "", customer_ref: "c" }),
      (err) => err instanceof ApiError && err.field === "currency" && !err.retryable,
    );
    assert.equal(s.requests.length, 1, "a 400 was retried");
  } finally {
    await s.close();
  }
});

// The reason the Reporter exists: reporting must not be able to slow down or
// fail the money movement it sits beside.
test("the reporter never blocks the payment path", async () => {
  let release;
  const held = new Promise((resolve) => (release = resolve));

  const s = await stub(async (req, res) => {
    await held; // the server is wedged, as it would be in an outage
    res.writeHead(202).end();
  });

  try {
    let dropped = 0;
    const reporter = new Reporter(s.client, {
      bufferSize: 4,
      onDrop: () => dropped++,
    });

    const started = Date.now();
    for (let i = 0; i < 500; i++) {
      reporter.reportDebit({
        transaction_id: `TX${i}`,
        transaction_type: "transfer",
        amount_minor: 1000,
        currency: "NGN",
        customer_ref: "c",
      });
    }
    const elapsed = Date.now() - started;

    assert.ok(elapsed < 1000, `500 reports took ${elapsed}ms against a wedged server`);
    assert.ok(dropped > 0, "nothing was dropped, so something must have waited");
    assert.ok(reporter.getStats().dropped > 0, "drops are not counted");

    release();
  } finally {
    await s.close();
  }
});

test("closing refuses further reports rather than losing them silently", async () => {
  const s = await stub((req, res) => res.writeHead(202).end());
  try {
    const reporter = new Reporter(s.client);
    reporter.reportDebit({
      transaction_id: "TX1",
      transaction_type: "transfer",
      amount_minor: 1,
      currency: "NGN",
      customer_ref: "c",
    });
    await reporter.close(2000);

    assert.equal(
      reporter.reportDebit({ transaction_id: "TX2", transaction_type: "transfer", amount_minor: 1, currency: "NGN", customer_ref: "c" }),
      false,
    );
  } finally {
    await s.close();
  }
});
