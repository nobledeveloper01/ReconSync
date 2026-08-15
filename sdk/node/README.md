# @reconsync/sdk

Report transaction legs to ReconSync and verify its reversal webhooks. No
dependencies, Node 18 or later.

```bash
npm install @reconsync/sdk
```

Not published yet. Until it is, install from the repository:

```bash
npm install github:nobledeveloper01/ReconSync#main --workspace-root  # or vendor sdk/node
```

## Reporting

```ts
import { ReconSyncClient, Reporter } from "@reconsync/sdk";

const client = new ReconSyncClient("https://reconsync.internal", process.env.RECONSYNC_KEY!);

// Inside the transfer, use the reporter: it returns immediately and can never
// make a reconciliation outage into a failed payment.
const reporter = new Reporter(client, {
  onDrop: (kind, id) => metrics.increment("reconsync.dropped", { kind }),
});

reporter.reportDebit({
  transaction_id: transfer.id,
  transaction_type: "transfer",
  provider: "paystack",
  amount_minor: transfer.amountKobo,
  currency: "NGN",
  customer_ref: transfer.customerId,
});

// Later, when the rail answers. "unknown" is the honest answer to a timeout.
reporter.reportCredit({ transaction_id: transfer.id, status: "unknown" });

// On shutdown, or the queue is lost.
await reporter.close();
```

## Receiving

The raw body, not the parsed one — the signature covers the exact bytes.

```ts
import express from "express";
import { parseWebhook, SignatureError } from "@reconsync/sdk";

app.post(
  "/hooks/reconsync",
  express.raw({ type: "application/json" }), // not express.json()
  (req, res) => {
    let hook;
    try {
      hook = parseWebhook(process.env.RECONSYNC_WEBHOOK_SECRET!, req.get("x-reconsync-signature"), req.body);
    } catch (err) {
      if (err instanceof SignatureError) return res.sendStatus(401);
      throw err;
    }

    if (hook.data.drill) return res.sendStatus(200); // a fire drill: acknowledge only

    // Reverse what is outstanding, never the full amount: part of it may have
    // arrived, and refunding the whole thing pays the customer twice for that part.
    const reverseMinor = hook.data.outstanding_minor ?? hook.data.amount_minor;

    // Advisory. Check your own ledger before moving money.
    queue.push({ transactionId: hook.data.transaction_id, reverseMinor, confidence: hook.data.confidence });
    res.sendStatus(200);
  },
);
```

Answer 2xx quickly and do the work asynchronously. A slow handler is retried,
and a retried reversal advice is a duplicate to deduplicate.

## Testing your integration

`POST /v1/fire-drill` sends a synthetic reversal down the real path. It is the
only way to find out your handler is broken before an incident does.
