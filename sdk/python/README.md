# reconsync

Report transaction legs to ReconSync and verify its reversal webhooks. Standard
library only, Python 3.10 or later.

```bash
pip install reconsync
```

Not published yet. Until it is, install from the repository:

```bash
pip install "git+https://github.com/nobledeveloper01/ReconSync.git#subdirectory=sdk/python"
```

## Reporting

```python
from reconsync import Client, Reporter

client = Client("https://reconsync.internal", os.environ["RECONSYNC_KEY"])

# Inside the transfer, use the reporter: it returns immediately and can never
# make a reconciliation outage into a failed payment.
reporter = Reporter(client, on_drop=lambda kind, txn: statsd.increment("reconsync.dropped"))

reporter.report_debit(
    transaction_id=transfer.id,
    transaction_type="transfer",
    provider="paystack",
    amount_minor=transfer.amount_kobo,
    currency="NGN",
    customer_ref=transfer.customer_id,
)

# Later, when the rail answers. "unknown" is the honest answer to a timeout.
reporter.report_credit(transaction_id=transfer.id, status="unknown")

# On shutdown, or the queue is lost.
reporter.close()
```

## Receiving

The raw body, not the parsed one — the signature covers the exact bytes.

```python
from flask import Flask, request
from reconsync import SIGNATURE_HEADER, SignatureError, parse_webhook

@app.post("/hooks/reconsync")
def reconsync_hook():
    try:
        hook = parse_webhook(
            os.environ["RECONSYNC_WEBHOOK_SECRET"],
            request.headers.get(SIGNATURE_HEADER),
            request.get_data(),  # not request.get_json()
        )
    except SignatureError:
        return "", 401

    if hook.is_drill:
        return "", 200  # a fire drill: acknowledge only

    # amount_to_reverse_minor is what is outstanding, which is not always the
    # full amount: part may have arrived, and refunding all of it would pay the
    # customer twice for that part.
    queue.push(
        transaction_id=hook.transaction_id,
        reverse_minor=hook.amount_to_reverse_minor,
        confidence=hook.confidence,   # advisory: check your own ledger first
    )
    return "", 200
```

Answer 2xx quickly and do the work asynchronously. A slow handler is retried,
and a retried reversal advice is a duplicate to deduplicate.

## Testing your integration

`POST /v1/fire-drill` sends a synthetic reversal down the real path. It is the
only way to find out your handler is broken before an incident does.
