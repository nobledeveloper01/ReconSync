# Client libraries

Three, for the stacks a transaction service is usually written in. All of them
do the same two things and make the same two promises.

| | Package | Install |
| --- | --- | --- |
| Go | `github.com/nobledeveloper01/ReconSync/pkg/reconsync` | `go get github.com/nobledeveloper01/ReconSync` |
| Node | [`sdk/node`](node/) | `npm install @reconsync/sdk` — *not published yet* |
| Python | [`sdk/python`](python/) | `pip install reconsync` — *not published yet* |

The Go package works today. Node and Python can be installed from this
repository until they are published; each README says how.

**No dependencies.** Every one is standard library only. This code goes into a
payment service, where each package added is one a security team has to approve.

**They agree with each other.** The signature is the place where three
implementations could quietly diverge, so [`fixtures/signatures.json`](fixtures/)
holds signatures produced by the Go server, and all three SDKs verify against
them in their own test suites. An implementation that only agrees with itself
would sign and verify its own output happily while rejecting every signature the
server actually sends.

## Publishing

`.github/workflows/release-sdks.yml` publishes both packages when a tag matching
`sdk-v*` is pushed, and refuses if the tag and the manifests disagree about the
version. It needs two things set up once:

- an `NPM_TOKEN` repository secret with publish rights to the `@reconsync` scope
- PyPI trusted publishing configured for this repository, so no token is stored

Run it with `workflow_dispatch` first: that builds and checks both packages
without publishing anything. Publishing is close to irreversible — npm refuses
to unpublish after 72 hours and PyPI never lets a version number be reused — so
it is deliberately not something a merge to main can cause.

## The two things

### Report the legs

```go
client.ReportDebit(ctx, reconsync.Debit{...})   // money left
client.ReportCredit(ctx, reconsync.Credit{...}) // success, failed, or unknown
```

`unknown` is the case this product exists for. Report it honestly — a timeout is
not a failure and not a success, and guessing either way is how customers get
paid twice or not at all.

### Verify the webhook, then act

```go
if err := reconsync.Verify(secret, header, rawBody, time.Now(), reconsync.DefaultTolerance); err != nil {
    return  // do not act on it
}
```

Verify **before** parsing, and verify the **raw bytes as received**. Parsing and
re-serialising changes whitespace and can reorder keys, and the signature covers
the bytes. In Express that means `express.raw({ type: "application/json" })`, not
`express.json()`; in Flask, `request.get_data()`, not `request.get_json()`.

## Do not let this fail a payment

The naive integration is the dangerous one:

```go
// Don't. A reconciliation service having a bad afternoon now fails a transfer.
if err := client.ReportDebit(ctx, debit); err != nil {
    return err
}
```

Every SDK ships a `Reporter` that queues instead: enqueuing never blocks, never
returns an error, and a full buffer drops the report rather than waiting.

```go
reporter := reconsync.NewReporter(client,
    reconsync.OnDrop(func(kind, id string) { metrics.Inc("reconsync_dropped") }))
defer reporter.Close(ctx)

reporter.ReportDebit(debit) // returns immediately, always
```

Wire `OnDrop` to an alert. A dropped debit is a transaction ReconSync will never
see and therefore can never detect the failure of, and a hole in the record looks
exactly like a quiet day unless something says otherwise. That is the same blind
spot the server models as an ingest gap, arriving from your side instead.

## Rotating the signing secret

While ReconSync rotates, the header carries a `v1=` per secret and every SDK
verifies against all of them — so a receiver holding either the old secret or the
new one keeps working, and you can redeploy whenever suits you.

If you would rather move first, every `verify` also takes a list:

```ts
verifySignature([process.env.OLD_SECRET!, process.env.NEW_SECRET!], header, rawBody);
```

```python
verify_signature([old_secret, new_secret], header, raw_body)
```

```go
reconsync.VerifyAny([]string{oldSecret, newSecret}, header, rawBody, time.Now(), reconsync.DefaultTolerance)
```

## What the payload is telling you

- `advisory` is always `true`. Check your own ledger before moving money. A
  compromise of ReconSync must not be able to cause a payment.
- `confidence` is 0 to 1. Set your own bar — auto-reverse above it, queue for a
  human below — rather than treating every verdict as equally certain.
- **Reverse `outstanding_minor`, not `amount_minor`**, whenever it is present.
  Part of the money already arrived; refunding the whole amount pays the
  customer twice for the part that did.
- `drill: true` is a fire drill. Acknowledge it and do nothing else.
