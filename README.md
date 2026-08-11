# ReconSync

Drop-in transaction reconciliation middleware for fintechs.

ReconSync watches a transaction stream for debits that never received a matching
credit confirmation, and fires a signed reversal webhook back to the customer's
system before the regulatory clock runs out.

**ReconSync never moves money.** It observes, detects, records and notifies; the
fintech's own system performs the reversal. The reversal webhook is advisory —
the customer's system verifies against its own ledger before acting. A total
compromise of ReconSync therefore cannot cause an unauthorised money movement.

## Status

Early build. Implemented so far:

| Component | State |
| --- | --- |
| Transaction state machine (§4.2) | Done, exhaustively tested |
| Card-data / PII screen (§8.4) | Done |
| Postgres schema + migrations (§5) | Done, applied and reverted against PG 16 |
| Store layer, tenant-scoped (§8.1) | Done — in-memory and Postgres, one shared conformance suite |
| Detection sweep with `SKIP LOCKED` (§4.4) | Done, verified across 5 concurrent schedulers |
| Ingest API, pipeline, dispatcher, SDKs, dashboard | Not started |

## Development

Requires Go 1.23+ and Postgres 16.

```bash
make test              # unit tests, race detector, no database
make test-integration  # full suite against a local Postgres
make ci                # what CI runs
```

Integration tests read `RECONSYNC_TEST_DATABASE_URL` and skip cleanly without
it, so `make test` works on a machine with no database.

```bash
make db-setup migrate-up    # create and migrate the local test database
make migrate-down           # roll it back
```

## Layout

```text
internal/domain/   transaction types + state machine (no infrastructure deps)
internal/store/    persistence port, in-memory and Postgres implementations
migrations/        schema, applied forward and backward in tests
```
