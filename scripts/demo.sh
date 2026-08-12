#!/usr/bin/env bash
#
# One command from nothing to a verified reversal webhook.
#
# Everything runs locally against the Postgres you already have. State goes in a
# throwaway database that is dropped on exit, so this never touches real data.
set -euo pipefail

DB=${DEMO_DB:-reconsync_demo}
SERVER_ADDR=${DEMO_SERVER_ADDR:-127.0.0.1:8410}
ECHO_ADDR=${DEMO_ECHO_ADDR:-127.0.0.1:8411}
WINDOW=${DEMO_WINDOW:-10}

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$(mktemp -d)"
SERVER_PID=""
ECHO_PID=""

cleanup() {
  [ -n "$ECHO_PID" ] && kill "$ECHO_PID" 2>/dev/null || true
  [ -n "$SERVER_PID" ] && kill -TERM "$SERVER_PID" 2>/dev/null || true
  wait 2>/dev/null || true
  dropdb --if-exists "$DB" 2>/dev/null || true
  rm -rf "$BIN"
}
trap cleanup EXIT

step() { printf '\n\033[1m%s\033[0m\n' "$*"; }
fail() { printf '\n\033[31m%s\033[0m\n' "$*" >&2; exit 1; }

# --- preflight -------------------------------------------------------------
step "1/6  Checking prerequisites"
command -v go >/dev/null 2>&1 || fail "go is not installed"
command -v psql >/dev/null 2>&1 || fail "psql is not installed"
pg_isready -q 2>/dev/null || fail "Postgres is not accepting connections. Start it and retry."
echo "      go, psql, postgres — ok"

# --- schema ----------------------------------------------------------------
step "2/6  Creating a throwaway database ($DB)"
dropdb --if-exists "$DB" 2>/dev/null || true
createdb "$DB"
for f in "$ROOT"/migrations/*.up.sql; do
  psql -q -v ON_ERROR_STOP=1 -d "$DB" -f "$f"
done
echo "      schema applied, dropped again on exit"

export RECONSYNC_DATABASE_URL="postgres://localhost:5432/$DB?sslmode=disable"
export RECONSYNC_TENANT_SALT="demo-salt-not-for-production"
export RECONSYNC_WEBHOOK_SECRET="whsec_demo_not_for_production"
export RECONSYNC_ADDR="$SERVER_ADDR"
# The receiver is on loopback, which the SSRF guard refuses by design.
export RECONSYNC_ALLOW_PRIVATE_WEBHOOK_TARGETS=true

# --- build -----------------------------------------------------------------
step "3/6  Building"
(cd "$ROOT" && go build -o "$BIN/reconsync" ./cmd/reconsync)
(cd "$ROOT" && go build -o "$BIN/reconsyncctl" ./cmd/reconsyncctl)
(cd "$ROOT" && go build -o "$BIN/reconsync-echo" ./cmd/reconsync-echo)
echo "      reconsync, reconsyncctl, reconsync-echo"

# --- seed ------------------------------------------------------------------
step "4/6  Setting up a tenant, key, endpoint and a ${WINDOW}s window"
"$BIN/reconsyncctl" tenant create --id tnt_demo --name "Demo Fintech" --env test >/dev/null
KEY=$("$BIN/reconsyncctl" keys create --tenant tnt_demo --env test | awk '/^secret:/{print $2}')
[ -n "$KEY" ] || fail "could not create an api key"

ECHO_ADDR="$ECHO_ADDR" "$BIN/reconsync-echo" &
ECHO_PID=$!
for _ in $(seq 1 40); do
  curl -sf "http://$ECHO_ADDR/healthz" >/dev/null 2>&1 && break
  sleep 0.25
done

"$BIN/reconsyncctl" endpoints create --tenant tnt_demo --id we_demo \
  --url "https://$ECHO_ADDR/hook" --allow-private >/dev/null
# The echo server speaks plain http on loopback; https is only required so the
# endpoint passes registration validation.
psql -q -d "$DB" -c "UPDATE webhook_endpoints SET url='http://$ECHO_ADDR/hook' WHERE id='we_demo';"

"$BIN/reconsyncctl" rules create --tenant tnt_demo --type transfer --window "$WINDOW" >/dev/null
echo "      tenant tnt_demo, endpoint we_demo, transfers reconcile in ${WINDOW}s"

# --- run -------------------------------------------------------------------
step "5/6  Starting ReconSync"
"$BIN/reconsync" >"$BIN/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 60); do
  curl -sf "http://$SERVER_ADDR/healthz" >/dev/null 2>&1 && break
  sleep 0.25
done
curl -sf "http://$SERVER_ADDR/healthz" >/dev/null 2>&1 || fail "server did not start. Log:$(printf '\n')$(cat "$BIN/server.log")"
echo "      listening on http://$SERVER_ADDR"

# --- the actual demo -------------------------------------------------------
step "6/6  Reporting two debits"
now=$(date -u +%Y-%m-%dT%H:%M:%SZ)

debit() {
  curl -s -o /dev/null -X POST "http://$SERVER_ADDR/v1/events/debit" \
    -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
    -H "Idempotency-Key: idem-$1" \
    -d "{\"transaction_id\":\"$1\",\"transaction_type\":\"transfer\",\"provider\":\"paystack\",\"amount_minor\":5000000,\"currency\":\"NGN\",\"debit_at\":\"$now\",\"customer_ref\":\"usr_ada\"}"
}

debit TXN-SETTLED
debit TXN-ORPHANED
echo "      TXN-SETTLED  — the credit will arrive"
echo "      TXN-ORPHANED — the credit never will"

curl -s -o /dev/null -X POST "http://$SERVER_ADDR/v1/events/credit" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: idem-credit' \
  -d "{\"transaction_id\":\"TXN-SETTLED\",\"status\":\"success\",\"credit_at\":\"$now\"}"

printf '\n      waiting %ss for the window to close' "$WINDOW"
deadline=$((SECONDS + WINDOW + 25))
delivered=""
while [ $SECONDS -lt $deadline ]; do
  delivered=$(psql -tA -d "$DB" -c \
    "SELECT status FROM webhook_deliveries WHERE transaction_id='TXN-ORPHANED' AND status='delivered'" 2>/dev/null || true)
  [ -n "$delivered" ] && break
  printf '.'
  sleep 1
done
printf '\n'

[ -n "$delivered" ] || fail "no webhook was delivered within the timeout. Server log:$(printf '\n')$(tail -20 "$BIN/server.log")"

step "Result"
psql -d "$DB" -c \
  "SELECT transaction_id, status, EXTRACT(EPOCH FROM (expected_completion_at - debit_at))::int AS window_s FROM transactions ORDER BY transaction_id;"
psql -d "$DB" -c \
  "SELECT transaction_id, event_type, status, response_code, attempt FROM webhook_deliveries;"

cat <<EOF

What just happened:

  TXN-SETTLED   its credit arrived inside the window, so it settled and no
                webhook was sent. Nothing to reverse.

  TXN-ORPHANED  no credit arrived. The detection sweep noticed at ${WINDOW}s,
                queued a reversal, and the dispatcher delivered it. The
                receiver above verified the signature before printing it.

The signature check in cmd/reconsync-echo/main.go is what your own handler
needs to do. The payload is marked "advisory": check your own ledger before
moving money.

Everything is torn down now. Nothing was left behind.
EOF
