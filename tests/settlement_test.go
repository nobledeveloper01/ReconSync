package tests

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/provider"
)

// settlementDir writes files into a fresh directory and returns its path.
func settlementDir(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	return dir
}

func settlementProvider(t *testing.T, dir string, now time.Time) *provider.SettlementProvider {
	t.Helper()
	p, err := provider.NewSettlementProvider(provider.SettlementOptions{
		Name: "sterling",
		Dir:  dir,
		Columns: provider.SettlementColumns{
			Reference: "session_id",
			Amount:    "amount",
			Currency:  "currency",
			SettledAt: "settled_at",
			Status:    "status",
		},
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatalf("NewSettlementProvider: %v", err)
	}
	return p
}

var settleNow = time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)

const oneDayFile = `session_id,amount,currency,settled_at,status
TXN-OK,5000000,NGN,2026-08-12T10:15:00Z,successful
TXN-FAILED,5000000,NGN,2026-08-12T10:16:00Z,reversed
`

func TestSettlementConfirmsWhatTheFileSays(t *testing.T) {
	p := settlementProvider(t, settlementDir(t, map[string]string{"day1.csv": oneDayFile}), settleNow)
	ctx := context.Background()

	got, err := p.Query(ctx, provider.Ref{TransactionID: "TXN-OK", AmountMinor: 5_000_000})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled: %s", got.Outcome, got.Detail)
	}

	// A row the institution itself marks as not settled is the strongest
	// evidence the system can obtain — better than any inference from silence.
	got, err = p.Query(ctx, provider.Ref{TransactionID: "TXN-FAILED", AmountMinor: 5_000_000})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Failed {
		t.Errorf("outcome = %s, want failed: %s", got.Outcome, got.Detail)
	}
	if !strings.Contains(got.Detail, "reversed") {
		t.Errorf("detail = %q, want it to name the status in the file", got.Detail)
	}
}

// The entire difficulty of this adapter: absent because it failed, or absent
// because today's file has not arrived? Treating the two the same way would
// reverse every transaction on any day a file was late.
func TestSettlementAbsenceIsInconclusiveInsideTheGracePeriod(t *testing.T) {
	dir := settlementDir(t, map[string]string{"day1.csv": oneDayFile})
	ctx := context.Background()

	// Two hours after the file's coverage ends: well inside the grace period.
	soon := settlementProvider(t, dir, time.Date(2026, 8, 12, 12, 16, 0, 0, time.UTC))
	got, err := soon.Query(ctx, provider.Ref{TransactionID: "TXN-MISSING"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown while the next file may still be coming", got.Outcome)
	}
	if !strings.Contains(got.Detail, "grace") {
		t.Errorf("detail = %q, want it to explain the wait", got.Detail)
	}

	// Well past it, absence from a covering file is evidence.
	later := settlementProvider(t, dir, time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC))
	got, err = later.Query(ctx, provider.Ref{TransactionID: "TXN-MISSING"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.NotFound {
		t.Errorf("outcome = %s, want not_found once the grace period has passed", got.Outcome)
	}
}

// No files at all is not evidence of anything. A deployment whose SFTP drop is
// misconfigured must not reverse every transaction it sees.
func TestSettlementWithNoFilesIsAlwaysUnknown(t *testing.T) {
	p := settlementProvider(t, t.TempDir(), settleNow)

	got, err := p.Query(context.Background(), provider.Ref{TransactionID: "ANY"})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown with no files delivered", got.Outcome)
	}
	if !strings.Contains(got.Detail, "no settlement files") {
		t.Errorf("detail = %q, want it to say no files arrived", got.Detail)
	}
}

// A settlement for a different amount is not a settlement of this transaction.
func TestSettlementRefusesToMatchOnAmountMismatch(t *testing.T) {
	p := settlementProvider(t, settlementDir(t, map[string]string{"day1.csv": oneDayFile}), settleNow)

	got, err := p.Query(context.Background(), provider.Ref{
		TransactionID: "TXN-OK",
		AmountMinor:   9_999_999, // we recorded something else
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Unknown {
		t.Fatalf("outcome = %s, want unknown — a mismatched amount needs a human", got.Outcome)
	}
	if !strings.Contains(got.Detail, "5000000") {
		t.Errorf("detail = %q, want both amounts named", got.Detail)
	}
}

// A file arriving mid-morning must start answering immediately, not after the
// next deploy.
func TestSettlementPicksUpANewFileWithoutRestarting(t *testing.T) {
	dir := settlementDir(t, map[string]string{"day1.csv": oneDayFile})
	p := settlementProvider(t, dir, settleNow)
	ctx := context.Background()

	if got, _ := p.Query(ctx, provider.Ref{TransactionID: "TXN-LATE"}); got.Outcome == provider.Settled {
		t.Fatal("a transaction settled before its file existed")
	}

	if err := os.WriteFile(filepath.Join(dir, "day2.csv"), []byte(
		"session_id,amount,currency,settled_at,status\nTXN-LATE,5000000,NGN,2026-08-13T09:00:00Z,successful\n"),
		0o600); err != nil {
		t.Fatalf("write day2: %v", err)
	}

	got, err := p.Query(ctx, provider.Ref{TransactionID: "TXN-LATE", AmountMinor: 5_000_000})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled after the new file landed: %s", got.Outcome, got.Detail)
	}
}

// Institutions publish amounts both ways, and export files through Excel.
func TestSettlementReadsRealWorldFileShapes(t *testing.T) {
	dir := settlementDir(t, map[string]string{
		// A byte order mark, major-unit amounts with a thousands separator,
		// mixed-case references, and a blank trailing line.
		"messy.csv": "\ufeffSession_ID,Amount,Currency,Settled_At,Status\r\n" +
			"txn-major,\"50,000.00\",NGN,2026-08-12T10:15:00Z,SUCCESS\r\n" +
			"\r\n",
	})
	p, err := provider.NewSettlementProvider(provider.SettlementOptions{
		Name: "fidelity",
		Dir:  dir,
		Columns: provider.SettlementColumns{
			Reference: "session_id", Amount: "amount",
			Currency: "currency", SettledAt: "settled_at", Status: "status",
		},
		Now: func() time.Time { return settleNow },
	})
	if err != nil {
		t.Fatalf("NewSettlementProvider: %v", err)
	}

	// Reference matching is case-insensitive: their file says txn-major, our
	// ledger says TXN-MAJOR, and both mean the same transfer.
	got, err := p.Query(context.Background(), provider.Ref{
		TransactionID: "TXN-MAJOR",
		AmountMinor:   5_000_000, // 50,000.00 in minor units
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled: %s", got.Outcome, got.Detail)
	}
}

// The provider's own reference wins when we have it: their file is keyed by it.
func TestSettlementPrefersTheProviderReference(t *testing.T) {
	p := settlementProvider(t, settlementDir(t, map[string]string{"day1.csv": oneDayFile}), settleNow)

	got, err := p.Query(context.Background(), provider.Ref{
		TransactionID: "OUR-OWN-ID",
		ProviderRef:   "TXN-OK",
		AmountMinor:   5_000_000,
	})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled via the provider reference", got.Outcome)
	}
}

// A file whose reference column is missing is a configuration error, and
// starting anyway would answer Unknown forever while looking healthy.
func TestSettlementRejectsAFileItCannotRead(t *testing.T) {
	dir := settlementDir(t, map[string]string{
		"wrong.csv": "id,amount\nTXN-1,5000000\n",
	})
	_, err := provider.NewSettlementProvider(provider.SettlementOptions{
		Name:    "sterling",
		Dir:     dir,
		Columns: provider.SettlementColumns{Reference: "session_id"},
	})
	if err == nil {
		t.Fatal("accepted a file with no reference column")
	}
	if !strings.Contains(err.Error(), "session_id") {
		t.Errorf("error = %v, want it to name the missing column", err)
	}
}

// The adapter must satisfy the same contract as every other rail.
func TestSettlementSatisfiesTheProviderContract(t *testing.T) {
	p := settlementProvider(t, settlementDir(t, map[string]string{"day1.csv": oneDayFile}), settleNow)

	reg := provider.NewRegistry()
	if err := reg.Register(p); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got := reg.Query(context.Background(), provider.Ref{
		Provider: "sterling", TransactionID: "TXN-OK", AmountMinor: 5_000_000,
	})
	if got.Outcome != provider.Settled {
		t.Errorf("through the registry: %s (%s)", got.Outcome, got.Detail)
	}
}

// A settlement rail must be configurable from the same file as an HTTP one, or
// nobody can deploy it.
func TestSettlementRailLoadsFromConfig(t *testing.T) {
	dir := settlementDir(t, map[string]string{"day1.csv": oneDayFile})
	cfg := filepath.Join(t.TempDir(), "providers.json")
	body := `[{
		"name": "sterling",
		"kind": "settlement",
		"settlement": {
			"dir": ` + strconv.Quote(dir) + `,
			"reference_column": "session_id",
			"amount_column": "amount",
			"settled_at_column": "settled_at",
			"status_column": "status"
		}
	}]`
	if err := os.WriteFile(cfg, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	reg, err := provider.LoadRegistry(cfg)
	if err != nil {
		t.Fatalf("LoadRegistry: %v", err)
	}
	got := reg.Query(context.Background(), provider.Ref{
		Provider: "sterling", TransactionID: "TXN-OK", AmountMinor: 5_000_000,
	})
	if got.Outcome != provider.Settled {
		t.Errorf("outcome = %s, want settled: %s", got.Outcome, got.Detail)
	}

	// A settlement rail with no settlement block is a configuration error, not
	// a rail that quietly answers Unknown forever.
	bad := filepath.Join(t.TempDir(), "bad.json")
	if err := os.WriteFile(bad, []byte(`[{"name":"x","kind":"settlement"}]`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := provider.LoadRegistry(bad); err == nil {
		t.Error("accepted a settlement rail with no settlement block")
	}

	// And an unknown kind is named rather than silently treated as http.
	worse := filepath.Join(t.TempDir(), "worse.json")
	if err := os.WriteFile(worse, []byte(`[{"name":"x","kind":"carrier-pigeon"}]`), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	if _, err := provider.LoadRegistry(worse); err == nil || !strings.Contains(err.Error(), "carrier-pigeon") {
		t.Errorf("unknown kind = %v, want it named", err)
	}
}
