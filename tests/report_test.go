package tests

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
	"github.com/nobledeveloper01/ReconSync/internal/report"
)

var reportNow = time.Date(2026, 8, 12, 12, 0, 0, 0, time.UTC)

// reversed builds a transaction that was detected and reversed, taking the given
// time from debit to confirmation.
func reversed(id string, debitAgo, detectAfter, reverseAfter time.Duration) *domain.Transaction {
	debitAt := reportNow.Add(-debitAgo)
	detected := debitAt.Add(detectAfter)
	completed := debitAt.Add(reverseAfter)
	return &domain.Transaction{
		TransactionID:        id,
		AmountMinor:          5_000_000,
		Currency:             "NGN",
		Status:               domain.StatusReversalCompleted,
		DebitAt:              debitAt,
		ExpectedCompletionAt: debitAt.Add(5 * time.Minute),
		DetectedAt:           &detected,
		ReversalCompletedAt:  &completed,
	}
}

func reportInput(txns ...*domain.Transaction) report.Input {
	return report.Input{
		TenantID:       tenantA,
		From:           reportNow.AddDate(0, 0, -30),
		To:             reportNow,
		CountsByStatus: map[domain.Status]int{},
		Reversals:      txns,
	}
}

func TestComplianceCountsWithinAndBreached(t *testing.T) {
	in := reportInput(
		reversed("FAST", 48*time.Hour, 6*time.Minute, 30*time.Minute),
		reversed("OK", 48*time.Hour, 6*time.Minute, 20*time.Hour),
		reversed("LATE", 48*time.Hour, 6*time.Minute, 30*time.Hour),
	)

	r := report.Compute(in, 24*time.Hour, reportNow)
	if r.Compliance.WithinDeadline != 2 {
		t.Errorf("within = %d, want 2", r.Compliance.WithinDeadline)
	}
	if r.Compliance.Breached != 1 {
		t.Errorf("breached = %d, want 1", r.Compliance.Breached)
	}
	if r.Compliance.Rate == nil || *r.Compliance.Rate < 0.66 || *r.Compliance.Rate > 0.67 {
		t.Errorf("rate = %v, want ~0.667", r.Compliance.Rate)
	}
	if len(r.Breaches) != 1 || r.Breaches[0].TransactionID != "LATE" {
		t.Fatalf("breaches = %v, want [LATE]", r.Breaches)
	}
	if r.Breaches[0].Reason == "" {
		t.Error("breach has no reason")
	}
}

// A reversal still in flight is neither compliant nor breached. Folding it into
// either would misstate the position to a regulator.
func TestComplianceSeparatesOutstandingFromBreached(t *testing.T) {
	inFlight := reversed("PENDING", time.Hour, 6*time.Minute, 0)
	inFlight.Status = domain.StatusReversalPending
	inFlight.ReversalCompletedAt = nil

	r := report.Compute(reportInput(inFlight), 24*time.Hour, reportNow)
	if r.Compliance.Outstanding != 1 {
		t.Errorf("outstanding = %d, want 1", r.Compliance.Outstanding)
	}
	if r.Compliance.Breached != 0 || r.Compliance.WithinDeadline != 0 {
		t.Errorf("an in-flight reversal was counted as concluded: %+v", r.Compliance)
	}
	// With nothing concluded there is no rate to state. 0%% and "no data" are
	// different claims.
	if r.Compliance.Rate != nil {
		t.Errorf("rate = %v, want none when nothing has concluded", *r.Compliance.Rate)
	}
}

// Past the deadline with no confirmation, "still in flight" is no longer an
// honest description.
func TestComplianceCountsOverdueInFlightAsBreached(t *testing.T) {
	overdue := reversed("OVERDUE", 48*time.Hour, 6*time.Minute, 0)
	overdue.Status = domain.StatusReversalPending
	overdue.ReversalCompletedAt = nil

	r := report.Compute(reportInput(overdue), 24*time.Hour, reportNow)
	if r.Compliance.Breached != 1 {
		t.Errorf("breached = %d, want 1 — the deadline passed with no confirmation", r.Compliance.Breached)
	}
	if r.Compliance.Outstanding != 0 {
		t.Errorf("outstanding = %d, want 0", r.Compliance.Outstanding)
	}
}

// A dead-lettered reversal means nobody acted, whatever the clock says.
func TestComplianceCountsDeadLetteredAsBreached(t *testing.T) {
	failed := reversed("DEAD", time.Hour, 6*time.Minute, 0)
	failed.Status = domain.StatusReversalFailed
	failed.ReversalCompletedAt = nil

	r := report.Compute(reportInput(failed), 24*time.Hour, reportNow)
	if r.Compliance.Breached != 1 {
		t.Fatalf("breached = %d, want 1", r.Compliance.Breached)
	}
	if len(r.Breaches) != 1 {
		t.Fatalf("breaches = %v", r.Breaches)
	}
	// Even though only an hour has passed, well inside the deadline.
	if r.Breaches[0].Status != string(domain.StatusReversalFailed) {
		t.Errorf("breach status = %s", r.Breaches[0].Status)
	}
}

func TestComplianceLatencies(t *testing.T) {
	var txns []*domain.Transaction
	// Detection at 1s..10s after the window closed.
	for i := 1; i <= 10; i++ {
		txn := reversed("TX", 48*time.Hour, 5*time.Minute+time.Duration(i)*time.Second, 2*time.Hour)
		txns = append(txns, txn)
	}

	r := report.Compute(reportInput(txns...), 24*time.Hour, reportNow)
	if r.Detection.Samples != 10 {
		t.Fatalf("detection samples = %d, want 10", r.Detection.Samples)
	}
	if r.Detection.Max != 10 {
		t.Errorf("detection max = %v, want 10s", r.Detection.Max)
	}
	if r.Detection.P50 < 4 || r.Detection.P50 > 6 {
		t.Errorf("detection p50 = %v, want ~5s", r.Detection.P50)
	}
	if r.Reversal.Samples != 10 {
		t.Errorf("reversal samples = %d, want 10", r.Reversal.Samples)
	}
}

func TestComplianceEmptyPeriod(t *testing.T) {
	r := report.Compute(reportInput(), 24*time.Hour, reportNow)
	if r.Totals.OrphansDetected != 0 || len(r.Breaches) != 0 {
		t.Errorf("empty period produced %+v", r)
	}
	if r.Compliance.Rate != nil {
		t.Error("empty period claimed a compliance rate")
	}
	if r.Detection.Samples != 0 {
		t.Error("empty period reported latency samples")
	}
}

func TestComplianceTotalsFromCounts(t *testing.T) {
	in := reportInput(reversed("TX", 48*time.Hour, 6*time.Minute, 2*time.Hour))
	in.CountsByStatus = map[domain.Status]int{
		domain.StatusCompleted:         900,
		domain.StatusPendingDebit:      50,
		domain.StatusSuspect:           7,
		domain.StatusReversalCompleted: 1,
	}

	r := report.Compute(in, 24*time.Hour, reportNow)
	if r.Totals.Transactions != 958 {
		t.Errorf("transactions = %d, want 958", r.Totals.Transactions)
	}
	if r.Totals.Settled != 900 {
		t.Errorf("settled = %d, want 900", r.Totals.Settled)
	}
	if r.Totals.AwaitingInvestigation != 7 {
		t.Errorf("awaiting investigation = %d, want 7", r.Totals.AwaitingInvestigation)
	}
	if r.Totals.StillOpen != 50 {
		t.Errorf("still open = %d, want 50", r.Totals.StillOpen)
	}
}

// Worst first: an auditor reads the top of the list.
func TestComplianceBreachesAreWorstFirst(t *testing.T) {
	in := reportInput(
		reversed("MILD", 96*time.Hour, 6*time.Minute, 25*time.Hour),
		reversed("SEVERE", 96*time.Hour, 6*time.Minute, 72*time.Hour),
		reversed("MODERATE", 96*time.Hour, 6*time.Minute, 40*time.Hour),
	)

	r := report.Compute(in, 24*time.Hour, reportNow)
	if len(r.Breaches) != 3 {
		t.Fatalf("breaches = %d, want 3", len(r.Breaches))
	}
	if r.Breaches[0].TransactionID != "SEVERE" || r.Breaches[2].TransactionID != "MILD" {
		t.Errorf("order = %s, %s, %s", r.Breaches[0].TransactionID, r.Breaches[1].TransactionID, r.Breaches[2].TransactionID)
	}
}

// A report that silently drops breaches is worse than no report.
func TestComplianceMarksTruncation(t *testing.T) {
	var txns []*domain.Transaction
	for i := 0; i <= report.MaxBreaches+10; i++ {
		txns = append(txns, reversed("TX", 96*time.Hour, 6*time.Minute, 72*time.Hour))
	}

	r := report.Compute(reportInput(txns...), 24*time.Hour, reportNow)
	if !r.Truncated {
		t.Error("breach list was capped without saying so")
	}
	if len(r.Breaches) != report.MaxBreaches {
		t.Errorf("breaches = %d, want the cap of %d", len(r.Breaches), report.MaxBreaches)
	}
	// The count is still complete even though the list is not.
	if r.Compliance.Breached != len(txns) {
		t.Errorf("breached count = %d, want %d — the total must not be truncated", r.Compliance.Breached, len(txns))
	}
}

// Truncation and incompleteness are different claims. A truncated report still
// states the right numbers; an incomplete one does not, and has to say so.
func TestComplianceMarksIncompleteInput(t *testing.T) {
	in := reportInput(reversed("LATE", 96*time.Hour, 6*time.Minute, 72*time.Hour))
	in.Incomplete = true

	r := report.Compute(in, 24*time.Hour, reportNow)
	if !r.Incomplete {
		t.Fatal("a partial candidate set was reported as if it were the whole period")
	}
	if r.Notice == "" {
		t.Error("incomplete report gives no explanation")
	}
	if r.Truncated {
		t.Error("incomplete was conflated with a truncated breach list")
	}

	// And the normal case must not carry the warning.
	if clean := report.Compute(reportInput(), 24*time.Hour, reportNow); clean.Incomplete || clean.Notice != "" {
		t.Errorf("complete report claimed to be incomplete: %+v", clean)
	}
}

func TestComplianceCSV(t *testing.T) {
	r := report.Compute(reportInput(
		reversed("LATE", 96*time.Hour, 6*time.Minute, 72*time.Hour),
	), 24*time.Hour, reportNow)

	csv := r.CSV()
	if !strings.Contains(csv, "transaction_id,status,amount_minor") {
		t.Errorf("no header row: %s", csv)
	}
	if !strings.Contains(csv, "LATE") {
		t.Errorf("breach missing from CSV: %s", csv)
	}
}

// Every field in this export is customer-controlled: a transaction id is
// validated for length and nothing else. Built with string concatenation, one
// containing a comma silently misaligns every column after it — in the document
// a regulator reads.
func TestComplianceCSVSurvivesHostileIdentifiers(t *testing.T) {
	in := reportInput(reversed(`TX,"1`+"\n"+`INJECTED`, 96*time.Hour, 6*time.Minute, 72*time.Hour))
	csvOut := report.Compute(in, 24*time.Hour, reportNow).CSV()

	rows, err := csv.NewReader(strings.NewReader(csvOut)).ReadAll()
	if err != nil {
		t.Fatalf("the export is not parseable CSV: %v\n%s", err, csvOut)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want a header and one breach — the id broke the row structure", len(rows))
	}
	if len(rows[1]) != len(rows[0]) {
		t.Fatalf("breach row has %d columns against %d in the header", len(rows[1]), len(rows[0]))
	}
	// The id survives intact rather than being mangled or split.
	if !strings.Contains(rows[1][0], "INJECTED") {
		t.Errorf("transaction_id came back as %q", rows[1][0])
	}
}

// Excel and Sheets execute a cell beginning =, +, - or @. Quoting does not stop
// it: the formula runs after the CSV is parsed, when a compliance officer opens
// the file.
func TestComplianceCSVDefusesSpreadsheetFormulas(t *testing.T) {
	for _, id := range []string{`=cmd|'/c calc'!A1`, `+1+1`, `-2+3`, `@SUM(A1:A9)`} {
		t.Run(id, func(t *testing.T) {
			in := reportInput(reversed(id, 96*time.Hour, 6*time.Minute, 72*time.Hour))
			csvOut := report.Compute(in, 24*time.Hour, reportNow).CSV()

			rows, err := csv.NewReader(strings.NewReader(csvOut)).ReadAll()
			if err != nil {
				t.Fatalf("unparseable: %v", err)
			}
			cell := rows[1][0]
			if strings.HasPrefix(cell, "=") || strings.HasPrefix(cell, "+") ||
				strings.HasPrefix(cell, "-") || strings.HasPrefix(cell, "@") {
				t.Errorf("cell %q would be executed as a formula", cell)
			}
			// Defused, not discarded: the original is still readable.
			if !strings.Contains(cell, strings.TrimLeft(id, "=+-@")) {
				t.Errorf("cell %q lost the original value", cell)
			}
		})
	}
}

// A PDF is what gets attached to an email and filed, so it has to be a document
// a viewer will actually open — not merely bytes we called a PDF.
func TestCompliancePDFIsAValidDocument(t *testing.T) {
	r := report.Compute(reportInput(
		reversed("TX-LATE", 96*time.Hour, 6*time.Minute, 72*time.Hour),
	), 24*time.Hour, reportNow)

	pdf := r.PDF()
	if !bytes.HasPrefix(pdf, []byte("%PDF-1.4")) {
		t.Fatalf("no PDF header: %q", pdf[:min(16, len(pdf))])
	}
	if !bytes.Contains(pdf, []byte("%%EOF")) {
		t.Error("no EOF marker")
	}
	// The cross-reference table is what a viewer uses to find objects; a wrong
	// offset renders as a blank or corrupt document rather than an error.
	xrefAt := bytes.LastIndex(pdf, []byte("startxref"))
	if xrefAt < 0 {
		t.Fatal("no startxref")
	}
	var offset int
	if _, err := fmt.Sscanf(string(pdf[xrefAt:]), "startxref\n%d", &offset); err != nil {
		t.Fatalf("unreadable startxref: %v", err)
	}
	if offset <= 0 || offset >= len(pdf) {
		t.Fatalf("startxref points to %d, outside a %d byte file", offset, len(pdf))
	}
	if !bytes.HasPrefix(pdf[offset:], []byte("xref")) {
		t.Errorf("startxref points at %q, not the xref table", pdf[offset:min(offset+12, len(pdf))])
	}

	// The content a compliance officer is looking for.
	// Escaped as it appears in the content stream — the escaping applies to our
	// own header text as much as to a customer's identifier.
	for _, want := range []string{"Reversal SLA Compliance Report", "TX-LATE", `Amount \(minor units\)`} {
		if !bytes.Contains(pdf, []byte(want)) {
			t.Errorf("document does not contain %q", want)
		}
	}
}

// Every value in the document is customer-controlled. A transaction id
// containing a bracket would otherwise close the PDF string early and have the
// rest parsed as operators — the same class of bug the CSV had.
func TestCompliancePDFEscapesHostileIdentifiers(t *testing.T) {
	r := report.Compute(reportInput(
		reversed(`TX(evil)\ (Tj 0 0 Td (injected`, 96*time.Hour, 6*time.Minute, 72*time.Hour),
	), 24*time.Hour, reportNow)

	pdf := string(r.PDF())
	// Each bracket and backslash from the id must appear escaped.
	if strings.Contains(pdf, `(TX(evil)`) {
		t.Error("an unescaped bracket from the transaction id reached the content stream")
	}
	if !strings.Contains(pdf, `TX\(evil\)`) {
		t.Error("the transaction id was not escaped")
	}
	// Still a well-formed document afterwards.
	if !strings.HasPrefix(pdf, "%PDF-1.4") || !strings.Contains(pdf, "%%EOF") {
		t.Error("the document was corrupted by the identifier")
	}
}

// A long report has to break across pages, and every page needs its headers or
// page four is unreadable on its own.
func TestCompliancePDFPaginates(t *testing.T) {
	var txns []*domain.Transaction
	for i := 0; i < 200; i++ {
		txns = append(txns, reversed("TX", 96*time.Hour, 6*time.Minute, 72*time.Hour))
	}
	pdf := string(report.Compute(reportInput(txns...), 24*time.Hour, reportNow).PDF())

	pages := strings.Count(pdf, "/Type /Page ")
	if pages < 2 {
		t.Fatalf("200 breaches produced %d page(s)", pages)
	}
	if got := strings.Count(pdf, "/Count "+strconv.Itoa(pages)); got != 1 {
		t.Errorf("the page tree does not declare %d pages", pages)
	}
	// Headers repeated per page, not just once.
	if strings.Count(pdf, "Amount \\(minor units\\)") < pages {
		t.Error("column headers are missing from at least one page")
	}
}

func TestIngestCompliancePDF(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	w := f.do(t, http.MethodGet, "/v1/reports/reversal-compliance?format=pdf", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/pdf" {
		t.Errorf("content-type = %q", ct)
	}
	if !strings.Contains(w.Header().Get("Content-Disposition"), ".pdf") {
		t.Errorf("content-disposition = %q", w.Header().Get("Content-Disposition"))
	}
	if !bytes.HasPrefix(w.Body.Bytes(), []byte("%PDF")) {
		t.Error("the body is not a PDF")
	}

	// An unknown format is still refused rather than silently served as JSON.
	if w := f.do(t, http.MethodGet, "/v1/reports/reversal-compliance?format=xlsx", f.keyA, nil); w.Code != http.StatusBadRequest {
		t.Errorf("unknown format = %d, want 400", w.Code)
	}
}
