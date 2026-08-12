// Package report turns stored transactions into the evidence a compliance
// officer has to produce.
//
// The question a regulator asks is not "does your system work" but "show me
// every failed transfer in this period and prove you reversed it in time". This
// package answers exactly that, and is deliberately explicit about what it
// cannot yet know.
package report

import (
	"fmt"
	"sort"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

// DefaultReversalDeadline is how long after the debit a reversal is expected to
// be complete. It is a parameter rather than a constant because the mandate
// differs by regulator and by transaction type, and baking in one jurisdiction's
// number would quietly produce wrong reports everywhere else.
const DefaultReversalDeadline = 24 * time.Hour

// Totals counts transactions by outcome over the period.
type Totals struct {
	Transactions          int `json:"transactions"`
	Settled               int `json:"settled"`
	OrphansDetected       int `json:"orphans_detected"`
	ReversalsDispatched   int `json:"reversals_dispatched"`
	ReversalsCompleted    int `json:"reversals_completed"`
	ReversalsFailed       int `json:"reversals_failed"`
	AwaitingInvestigation int `json:"awaiting_investigation"`
	StillOpen             int `json:"still_open"`
}

// Latency summarises a distribution in whole seconds.
type Latency struct {
	Samples int     `json:"samples"`
	P50     float64 `json:"p50_seconds"`
	P95     float64 `json:"p95_seconds"`
	Max     float64 `json:"max_seconds"`
}

// Compliance is the headline: did reversals complete inside the deadline.
type Compliance struct {
	WithinDeadline int `json:"within_deadline"`
	Breached       int `json:"breached"`

	// Outstanding is reversals still in flight. They are neither compliant nor
	// breached yet, and folding them into either would misstate the position.
	Outstanding int `json:"outstanding"`

	// Rate is over settled cases only — within / (within + breached). Nil when
	// nothing has concluded, because 0% and "no data" are not the same claim.
	Rate *float64 `json:"rate,omitempty"`
}

// Breach is one transaction that missed its deadline.
type Breach struct {
	TransactionID       string     `json:"transaction_id"`
	AmountMinor         int64      `json:"amount_minor"`
	Currency            string     `json:"currency"`
	DebitAt             time.Time  `json:"debit_at"`
	DetectedAt          *time.Time `json:"detected_at,omitempty"`
	ReversalCompletedAt *time.Time `json:"reversal_completed_at,omitempty"`
	ElapsedSeconds      float64    `json:"elapsed_seconds"`
	Status              string     `json:"status"`
	Reason              string     `json:"reason"`
}

// Report is the whole document.
type Report struct {
	TenantID                string    `json:"tenant_id"`
	From                    time.Time `json:"from"`
	To                      time.Time `json:"to"`
	GeneratedAt             time.Time `json:"generated_at"`
	ReversalDeadlineSeconds int       `json:"reversal_deadline_seconds"`

	Totals     Totals     `json:"totals"`
	Detection  Latency    `json:"detection_latency"`
	Reversal   Latency    `json:"reversal_latency"`
	Compliance Compliance `json:"compliance"`
	Breaches   []Breach   `json:"breaches"`

	// Truncated says the breach list was capped. The counts above are still
	// exact; only the itemised list is short. A report that silently drops
	// breaches is worse than no report.
	Truncated bool `json:"truncated,omitempty"`

	// Incomplete says more went wrong in this period than the report could
	// examine, so every compliance count is a lower bound. Narrow the period
	// and run it again.
	Incomplete bool   `json:"incomplete,omitempty"`
	Notice     string `json:"notice,omitempty"`
}

// Input is what the store supplies.
type Input struct {
	TenantID string
	From     time.Time
	To       time.Time

	// CountsByStatus covers every transaction in the period.
	CountsByStatus map[domain.Status]int

	// Reversals are the transactions that reached orphaned or beyond, in full.
	// Only this subset is fetched whole, so the report stays cheap on a tenant
	// with millions of healthy transactions.
	Reversals []*domain.Transaction

	// Incomplete says the caller could not supply every candidate. Every count
	// derived from Reversals is then a lower bound, and the report has to say
	// so — an understated breach count read as fact is the worst thing this
	// package could produce.
	Incomplete bool
}

// MaxBreaches caps the itemised list.
const MaxBreaches = 1000

// Compute builds the report.
func Compute(in Input, deadline time.Duration, now time.Time) Report {
	if deadline <= 0 {
		deadline = DefaultReversalDeadline
	}

	r := Report{
		TenantID:                in.TenantID,
		From:                    in.From.UTC(),
		To:                      in.To.UTC(),
		GeneratedAt:             now.UTC(),
		ReversalDeadlineSeconds: int(deadline / time.Second),
		// Always an array, never null: a consumer should be able to iterate the
		// breach list without a nil check.
		Breaches: []Breach{},
	}

	for status, n := range in.CountsByStatus {
		r.Totals.Transactions += n
		switch status {
		case domain.StatusCompleted:
			r.Totals.Settled += n
		case domain.StatusOrphaned:
			r.Totals.OrphansDetected += n
		case domain.StatusReversalPending:
			r.Totals.ReversalsDispatched += n
		case domain.StatusReversalCompleted:
			r.Totals.ReversalsCompleted += n
		case domain.StatusReversalFailed:
			r.Totals.ReversalsFailed += n
		case domain.StatusSuspect:
			r.Totals.AwaitingInvestigation += n
		case domain.StatusPendingDebit, domain.StatusPendingUnknown:
			r.Totals.StillOpen += n
		}
	}
	// Every transaction that ever reached orphaned was detected, whatever it
	// became afterwards.
	r.Totals.OrphansDetected = len(in.Reversals)

	if in.Incomplete {
		r.Incomplete = true
		r.Notice = "more transactions were detected in this period than the report could examine; " +
			"every compliance count below is a lower bound. Narrow the period and run it again."
	}

	var detection, reversal []float64

	for _, txn := range in.Reversals {
		if txn.DetectedAt != nil {
			// How long after the window closed we noticed. This is the number
			// our own SLO is written against.
			detection = append(detection, txn.DetectedAt.Sub(txn.ExpectedCompletionAt).Seconds())
		}

		switch {
		case txn.ReversalCompletedAt != nil:
			elapsed := txn.ReversalCompletedAt.Sub(txn.DebitAt)
			if txn.DetectedAt != nil {
				reversal = append(reversal, txn.ReversalCompletedAt.Sub(*txn.DetectedAt).Seconds())
			}
			if elapsed <= deadline {
				r.Compliance.WithinDeadline++
				continue
			}
			r.Compliance.Breached++
			addBreach(&r, txn, elapsed.Seconds(), "reversal completed after the deadline")

		case txn.Status == domain.StatusReversalFailed:
			// Delivery exhausted its retries, so nobody acted. Breached whatever
			// the clock says.
			r.Compliance.Breached++
			addBreach(&r, txn, now.Sub(txn.DebitAt).Seconds(), "reversal delivery dead-lettered; no confirmation received")

		default:
			// Still in flight. Past the deadline it is already a breach, and
			// counting it as merely outstanding would understate the position.
			if now.Sub(txn.DebitAt) > deadline {
				r.Compliance.Breached++
				addBreach(&r, txn, now.Sub(txn.DebitAt).Seconds(), "deadline passed with no reversal confirmation")
				continue
			}
			r.Compliance.Outstanding++
		}
	}

	if concluded := r.Compliance.WithinDeadline + r.Compliance.Breached; concluded > 0 {
		rate := float64(r.Compliance.WithinDeadline) / float64(concluded)
		r.Compliance.Rate = &rate
	}

	r.Detection = summarise(detection)
	r.Reversal = summarise(reversal)

	// Worst first: an auditor reads the top of the list.
	sort.SliceStable(r.Breaches, func(i, j int) bool {
		return r.Breaches[i].ElapsedSeconds > r.Breaches[j].ElapsedSeconds
	})
	return r
}

func addBreach(r *Report, txn *domain.Transaction, elapsed float64, reason string) {
	if len(r.Breaches) >= MaxBreaches {
		r.Truncated = true
		return
	}
	r.Breaches = append(r.Breaches, Breach{
		TransactionID:       txn.TransactionID,
		AmountMinor:         txn.AmountMinor,
		Currency:            txn.Currency,
		DebitAt:             txn.DebitAt.UTC(),
		DetectedAt:          txn.DetectedAt,
		ReversalCompletedAt: txn.ReversalCompletedAt,
		ElapsedSeconds:      round1(elapsed),
		Status:              txn.Status.String(),
		Reason:              reason,
	})
}

// summarise computes percentiles by nearest rank, which needs no interpolation
// and is what an auditor can reproduce by hand from the raw list.
func summarise(values []float64) Latency {
	if len(values) == 0 {
		return Latency{}
	}
	sorted := make([]float64, len(values))
	copy(sorted, values)
	sort.Float64s(sorted)

	return Latency{
		Samples: len(sorted),
		P50:     round1(percentile(sorted, 0.50)),
		P95:     round1(percentile(sorted, 0.95)),
		Max:     round1(sorted[len(sorted)-1]),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p*float64(len(sorted))+0.5) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func round1(v float64) float64 {
	return float64(int(v*10+0.5)) / 10
}

// CSV renders the breach list, which is the part a compliance team works from.
func (r Report) CSV() string {
	out := "transaction_id,status,amount_minor,currency,debit_at,detected_at,reversal_completed_at,elapsed_seconds,reason\n"
	for _, b := range r.Breaches {
		out += fmt.Sprintf("%s,%s,%d,%s,%s,%s,%s,%.1f,%q\n",
			b.TransactionID, b.Status, b.AmountMinor, b.Currency,
			b.DebitAt.Format(time.RFC3339), formatPtr(b.DetectedAt), formatPtr(b.ReversalCompletedAt),
			b.ElapsedSeconds, b.Reason)
	}
	return out
}

func formatPtr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
