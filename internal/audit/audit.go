// Package audit turns the append-only log into a verifiable chain.
//
// Immutability triggers stop a row being edited or removed. They do not prove
// nothing was *replaced* — a row deleted and reinserted through a path that
// bypassed the trigger leaves no trace. Chaining each record to the one before
// it makes any such gap detectable: every hash after the tampered record stops
// matching, and no attacker can repair the chain without our records.
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// Event types recorded on the chain. Stable strings — they are hashed, so
// renaming one invalidates every record after it.
const (
	EventDetected           = "transaction.detected"
	EventReversalDispatched = "reversal.dispatched"
	EventReversalCompleted  = "reversal.completed"
	EventReversalFailed     = "reversal.failed"
	EventSettledByProvider  = "transaction.settled_by_provider"

	// Detection stopping is itself an auditable event: "why did you not watch
	// our transactions between 02:00 and 04:00" has to be answerable.
	EventSilenceDetected = "integration.silent"
	EventSilenceResolved = "integration.recovered"
)

// Record is one entry on a tenant's chain.
type Record struct {
	ID         int64
	Seq        int64
	TenantID   string
	EventType  string
	OccurredAt time.Time
	RecordedAt time.Time

	// Actor is who caused this — the sweep, the dispatcher, an API key.
	Actor map[string]any

	// Subject is what it concerns, normally a transaction.
	Subject map[string]any

	// Payload is the detail, including the evidence a verdict rested on.
	Payload map[string]any

	PrevHash string
	Hash     string
}

// hashedContent is the exact shape that gets hashed.
//
// Written out explicitly rather than hashing the Record struct, so adding a Go
// field cannot silently invalidate every existing chain. Changing anything here
// is a breaking change to every stored record and must be treated as one.
type hashedContent struct {
	Seq        int64          `json:"seq"`
	TenantID   string         `json:"tenant_id"`
	EventType  string         `json:"event_type"`
	OccurredAt string         `json:"occurred_at"`
	Actor      map[string]any `json:"actor"`
	Subject    map[string]any `json:"subject"`
	Payload    map[string]any `json:"payload"`
	PrevHash   string         `json:"prev_hash"`
}

// StoragePrecision is the finest timestamp the store can hold: Postgres
// timestamptz keeps microseconds.
//
// The hash is computed at this precision, not at the caller's. Hashing the
// nanoseconds a Go clock happens to provide would produce a hash that stops
// matching the moment the record is read back, because storage rounded them
// away — every record written on a platform with nanosecond resolution would
// fail its own verification. That is not a theoretical concern: it is what
// Linux does, while a macOS clock usually gives microseconds and hides it.
const StoragePrecision = time.Microsecond

// ComputeHash derives a record's hash from its content and the one before it.
//
// Deterministic by construction: timestamps are normalised to UTC at storage
// precision and Go's JSON encoder sorts map keys, so the same record always
// hashes the same way on any machine, before or after a round trip.
func ComputeHash(r Record, prevHash string) (string, error) {
	content := hashedContent{
		Seq:        r.Seq,
		TenantID:   r.TenantID,
		EventType:  r.EventType,
		OccurredAt: r.OccurredAt.UTC().Truncate(StoragePrecision).Format(time.RFC3339Nano),
		Actor:      orEmpty(r.Actor),
		Subject:    orEmpty(r.Subject),
		Payload:    orEmpty(r.Payload),
		PrevHash:   prevHash,
	}

	encoded, err := json.Marshal(content)
	if err != nil {
		return "", fmt.Errorf("audit: encode record for hashing: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// orEmpty keeps nil and an empty map hashing identically, so a caller passing
// one or the other does not produce two different hashes for the same content.
func orEmpty(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

// Verification is the outcome of walking a chain.
type Verification struct {
	TenantID string `json:"tenant_id"`
	Records  int    `json:"records"`
	Verified bool   `json:"verified"`

	// BrokenAt is the seq of the first record that failed, zero when intact.
	BrokenAt int64 `json:"broken_at,omitempty"`

	// Reason says what failed, in terms an auditor can act on.
	Reason string `json:"reason,omitempty"`

	FirstSeq int64  `json:"first_seq,omitempty"`
	LastSeq  int64  `json:"last_seq,omitempty"`
	LastHash string `json:"last_hash,omitempty"`
}

// VerifyChain walks records in seq order and reports the first break.
//
// Three things are checked, and all three matter. Recomputing each hash catches
// an edited record. Comparing each prev_hash against the previous record's hash
// catches a record swapped in from elsewhere. Requiring seq to be gapless from 1
// catches a record simply removed — without it, deletion would be invisible,
// which is the failure mode a chain exists to prevent.
func VerifyChain(tenantID string, records []Record) Verification {
	v := Verification{TenantID: tenantID, Records: len(records), Verified: true}
	if len(records) == 0 {
		return v
	}

	v.FirstSeq = records[0].Seq
	v.LastSeq = records[len(records)-1].Seq

	prevHash := ""
	for i, r := range records {
		expectedSeq := int64(i + 1)
		if r.Seq != expectedSeq {
			return broken(v, r.Seq, fmt.Sprintf(
				"sequence jumps to %d where %d was expected; a record is missing", r.Seq, expectedSeq))
		}
		if r.PrevHash != prevHash {
			return broken(v, r.Seq, "prev_hash does not match the preceding record; the chain was re-linked")
		}

		want, err := ComputeHash(r, prevHash)
		if err != nil {
			return broken(v, r.Seq, "record could not be hashed: "+err.Error())
		}
		if want != r.Hash {
			return broken(v, r.Seq, "recorded hash does not match the record's content; it was altered")
		}
		prevHash = r.Hash
	}

	v.LastHash = prevHash
	return v
}

func broken(v Verification, seq int64, reason string) Verification {
	v.Verified = false
	v.BrokenAt = seq
	v.Reason = reason
	return v
}
