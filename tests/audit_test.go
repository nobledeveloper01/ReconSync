package tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/audit"
	"github.com/nobledeveloper01/ReconSync/internal/store"
)

func auditRecord(event, txnID string) *audit.Record {
	return &audit.Record{
		EventType:  event,
		OccurredAt: time.Date(2026, 8, 12, 9, 14, 22, 0, time.UTC),
		Actor:      map[string]any{"type": "system", "id": "detection-sweep"},
		Subject:    map[string]any{"type": "transaction", "id": txnID},
		Payload:    map[string]any{"verdict": "orphaned", "confidence": 0.7},
	}
}

func TestComputeHashIsDeterministic(t *testing.T) {
	r := *auditRecord(audit.EventDetected, "TX-1")
	r.Seq = 1

	first, err := audit.ComputeHash(r, "")
	if err != nil {
		t.Fatalf("ComputeHash: %v", err)
	}
	second, err := audit.ComputeHash(r, "")
	if err != nil {
		t.Fatalf("ComputeHash: %v", err)
	}
	if first != second {
		t.Fatal("the same record hashed two different ways")
	}

	// Every input must change the hash, or it is not covering that field.
	changes := map[string]func(*audit.Record){
		"seq":        func(x *audit.Record) { x.Seq = 2 },
		"event type": func(x *audit.Record) { x.EventType = "something.else" },
		"occurred":   func(x *audit.Record) { x.OccurredAt = x.OccurredAt.Add(time.Second) },
		"actor":      func(x *audit.Record) { x.Actor = map[string]any{"type": "user"} },
		"subject":    func(x *audit.Record) { x.Subject = map[string]any{"id": "TX-2"} },
		"payload":    func(x *audit.Record) { x.Payload = map[string]any{"verdict": "completed"} },
	}
	for name, mutate := range changes {
		altered := r
		mutate(&altered)
		got, err := audit.ComputeHash(altered, "")
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if got == first {
			t.Errorf("changing the %s did not change the hash", name)
		}
	}

	// And the link to the previous record is part of it.
	linked, err := audit.ComputeHash(r, "sha256:deadbeef")
	if err != nil {
		t.Fatalf("ComputeHash: %v", err)
	}
	if linked == first {
		t.Error("prev_hash is not covered by the hash")
	}
}

// A nil map and an empty map are the same content and must hash the same, or
// callers would produce two hashes for one record.
func TestComputeHashTreatsNilAndEmptyMapsAlike(t *testing.T) {
	withNil := audit.Record{Seq: 1, TenantID: "t", EventType: "e"}
	withEmpty := audit.Record{Seq: 1, TenantID: "t", EventType: "e",
		Actor: map[string]any{}, Subject: map[string]any{}, Payload: map[string]any{}}

	a, err := audit.ComputeHash(withNil, "")
	if err != nil {
		t.Fatalf("ComputeHash: %v", err)
	}
	b, err := audit.ComputeHash(withEmpty, "")
	if err != nil {
		t.Fatalf("ComputeHash: %v", err)
	}
	if a != b {
		t.Error("nil and empty maps hashed differently")
	}
}

func TestVerifyChainAcceptsAnIntactChain(t *testing.T) {
	var records []audit.Record
	prev := ""
	for i := 1; i <= 5; i++ {
		r := *auditRecord(audit.EventDetected, "TX")
		r.Seq = int64(i)
		r.TenantID = tenantA
		r.PrevHash = prev
		h, err := audit.ComputeHash(r, prev)
		if err != nil {
			t.Fatalf("ComputeHash: %v", err)
		}
		r.Hash = h
		prev = h
		records = append(records, r)
	}

	v := audit.VerifyChain(tenantA, records)
	if !v.Verified {
		t.Fatalf("intact chain failed: %s at %d", v.Reason, v.BrokenAt)
	}
	if v.Records != 5 || v.LastSeq != 5 {
		t.Errorf("verification = %+v", v)
	}
	if v.LastHash != prev {
		t.Error("last hash does not match the chain tip")
	}
}

func TestVerifyChainEmpty(t *testing.T) {
	v := audit.VerifyChain(tenantA, nil)
	if !v.Verified || v.Records != 0 {
		t.Errorf("empty chain = %+v, want verified with no records", v)
	}
}

// The three ways a chain can be broken, each of which the triggers alone would
// not catch.
func TestVerifyChainDetectsTampering(t *testing.T) {
	build := func() []audit.Record {
		var records []audit.Record
		prev := ""
		for i := 1; i <= 4; i++ {
			r := *auditRecord(audit.EventDetected, "TX")
			r.Seq = int64(i)
			r.TenantID = tenantA
			r.PrevHash = prev
			h, _ := audit.ComputeHash(r, prev)
			r.Hash = h
			prev = h
			records = append(records, r)
		}
		return records
	}

	t.Run("record altered", func(t *testing.T) {
		records := build()
		// Change the verdict without recomputing the hash, as an edit would.
		records[2].Payload = map[string]any{"verdict": "completed", "confidence": 1.0}

		v := audit.VerifyChain(tenantA, records)
		if v.Verified {
			t.Fatal("an altered record verified")
		}
		if v.BrokenAt != 3 {
			t.Errorf("broken at %d, want 3", v.BrokenAt)
		}
	})

	t.Run("record removed", func(t *testing.T) {
		records := build()
		records = append(records[:1], records[2:]...) // drop seq 2

		v := audit.VerifyChain(tenantA, records)
		if v.Verified {
			t.Fatal("a chain with a removed record verified — deletion would be invisible")
		}
	})

	t.Run("chain re-linked", func(t *testing.T) {
		records := build()
		records[2].PrevHash = "sha256:0000"
		h, _ := audit.ComputeHash(records[2], "sha256:0000")
		records[2].Hash = h // internally consistent, but detached from the chain

		v := audit.VerifyChain(tenantA, records)
		if v.Verified {
			t.Fatal("a re-linked record verified")
		}
		if v.BrokenAt != 3 {
			t.Errorf("broken at %d, want 3", v.BrokenAt)
		}
	})
}

// --- store conformance ---

func testAuditChainAppend(t *testing.T, s store.Store) {
	ctx := context.Background()
	tenant := uniqueTenant(t, s)

	for i := 1; i <= 3; i++ {
		got, err := s.AppendAudit(ctx, tenant, auditRecord(audit.EventDetected, "TX-1"))
		if err != nil {
			t.Fatalf("AppendAudit %d: %v", i, err)
		}
		if got.Seq != int64(i) {
			t.Errorf("seq = %d, want %d", got.Seq, i)
		}
		if got.Hash == "" {
			t.Error("no hash computed")
		}
	}

	records, err := s.ListAudit(ctx, tenant, 100)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("listed %d records, want 3", len(records))
	}

	// The first record starts the chain, the rest link backwards.
	if records[0].PrevHash != "" {
		t.Errorf("first record has prev_hash %q, want empty", records[0].PrevHash)
	}
	for i := 1; i < len(records); i++ {
		if records[i].PrevHash != records[i-1].Hash {
			t.Errorf("record %d is not linked to %d", i+1, i)
		}
	}

	if v := audit.VerifyChain(tenant, records); !v.Verified {
		t.Errorf("stored chain does not verify: %s at %d", v.Reason, v.BrokenAt)
	}
}

// Content must survive the round trip through storage, or the stored hash would
// stop matching what comes back.
func testAuditRoundTripsContent(t *testing.T, s store.Store) {
	ctx := context.Background()
	tenant := uniqueTenant(t, s)

	written, err := s.AppendAudit(ctx, tenant, auditRecord(audit.EventDetected, "TX-9"))
	if err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	records, err := s.ListAudit(ctx, tenant, 10)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("listed %d, want 1", len(records))
	}

	got := records[0]
	if got.Hash != written.Hash {
		t.Errorf("hash changed in storage: %s vs %s", got.Hash, written.Hash)
	}
	if got.Subject["id"] != "TX-9" {
		t.Errorf("subject = %v", got.Subject)
	}
	if got.Payload["verdict"] != "orphaned" {
		t.Errorf("payload = %v", got.Payload)
	}

	// Recomputing from what was read back must reproduce the stored hash.
	recomputed, err := audit.ComputeHash(got, got.PrevHash)
	if err != nil {
		t.Fatalf("ComputeHash: %v", err)
	}
	if recomputed != got.Hash {
		t.Error("the stored hash does not match the stored content after a round trip")
	}
}

// The chain is built read-then-write, so replicas can compute the same seq. The
// unique constraint turns that into a retry rather than a forked chain — if it
// did not, the chain would silently branch and verification would be worthless.
func TestConcurrentAppendsDoNotForkTheChain(t *testing.T) {
	pool := testPool(t)
	s := store.NewPostgres(pool)
	ctx := context.Background()
	tenant := uniqueTenant(t, s)

	const writers = 8
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := s.AppendAudit(ctx, tenant, auditRecord(audit.EventDetected, fmt.Sprintf("TX-%d", n))); err != nil {
				t.Errorf("AppendAudit: %v", err)
			}
		}(i)
	}
	wg.Wait()

	records, err := s.ListAudit(ctx, tenant, 100)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(records) != writers {
		t.Fatalf("stored %d records, want %d", len(records), writers)
	}

	// Gapless, correctly linked, and verifying — despite the contention.
	if v := audit.VerifyChain(tenant, records); !v.Verified {
		t.Fatalf("concurrent appends forked the chain: %s at seq %d", v.Reason, v.BrokenAt)
	}
}

func testAuditChainsAreTenantScoped(t *testing.T, s store.Store) {
	ctx := context.Background()
	one := uniqueTenant(t, s)
	two := uniqueTenant(t, s)

	if _, err := s.AppendAudit(ctx, one, auditRecord(audit.EventDetected, "TX-A")); err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}
	second, err := s.AppendAudit(ctx, two, auditRecord(audit.EventDetected, "TX-B"))
	if err != nil {
		t.Fatalf("AppendAudit: %v", err)
	}

	// Each tenant's chain starts at 1 and is independent.
	if second.Seq != 1 {
		t.Errorf("second tenant's first record has seq %d, want 1", second.Seq)
	}
	if second.PrevHash != "" {
		t.Errorf("second tenant's chain links to the first tenant's: %q", second.PrevHash)
	}

	records, err := s.ListAudit(ctx, two, 10)
	if err != nil {
		t.Fatalf("ListAudit: %v", err)
	}
	if len(records) != 1 || records[0].Subject["id"] != "TX-B" {
		t.Errorf("tenant two sees %v", records)
	}
}
