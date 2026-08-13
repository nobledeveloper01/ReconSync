package provider

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// The adapter that works for every institution, including the ones with no API.
//
// Most Nigerian banks will not give a fintech a status endpoint. Nearly all of
// them deliver a settlement file — a daily list of what actually settled, by
// SFTP or a portal download. That file is the institution's own record, which
// makes it better evidence than any inference we could make from silence.
//
// The whole design problem is what a transaction's *absence* from a file means,
// and the answer depends on timing. See coverage below.

// SettlementRecord is one line of a settlement file, after parsing.
type SettlementRecord struct {
	Reference   string
	AmountMinor int64
	Currency    string
	SettledAt   time.Time
	Status      string
}

// SettlementFile is a parsed file and the period it covers.
type SettlementFile struct {
	Path       string
	CoversFrom time.Time
	CoversTo   time.Time
	Records    map[string]SettlementRecord
}

// SettlementOptions configures the adapter.
type SettlementOptions struct {
	// Name is the rail this file covers, matching transactions' provider field.
	Name string

	// Dir holds the settlement files. Re-read when they change, so a new
	// delivery is picked up without a restart.
	Dir string

	// Glob selects which files in Dir are settlement files.
	Glob string

	// Layout parses the settled-at column.
	Layout string

	// SettledStatuses are the values in the status column that mean the money
	// arrived. Everything else in the file is an explicit failure.
	//
	// A file that lists failures is more useful than one that lists only
	// successes: it turns "absent" into "reported failed", which is evidence
	// rather than inference.
	SettledStatuses []string

	// Columns maps our fields onto the institution's header names, because no
	// two of them agree on what to call anything.
	Columns SettlementColumns

	// Grace is how long after a transaction we wait before treating its absence
	// from a covering file as failure. Files are delivered in batches and a
	// transaction near the cut-off may simply be in tomorrow's.
	Grace time.Duration

	// RefreshInterval is how often the directory is checked for new files.
	//
	// It exists because a sweep asks about every orphan it claimed — up to 500
	// — and checking the directory per question means a stat call per file per
	// question. Against a year of daily files that is hundreds of thousands of
	// syscalls every few seconds, on the one path the detection SLO is measured
	// against. A settlement file that arrives is still picked up within this
	// interval, which is the only freshness anyone needs from a daily delivery.
	RefreshInterval time.Duration

	Now func() time.Time
}

// SettlementColumns names the header for each field.
type SettlementColumns struct {
	Reference string
	Amount    string
	Currency  string
	SettledAt string
	Status    string
}

// DefaultSettlementGrace is how long absence is treated as inconclusive.
//
// Erring long is deliberate: waiting produces a delayed reversal, and guessing
// produces a wrong one. Only one of those takes money off a customer who
// already received it.
const DefaultSettlementGrace = 26 * time.Hour

// DefaultRefreshInterval bounds how often the directory is re-checked.
const DefaultRefreshInterval = 5 * time.Second

// SettlementProvider answers from settlement files.
type SettlementProvider struct {
	opts SettlementOptions

	mu          sync.RWMutex
	files       []SettlementFile
	loaded      map[string]time.Time // path to modtime, so unchanged files are not re-parsed
	lastChecked time.Time
}

// NewSettlementProvider builds the adapter and loads what is already on disk.
func NewSettlementProvider(opts SettlementOptions) (*SettlementProvider, error) {
	if opts.Name == "" {
		return nil, errors.New("provider: settlement adapter needs a name")
	}
	if opts.Dir == "" {
		return nil, errors.New("provider: settlement adapter needs a directory")
	}
	if opts.Glob == "" {
		opts.Glob = "*.csv"
	}
	if opts.Layout == "" {
		opts.Layout = time.RFC3339
	}
	if opts.Columns.Reference == "" {
		opts.Columns.Reference = "reference"
	}
	if opts.Grace <= 0 {
		opts.Grace = DefaultSettlementGrace
	}
	if opts.RefreshInterval < 0 {
		opts.RefreshInterval = 0
	} else if opts.RefreshInterval == 0 {
		opts.RefreshInterval = DefaultRefreshInterval
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if len(opts.SettledStatuses) == 0 {
		opts.SettledStatuses = []string{"success", "successful", "settled", "completed", "paid"}
	}

	p := &SettlementProvider{opts: opts, loaded: map[string]time.Time{}}
	if err := p.Reload(); err != nil {
		return nil, err
	}
	return p, nil
}

// Name reports the rail this adapter covers.
func (p *SettlementProvider) Name() string { return p.opts.Name }

// Reload re-reads files whose modification time has changed.
//
// Unchanged files are skipped rather than re-parsed: a settlement directory
// accumulates months of history, and re-reading all of it on every sweep would
// make the detection loop as slow as the disk.
func (p *SettlementProvider) Reload() error {
	paths, err := filepath.Glob(filepath.Join(p.opts.Dir, p.opts.Glob))
	if err != nil {
		return fmt.Errorf("provider: list settlement files: %w", err)
	}
	sort.Strings(paths)

	p.mu.Lock()
	defer p.mu.Unlock()

	var (
		files    []SettlementFile
		reloaded = map[string]time.Time{}
	)

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			// A file that vanished between glob and stat is not an error worth
			// failing the whole reload over.
			continue
		}
		reloaded[path] = info.ModTime()

		file, err := p.parseFile(path)
		if err != nil {
			// One malformed file must not blind the adapter to the others. It
			// yields Unknown for what it would have covered, which is the safe
			// direction.
			return fmt.Errorf("provider: %w", err)
		}
		files = append(files, file)
	}

	p.files = files
	p.loaded = reloaded
	return nil
}

// Query answers from the files on disk.
func (p *SettlementProvider) Query(_ context.Context, ref Ref) (Status, error) {
	// Picked up without a restart: a settlement file arriving mid-morning must
	// start answering questions within the refresh interval, not after the next
	// deploy.
	if p.dueForRefresh() && p.changed() {
		if err := p.Reload(); err != nil {
			return Status{Outcome: Unknown, Provider: p.opts.Name,
				Detail: "settlement files could not be read"}, nil
		}
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	key := lookupKey(ref)
	for _, file := range p.files {
		rec, ok := file.Records[key]
		if !ok {
			continue
		}
		if !p.isSettled(rec.Status) {
			// The institution's own record says it did not settle. This is the
			// strongest evidence the system can obtain.
			return Status{
				Outcome:    Failed,
				Provider:   p.opts.Name,
				Reference:  rec.Reference,
				ObservedAt: rec.SettledAt,
				Detail:     "settlement file records status " + rec.Status,
			}, nil
		}
		// An amount that disagrees is not a settlement of this transaction. It
		// may be a partial, a fee, or a reference collision — none of which we
		// can resolve, and all of which a human must look at.
		if ref.AmountMinor > 0 && rec.AmountMinor > 0 && rec.AmountMinor != ref.AmountMinor {
			return Status{
				Outcome:   Unknown,
				Provider:  p.opts.Name,
				Reference: rec.Reference,
				Detail: fmt.Sprintf("settlement file shows %d, we recorded %d",
					rec.AmountMinor, ref.AmountMinor),
			}, nil
		}
		return Status{
			Outcome:    Settled,
			Provider:   p.opts.Name,
			Reference:  rec.Reference,
			ObservedAt: rec.SettledAt,
			Detail:     "settlement file confirms it settled",
		}, nil
	}

	return p.absent(ref), nil
}

// absent decides what a transaction's absence means, which is the entire
// difficulty of this adapter.
//
// Absent from a file that covers the period is evidence of failure. Absent
// because no file covers the period yet is no evidence at all, and treating the
// two the same way would reverse every transaction on any day a file was late.
func (p *SettlementProvider) absent(ref Ref) Status {
	now := p.opts.Now().UTC()

	if len(p.files) == 0 {
		return Status{Outcome: Unknown, Provider: p.opts.Name,
			Detail: "no settlement files have been delivered"}
	}

	// Only conclude failure once the grace period has passed: a transaction
	// near a file's cut-off is often in the next delivery rather than missing.
	deadline := p.latestCoverage().Add(p.opts.Grace)
	if now.Before(deadline) {
		return Status{Outcome: Unknown, Provider: p.opts.Name,
			Detail: fmt.Sprintf("not in any settlement file yet; still inside the %s grace period",
				p.opts.Grace)}
	}

	return Status{
		Outcome:  NotFound,
		Provider: p.opts.Name,
		Detail: fmt.Sprintf("absent from settlement files covering up to %s",
			p.latestCoverage().Format(time.RFC3339)),
	}
}

// latestCoverage is the end of the most recent period any file covers.
func (p *SettlementProvider) latestCoverage() time.Time {
	var latest time.Time
	for _, f := range p.files {
		if f.CoversTo.After(latest) {
			latest = f.CoversTo
		}
	}
	return latest
}

// dueForRefresh rate-limits the directory check, and records that it happened.
//
// Deliberately not exact under concurrency: two sweeps racing here cost one
// extra directory listing, which is cheaper than serialising every query behind
// a write lock to be precise about it.
func (p *SettlementProvider) dueForRefresh() bool {
	now := p.opts.Now()

	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.lastChecked.IsZero() && now.Sub(p.lastChecked) < p.opts.RefreshInterval {
		return false
	}
	p.lastChecked = now
	return true
}

// changed reports whether any file appeared, vanished or was rewritten.
func (p *SettlementProvider) changed() bool {
	paths, err := filepath.Glob(filepath.Join(p.opts.Dir, p.opts.Glob))
	if err != nil {
		return false
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(paths) != len(p.loaded) {
		return true
	}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return true
		}
		if known, ok := p.loaded[path]; !ok || !known.Equal(info.ModTime()) {
			return true
		}
	}
	return false
}

func (p *SettlementProvider) isSettled(status string) bool {
	if status == "" {
		// A file with no status column lists settlements only, so being in it
		// is the confirmation.
		return true
	}
	got := strings.ToLower(strings.TrimSpace(status))
	for _, s := range p.opts.SettledStatuses {
		if got == strings.ToLower(s) {
			return true
		}
	}
	return false
}

// parseFile reads one CSV into records keyed by reference.
func (p *SettlementProvider) parseFile(path string) (SettlementFile, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return SettlementFile{}, fmt.Errorf("open settlement file %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)
	r.TrimLeadingSpace = true
	// Institutions pad their exports with trailing separators and comment
	// lines; a strict field count would reject an otherwise readable file.
	r.FieldsPerRecord = -1

	header, err := r.Read()
	if err != nil {
		return SettlementFile{}, fmt.Errorf("read header of %s: %w", path, err)
	}
	index := headerIndex(header)

	refCol, ok := index[strings.ToLower(p.opts.Columns.Reference)]
	if !ok {
		return SettlementFile{}, fmt.Errorf("%s has no %q column; found %s",
			filepath.Base(path), p.opts.Columns.Reference, strings.Join(header, ", "))
	}

	file := SettlementFile{Path: path, Records: map[string]SettlementRecord{}}
	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return SettlementFile{}, fmt.Errorf("read %s: %w", filepath.Base(path), err)
		}
		if refCol >= len(row) || strings.TrimSpace(row[refCol]) == "" {
			continue // padding or a blank trailing line
		}

		rec := SettlementRecord{
			Reference: strings.TrimSpace(row[refCol]),
			Currency:  strings.ToUpper(cell(row, index, p.opts.Columns.Currency)),
			Status:    cell(row, index, p.opts.Columns.Status),
		}
		if raw := cell(row, index, p.opts.Columns.Amount); raw != "" {
			rec.AmountMinor = parseMinor(raw)
		}
		if raw := cell(row, index, p.opts.Columns.SettledAt); raw != "" {
			if t, err := time.Parse(p.opts.Layout, raw); err == nil {
				rec.SettledAt = t.UTC()
			}
		}

		file.Records[strings.ToUpper(rec.Reference)] = rec

		// Coverage is derived from the rows themselves rather than the filename,
		// which no two institutions format the same way.
		if !rec.SettledAt.IsZero() {
			if file.CoversFrom.IsZero() || rec.SettledAt.Before(file.CoversFrom) {
				file.CoversFrom = rec.SettledAt
			}
			if rec.SettledAt.After(file.CoversTo) {
				file.CoversTo = rec.SettledAt
			}
		}
	}

	// A file with no usable timestamps covers nothing we can reason about, so
	// its absence proves nothing either. Falling back to the modification time
	// would let a re-copied file silently extend its own coverage.
	return file, nil
}

// bom is the byte order mark Excel puts at the head of a CSV it exported. A
// header column named "\ufeffreference" matches nothing, and the resulting error
// blames the institution for a file that is fine.
const bom = "\ufeff"

func headerIndex(header []string) map[string]int {
	out := make(map[string]int, len(header))
	for i, h := range header {
		out[strings.ToLower(strings.TrimSpace(strings.TrimPrefix(h, bom)))] = i
	}
	return out
}

func cell(row []string, index map[string]int, name string) string {
	if name == "" {
		return ""
	}
	i, ok := index[strings.ToLower(name)]
	if !ok || i >= len(row) {
		return ""
	}
	return strings.TrimSpace(row[i])
}

// parseMinor reads an amount in either minor units (500000) or major units with
// a decimal point (5000.00), because institutions publish both.
//
// Returns 0 when it cannot tell, and 0 disables the amount comparison rather
// than failing a match — a mis-parsed amount must not turn a real settlement
// into an investigation.
func parseMinor(raw string) int64 {
	clean := strings.NewReplacer(",", "", " ", "", " ", "").Replace(raw)
	if n, err := strconv.ParseInt(clean, 10, 64); err == nil {
		return n
	}
	f, err := strconv.ParseFloat(clean, 64)
	if err != nil {
		return 0
	}
	// Rounded rather than truncated: 5000.005 is a rounding artefact in the
	// export, not four thousand nine hundred and ninety-nine.
	return int64(f*100 + 0.5)
}

// lookupKey picks the identifier to match on.
//
// In practice this is almost always the transaction id: a provider reference
// reaches us on the credit event, and an orphan is precisely a transaction whose
// credit never arrived. Matching on the fintech's own id is what actually works,
// since that is the reference they put on the transfer. The ProviderRef branch
// is for the case where a rail's reference is known some other way.
func lookupKey(ref Ref) string {
	if ref.ProviderRef != "" {
		return strings.ToUpper(ref.ProviderRef)
	}
	return strings.ToUpper(ref.TransactionID)
}
