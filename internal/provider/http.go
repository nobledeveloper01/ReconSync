package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// maxResponseBytes bounds what an adapter will read. A provider returning a
// large error page must not be able to spend our memory.
const maxResponseBytes = 1 << 20

// HTTPConfig describes a JSON status endpoint.
//
// It is deliberately generic rather than one type per rail: Paystack,
// Flutterwave and most bank APIs all answer "what is the status of X" with a
// JSON field, and the differences are configuration, not code. A rail that does
// not fit implements StatusProvider directly.
type HTTPConfig struct {
	// ProviderName is the rail this adapter answers for, matching the provider
	// recorded on the transaction.
	ProviderName string

	// URLTemplate is the status endpoint. {reference} is replaced with the
	// provider reference, or the transaction id when there is none.
	URLTemplate string

	// AuthHeader and AuthValue are sent on every request.
	//
	// AuthValue may contain {value}, which is replaced with the credential.
	// That exists because nearly every rail wants "Bearer sk_live_..." while
	// the secret manager holds "sk_live_...", and an operator who stores the
	// raw key without the prefix gets 401 on every query — which is Unknown,
	// which silently stops every reversal. A template makes the prefix
	// configuration rather than a trap in how the secret was stored.
	AuthHeader string
	AuthValue  string

	// AuthCredential is the secret substituted into AuthValue's {value}.
	AuthCredential string

	// StatusPath is a dotted path to the status field, e.g. "data.status".
	StatusPath string

	// SettledValues and FailedValues map that field onto an outcome. Matching
	// is case-insensitive. Anything unmatched is Unknown — never a guess.
	SettledValues []string
	FailedValues  []string

	// AmountPath is a dotted path to the amount, in minor units. Optional.
	//
	// When set, a settled response whose amount disagrees with ours is Unknown
	// rather than Settled. The settlement-file adapter has always checked this;
	// without it here, a reference collision or a partial settlement would read
	// as "the money arrived" and cancel a reversal that should have happened.
	AmountPath string

	Timeout time.Duration
	Client  *http.Client
}

// HTTPProvider queries a JSON status endpoint.
type HTTPProvider struct {
	cfg     HTTPConfig
	client  *http.Client
	settled map[string]struct{}
	failed  map[string]struct{}
}

// NewHTTP builds an adapter from a config.
func NewHTTP(cfg HTTPConfig) (*HTTPProvider, error) {
	switch {
	case cfg.ProviderName == "":
		return nil, errors.New("provider: ProviderName is required")
	case cfg.URLTemplate == "":
		return nil, errors.New("provider: URLTemplate is required")
	case !strings.Contains(cfg.URLTemplate, "{reference}"):
		return nil, errors.New("provider: URLTemplate must contain {reference}")
	case cfg.StatusPath == "":
		return nil, errors.New("provider: StatusPath is required")
	case len(cfg.SettledValues) == 0 && len(cfg.FailedValues) == 0:
		return nil, errors.New("provider: at least one of SettledValues or FailedValues is required")
	}

	p := &HTTPProvider{
		cfg:     cfg,
		client:  cfg.Client,
		settled: lowerSet(cfg.SettledValues),
		failed:  lowerSet(cfg.FailedValues),
	}
	if p.client == nil {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 5 * time.Second
		}
		// Bounded on purpose: this runs inside the detection sweep, and a rail
		// that hangs must not stall detection for every other tenant.
		p.client = &http.Client{Timeout: timeout}
	}
	return p, nil
}

func msToDuration(ms int) time.Duration {
	if ms <= 0 {
		return 0
	}
	return time.Duration(ms) * time.Millisecond
}

func lowerSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, v := range values {
		out[strings.ToLower(strings.TrimSpace(v))] = struct{}{}
	}
	return out
}

func (p *HTTPProvider) Name() string { return p.cfg.ProviderName }

// Query asks the endpoint about one transaction.
//
// Every failure path returns Unknown with a nil error, so a caller cannot
// mistake "we could not ask" for "it did not happen".
func (p *HTTPProvider) Query(ctx context.Context, ref Ref) (Status, error) {
	reference := ref.ProviderRef
	if reference == "" {
		reference = ref.TransactionID
	}

	url := strings.ReplaceAll(p.cfg.URLTemplate, "{reference}", reference)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return p.unknown("could not build request"), nil
	}
	req.Header.Set("Accept", "application/json")
	if p.cfg.AuthHeader != "" {
		req.Header.Set(p.cfg.AuthHeader, p.authValue())
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return p.unknown("provider unreachable"), nil
	}
	defer func() { _ = resp.Body.Close() }()

	// A provider that has no record of a transfer we believe we initiated is
	// itself evidence: it never happened.
	if resp.StatusCode == http.StatusNotFound {
		return Status{
			Outcome:    NotFound,
			Provider:   p.cfg.ProviderName,
			Reference:  reference,
			ObservedAt: time.Now().UTC(),
			Detail:     "provider has no record of this transaction",
		}, nil
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return p.unknown(fmt.Sprintf("provider returned HTTP %d", resp.StatusCode)), nil
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return p.unknown("could not read provider response"), nil
	}

	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		return p.unknown("provider response was not valid JSON"), nil
	}

	raw, ok := lookup(decoded, p.cfg.StatusPath)
	if !ok {
		return p.unknown("status field " + p.cfg.StatusPath + " missing from response"), nil
	}
	value, ok := raw.(string)
	if !ok {
		return p.unknown("status field " + p.cfg.StatusPath + " was not a string"), nil
	}

	normalised := strings.ToLower(strings.TrimSpace(value))
	status := Status{
		Provider:   p.cfg.ProviderName,
		Reference:  reference,
		ObservedAt: time.Now().UTC(),
	}

	switch {
	case contains(p.settled, normalised):
		// Checked before believing the good news: a settled response for a
		// different amount is not this transaction settling.
		if detail, ok := p.amountDisagrees(decoded, ref); ok {
			status.Outcome = Unknown
			status.Detail = detail
			break
		}
		status.Outcome = Settled
		status.Detail = "provider reports settled"
	case contains(p.failed, normalised):
		status.Outcome = Failed
		status.Detail = "provider reports failed"
	default:
		// A status we do not recognise is not a verdict. Pending, processing,
		// queued and anything new the provider invents all land here.
		status.Outcome = Unknown
		status.Detail = "unrecognised provider status"
	}
	return status, nil
}

// authValue renders the credential into the configured template.
func (p *HTTPProvider) authValue() string {
	if strings.Contains(p.cfg.AuthValue, "{value}") {
		return strings.ReplaceAll(p.cfg.AuthValue, "{value}", p.cfg.AuthCredential)
	}
	// No template: the configured value is used verbatim, which is what every
	// existing config does.
	return p.cfg.AuthValue
}

// amountDisagrees reports whether the provider's amount contradicts ours.
//
// Silent when either side is unknown: a missing or unparseable amount must not
// turn a real settlement into an investigation, because the cost of that is a
// reversal we should not send.
func (p *HTTPProvider) amountDisagrees(decoded map[string]any, ref Ref) (string, bool) {
	if p.cfg.AmountPath == "" || ref.AmountMinor <= 0 {
		return "", false
	}
	raw, ok := lookup(decoded, p.cfg.AmountPath)
	if !ok {
		return "", false
	}

	var got int64
	switch v := raw.(type) {
	case float64:
		got = int64(v)
	case string:
		got = parseMinor(v)
	default:
		return "", false
	}
	if got <= 0 || got == ref.AmountMinor {
		return "", false
	}
	return fmt.Sprintf("provider reports %d settled, we recorded %d", got, ref.AmountMinor), true
}

func contains(set map[string]struct{}, v string) bool {
	_, ok := set[v]
	return ok
}

func (p *HTTPProvider) unknown(detail string) Status {
	return Status{
		Outcome:    Unknown,
		Provider:   p.cfg.ProviderName,
		ObservedAt: time.Now().UTC(),
		Detail:     detail,
	}
}

// lookup walks a dotted path through decoded JSON.
//
// A segment may be an array index — "data.0.status" — for rails that answer a
// reference lookup with a list. Flutterwave is the one that made this
// necessary: its transfer search returns data as an array rather than an
// object.
//
// An array reached without an index resolves only when it holds exactly one
// element. That rule is the point: a search returning several transfers for one
// reference is ambiguous, and picking the first would be a guess about which
// one is ours. Ambiguity ends as Unknown here, the same as everywhere else in
// this package, because the alternative is a verdict that moves money.
func lookup(doc map[string]any, path string) (any, bool) {
	var current any = doc
	for _, segment := range strings.Split(path, ".") {
		if arr, ok := current.([]any); ok {
			next, ok := indexArray(arr, segment)
			if !ok {
				return nil, false
			}
			current = next
			continue
		}

		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = obj[segment]
		if !ok {
			return nil, false
		}
	}

	// A path ending on a single-element array resolves to that element, so
	// "data.status" works against both shapes.
	if arr, ok := current.([]any); ok && len(arr) == 1 {
		return arr[0], true
	}
	return current, true
}

// indexArray resolves one path segment against an array.
func indexArray(arr []any, segment string) (any, bool) {
	if i, err := strconv.Atoi(segment); err == nil {
		if i < 0 || i >= len(arr) {
			return nil, false
		}
		return arr[i], true
	}

	// Not an index, so the segment addresses a field of the element — which is
	// only unambiguous when there is exactly one.
	if len(arr) != 1 {
		return nil, false
	}
	obj, ok := arr[0].(map[string]any)
	if !ok {
		return nil, false
	}
	v, ok := obj[segment]
	return v, ok
}
