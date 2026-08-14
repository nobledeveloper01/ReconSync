package tests

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/licence"
)

// issueLicence mints one expiring at the given offset from now.
func issueLicence(t *testing.T, expiresIn time.Duration) (licence.Token, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	token, err := licence.Issue(licence.Licence{
		Customer:  "Acme Fintech",
		Plan:      "compliance",
		IssuedAt:  time.Now().UTC(),
		ExpiresAt: time.Now().UTC().Add(expiresIn),
	}, priv)
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return token, base64.StdEncoding.EncodeToString(pub)
}

func checkerFor(t *testing.T, expiresIn time.Duration) *licence.Checker {
	t.Helper()
	token, pub := issueLicence(t, expiresIn)
	c, err := licence.New(licence.Options{Token: token, PublicKey: pub})
	if err != nil {
		t.Fatalf("licence.New: %v", err)
	}
	return c
}

// The whole point of the answer to Q2: expiry withholds the artefacts and never
// touches the thing that keeps a customer's money safe.
func TestExpiredLicenceWithholdsReportsButNotDetection(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{licence: checkerFor(t, -time.Hour)})
	ctx := context.Background()

	// Every artefact is withheld.
	for _, path := range []string{
		"/v1/reports/reversal-compliance",
		"/v1/reports/exposure",
		"/v1/reports/providers",
		"/v1/audit/verify",
	} {
		w := f.do(t, http.MethodGet, path, f.keyA, nil)
		// 402, not 403: this is not an authorisation failure, and calling it
		// one would send an operator hunting for a permissions bug.
		if w.Code != http.StatusPaymentRequired {
			t.Errorf("%s = %d, want 402", path, w.Code)
		}
		body, _ := decodeBody(t, w)["error"].(map[string]any)
		if msg, _ := body["message"].(string); !strings.Contains(msg, "Detection") {
			t.Errorf("%s message = %q, want it to say detection still runs", path, msg)
		}
	}

	// And the safety keeps running. Reporting a debit still works.
	now := time.Now().UTC()
	w := f.do(t, http.MethodPost, "/v1/events/debit", f.keyA, map[string]any{
		"transaction_id": "TX-1", "transaction_type": "transfer", "provider": "paystack",
		"amount_minor": 5000000, "currency": "NGN", "debit_at": now.Format(time.RFC3339),
		"customer_ref": "usr", "idempotency_key": "idem-1",
	})
	if w.Code != http.StatusAccepted {
		t.Fatalf("ingest with an expired licence = %d, want 202: %s", w.Code, w.Body.String())
	}

	// Detection still reverses. An expiry that stopped this would generate the
	// exact double-payment the product exists to prevent.
	newEndpoint(t, f.store, tenantA, "we_1", "https://customer.example.com/hook")
	mustUpsert(t, f.store, newExpiredTxn(tenantA, "TX-OLD", 5*time.Minute, time.Minute))
	res, err := newDetector(t, f.store).Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Queued != 1 {
		t.Errorf("queued %d reversals with an expired licence, want 1", res.Queued)
	}

	// Reversal confirmations still work: a customer must be able to close out
	// what they were told to reverse.
	if w := f.do(t, http.MethodPost, "/v1/events/reversal-completed", f.keyA,
		map[string]any{"transaction_id": "TX-OLD"}); w.Code != http.StatusOK {
		t.Errorf("reversal-completed = %d, want 200", w.Code)
	}
}

func TestValidLicenceServesEverything(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{licence: checkerFor(t, 90*24*time.Hour)})

	for _, path := range []string{
		"/v1/reports/reversal-compliance",
		"/v1/reports/exposure",
		"/v1/reports/providers",
		"/v1/audit/verify",
	} {
		if w := f.do(t, http.MethodGet, path, f.keyA, nil); w.Code != http.StatusOK {
			t.Errorf("%s = %d, want 200: %s", path, w.Code, w.Body.String())
		}
	}
}

// The countdown you asked for: thirty days out, and counting down.
func TestLicenceCountdown(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{licence: checkerFor(t, 20*24*time.Hour)})

	w := f.do(t, http.MethodGet, "/v1/licence", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("/v1/licence = %d", w.Code)
	}
	body := decodeBody(t, w)
	if body["licensed"] != true {
		t.Error("licensed = false with a valid licence")
	}
	if body["days_remaining"] != float64(19) && body["days_remaining"] != float64(20) {
		t.Errorf("days_remaining = %v, want about 20", body["days_remaining"])
	}
	// Inside the thirty-day window, so it says so.
	if notice, _ := body["notice"].(string); !strings.Contains(notice, "expires") {
		t.Errorf("notice = %q, want the expiry warning", notice)
	}
	if body["customer"] != "Acme Fintech" {
		t.Errorf("customer = %v", body["customer"])
	}
}

// A customer whose reports have stopped needs this endpoint most. Withholding
// the explanation along with the artefact would be the worst time to go quiet.
func TestLicenceEndpointWorksWhenExpired(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{licence: checkerFor(t, -48*time.Hour)})

	w := f.do(t, http.MethodGet, "/v1/licence", f.keyA, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("/v1/licence = %d, want 200 even when expired", w.Code)
	}
	body := decodeBody(t, w)
	if body["expired"] != true {
		t.Error("expired = false on an expired licence")
	}
	// Negative, so support can say how long ago rather than just "expired".
	if days, _ := body["days_remaining"].(float64); days >= 0 {
		t.Errorf("days_remaining = %v, want negative after expiry", days)
	}
}

// No licence configured behaves exactly as before licensing existed. Defaulting
// the other way would have locked out every deployment on upgrade.
func TestNoLicenceServesEverything(t *testing.T) {
	f := newIngestFixture(t, fixtureOpts{})

	if w := f.do(t, http.MethodGet, "/v1/reports/exposure", f.keyA, nil); w.Code != http.StatusOK {
		t.Errorf("exposure with no licence = %d, want 200", w.Code)
	}
	body := decodeBody(t, f.do(t, http.MethodGet, "/v1/licence", f.keyA, nil))
	if body["licensed"] != false {
		t.Errorf("licensed = %v, want false with none configured", body["licensed"])
	}
}

// A licence anyone could mint is not a licence.
func TestLicenceCannotBeForged(t *testing.T) {
	token, realPub := issueLicence(t, 24*time.Hour)

	otherPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	if _, err := licence.Parse(token, base64.StdEncoding.EncodeToString(otherPub)); err == nil {
		t.Error("a licence verified against the wrong public key")
	}

	// Editing the payload breaks it, which is what stops a customer extending
	// their own expiry.
	parts := strings.SplitN(string(token), ".", 2)
	if _, err := licence.Parse(licence.Token("eyJjdXN0b21lciI6IngifQ."+parts[1]), realPub); err == nil {
		t.Error("an edited licence still verified")
	}

	// A corrupted token stops startup rather than silently downgrading to
	// unlicensed, which would hand out the artefacts for free.
	if _, err := licence.New(licence.Options{Token: "not-a-licence", PublicKey: realPub}); err == nil {
		t.Error("a malformed token was accepted")
	}
	// And a token with no key to verify it is a configuration error.
	if _, err := licence.New(licence.Options{Token: token}); err == nil {
		t.Error("a token was accepted with no public key")
	}
}

// A countdown that rounds up tells someone they have a day they do not have.
func TestLicenceDaysRoundTowardsZero(t *testing.T) {
	c := checkerFor(t, 23*time.Hour)
	if got := c.Status().DaysRemaining; got != 0 {
		t.Errorf("days_remaining = %d with 23 hours left, want 0", got)
	}
	if c.Status().Expired {
		t.Error("a licence with 23 hours left is not expired")
	}
}

// The padded form ends in "=", the character that separates a shell KEY=VALUE
// line, so losing the padding to a copy-paste is genuinely easy.
func TestLicenceAcceptsUnpaddedPublicKey(t *testing.T) {
	token, padded := issueLicence(t, 24*time.Hour)
	unpadded := strings.TrimRight(padded, "=")
	if unpadded == padded {
		t.Skip("this key happened to need no padding")
	}
	if _, err := licence.Parse(token, unpadded); err != nil {
		t.Errorf("an unpadded public key was rejected: %v", err)
	}
}
