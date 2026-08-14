// Package licence records what a customer has paid for, and withholds the
// artefacts when they have not.
//
// What it deliberately does not do is stop reconciling. Blocking ingest would
// mean new debits are never observed, so the customer believes they are covered
// and is not — silent, and on our signature. Blocking credits would be worse:
// every in-flight debit would reach its window with no credit, orphan, and fire
// a spurious reversal, which is the exact double-payment this product exists to
// prevent. Expiry that generates incidents is not a commercial control, it is a
// liability.
//
// So expiry withholds the compliance report, the exposure report, the provider
// scorecard and audit verification — the artefacts a compliance officer renews
// for. The safety keeps running whether or not the invoice was paid.
//
// It is also, honestly, theatre. The customer compiles this source; one line
// defeats it permanently. That is why there is exactly one check location:
// scattering checks through the codebase would add bugs, not security.
package licence

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// WarnBefore is how long before expiry the countdown starts.
const WarnBefore = 30 * 24 * time.Hour

// Licence is what was signed.
type Licence struct {
	Customer  string    `json:"customer"`
	Plan      string    `json:"plan,omitempty"`
	IssuedAt  time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// Token is the wire format: base64(payload).base64(signature).
//
// Signed with Ed25519 rather than an HMAC for the same reason checkpoints are:
// the binary verifies with a public key, so shipping the verifier to a customer
// does not ship the ability to mint licences.
type Token string

var (
	// ErrMalformed means the token is not a licence at all.
	ErrMalformed = errors.New("licence: malformed token")

	// ErrSignature means the token was not signed by the expected key.
	ErrSignature = errors.New("licence: signature does not verify")
)

// Issue signs a licence. Vendor side only.
func Issue(l Licence, privateKey ed25519.PrivateKey) (Token, error) {
	if l.Customer == "" {
		return "", errors.New("licence: customer is required")
	}
	if l.ExpiresAt.IsZero() {
		return "", errors.New("licence: expires_at is required")
	}
	l.IssuedAt = l.IssuedAt.UTC().Truncate(time.Second)
	l.ExpiresAt = l.ExpiresAt.UTC().Truncate(time.Second)

	payload, err := json.Marshal(l)
	if err != nil {
		return "", fmt.Errorf("licence: encode: %w", err)
	}
	sig := ed25519.Sign(privateKey, payload)
	return Token(base64.RawURLEncoding.EncodeToString(payload) + "." +
		base64.RawURLEncoding.EncodeToString(sig)), nil
}

// Parse verifies a token and returns what it says.
func Parse(t Token, publicKey string) (Licence, error) {
	pub, err := decodePublicKey(publicKey)
	if err != nil {
		return Licence{}, err
	}

	parts := strings.Split(string(t), ".")
	if len(parts) != 2 {
		return Licence{}, ErrMalformed
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return Licence{}, ErrMalformed
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return Licence{}, ErrMalformed
	}

	// Verified before decoding, so a malformed payload cannot be reached by
	// anyone who cannot sign.
	if !ed25519.Verify(pub, payload, sig) {
		return Licence{}, ErrSignature
	}

	var l Licence
	if err := json.Unmarshal(payload, &l); err != nil {
		return Licence{}, ErrMalformed
	}
	return l, nil
}

// decodePublicKey accepts base64 with or without padding.
//
// The padded form ends in "=", which is the character that separates a shell
// KEY=VALUE line — so it is genuinely easy to end up with the padding stripped
// by a copy-paste or an awk one-liner. Refusing that with "must be a base64
// Ed25519 key" would send someone hunting for the wrong problem.
func decodePublicKey(s string) ([]byte, error) {
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding} {
		if raw, err := enc.DecodeString(s); err == nil && len(raw) == ed25519.PublicKeySize {
			return raw, nil
		}
	}
	return nil, errors.New("licence: public key must be a base64 Ed25519 key")
}

// Status is what the rest of the system asks about.
type Status struct {
	// Licensed is false when no licence is configured at all. That is not an
	// error: a deployment with no licence behaves exactly as it did before
	// licensing existed, which is what keeps this backwards compatible and what
	// makes the check opt-in rather than a trap on upgrade.
	Licensed bool `json:"licensed"`

	Customer  string     `json:"customer,omitempty"`
	Plan      string     `json:"plan,omitempty"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`

	// Expired says the artefacts are withheld. Detection is unaffected.
	Expired bool `json:"expired"`

	// DaysRemaining counts down and goes negative after expiry, so a support
	// conversation can say how long ago rather than just "expired".
	DaysRemaining int `json:"days_remaining"`

	// Notice is the sentence to show a human. Empty when there is nothing to
	// say, which is most of the time.
	Notice string `json:"notice,omitempty"`
}

// Checker answers the one question the rest of the system asks.
//
// One instance, consulted in one place. The review that shaped this was explicit
// that scattering licence checks through a codebase adds bugs rather than
// security, and the thing being protected is a report, not a payment.
type Checker struct {
	licence   *Licence
	publicKey string
	now       func() time.Time
}

// Options configures a Checker.
type Options struct {
	// Token is the licence. Empty means unlicensed, which is permitted.
	Token Token

	// PublicKey verifies the token.
	PublicKey string

	Now func() time.Time
}

// New builds a Checker.
//
// A token that will not verify is a startup error rather than a silent
// downgrade to unlicensed. A customer who pasted a corrupted key deserves to
// find out immediately, not to discover their reports missing during an audit.
func New(opts Options) (*Checker, error) {
	c := &Checker{publicKey: opts.PublicKey, now: opts.Now}
	if c.now == nil {
		c.now = time.Now
	}
	if opts.Token == "" {
		return c, nil
	}
	if opts.PublicKey == "" {
		return nil, errors.New("licence: a token was configured but no public key to verify it")
	}

	l, err := Parse(opts.Token, opts.PublicKey)
	if err != nil {
		return nil, err
	}
	c.licence = &l
	return c, nil
}

// Status reports where the licence stands.
func (c *Checker) Status() Status {
	if c == nil || c.licence == nil {
		return Status{}
	}

	now := c.now().UTC()
	expires := c.licence.ExpiresAt.UTC()
	remaining := expires.Sub(now)

	// Days rounded towards zero on the safe side: with 23 hours left this says
	// 0 days, not 1. A countdown that rounds up tells someone they have a day
	// they do not have.
	days := int(remaining / (24 * time.Hour))

	s := Status{
		Licensed:      true,
		Customer:      c.licence.Customer,
		Plan:          c.licence.Plan,
		ExpiresAt:     &expires,
		DaysRemaining: days,
		Expired:       !now.Before(expires),
	}

	switch {
	case s.Expired:
		s.Notice = fmt.Sprintf("licence expired on %s; reports and audit verification are withheld. "+
			"Detection, reversals and ingest are unaffected and still running.",
			expires.Format("2006-01-02"))
	case remaining <= WarnBefore:
		s.Notice = fmt.Sprintf("licence expires on %s, in %d days.",
			expires.Format("2006-01-02"), days)
	}
	return s
}

// ArtefactsAvailable reports whether the reports and audit verification may be
// served.
//
// The single check location. Everything commercial hangs off this one method,
// and nothing else in the codebase asks about licensing.
func (c *Checker) ArtefactsAvailable() bool {
	return !c.Status().Expired
}
