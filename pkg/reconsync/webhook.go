// Package reconsync is the client library: everything an integrating service
// needs, and nothing that only the server needs.
//
// It is a public package rather than part of internal/ for one concrete reason.
// Verifying a webhook signature is the single security-critical thing every
// receiver must do, ReconSync already had a correct implementation, and being
// under internal/ meant no customer could import it — so every Go integration
// would have written its own, which is where timing leaks and missing replay
// checks come from. Shipping the tested one costs nothing.
//
// This package imports only the standard library, so adding it to a payment
// service brings in no transitive dependencies to review.
package reconsync

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// SignatureHeader carries the timestamp and signature (§7.2).
const SignatureHeader = "X-ReconSync-Signature"

const (
	// EventHeader names the event type, so a receiver can route without parsing.
	EventHeader = "X-ReconSync-Event"

	// DeliveryHeader is the delivery id, for idempotent receipt and support.
	DeliveryHeader = "X-ReconSync-Delivery"

	// DrillHeader is set only on a fire drill, so a handler can refuse the
	// payload before parsing it. Never set on a real event.
	DrillHeader = "X-ReconSync-Drill"

	// DefaultTolerance bounds replay of a captured request. Wide enough for real
	// clock skew, narrow enough that a captured payload expires quickly.
	DefaultTolerance = 5 * time.Minute
)

var (
	ErrMalformedSignature = errors.New("reconsync: malformed signature header")
	ErrSignatureMismatch  = errors.New("reconsync: signature does not match")
	ErrSignatureExpired   = errors.New("reconsync: signature timestamp outside tolerance")
)

// Sign returns the header value for a payload.
//
// The scheme is Stripe's on purpose: signing "{timestamp}.{body}" rather than
// the body alone is what stops a captured request being replayed with a fresh
// timestamp, and it is a design receivers already understand.
func Sign(secret string, at time.Time, body []byte) string {
	return SignWith([]string{secret}, at, body)
}

// SignWith signs with several secrets at once, emitting one v1 per secret.
//
// This is what makes rotation possible without a coordinated cutover. During
// the window the payload carries a signature for the old secret and the new
// one, so a receiver holding either verifies it, and the two systems can be
// changed on different days by different people.
func SignWith(secrets []string, at time.Time, body []byte) string {
	ts := at.Unix()

	var b strings.Builder
	fmt.Fprintf(&b, "t=%d", ts)
	for _, secret := range secrets {
		fmt.Fprintf(&b, ",v1=%s", computeSignature(secret, ts, body))
	}
	return b.String()
}

func computeSignature(secret string, ts int64, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(ts, 10)))
	mac.Write([]byte("."))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// Verify checks a signature header against a payload.
//
// Shipped because every SDK needs it and a receiver rolling their own is where
// timing leaks and missing replay checks come from.
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	return VerifyAny([]string{secret}, header, body, now, tolerance)
}

// VerifyAny checks the header against several candidate secrets.
//
// The other half of rotation. A sender that has already moved to a new secret
// and a receiver that has not yet been redeployed both keep working while
// either side holds both values.
func VerifyAny(secrets []string, header string, body []byte, now time.Time, tolerance time.Duration) error {
	ts, provided, err := parseSignature(header)
	if err != nil {
		return err
	}

	if tolerance > 0 {
		age := now.Sub(time.Unix(ts, 0))
		if age < 0 {
			age = -age
		}
		if age > tolerance {
			return ErrSignatureExpired
		}
	}

	// Every combination is compared even after one matches. Returning early
	// would make the time taken depend on which secret and which signature
	// matched, and the point of a constant-time compare is that it does not.
	matched := false
	for _, secret := range secrets {
		want := []byte(computeSignature(secret, ts, body))
		for _, candidate := range provided {
			// Compared as raw bytes in constant time; a byte-by-byte compare
			// would leak how much of the signature was correct.
			if hmac.Equal(want, []byte(candidate)) {
				matched = true
			}
		}
	}

	if !matched {
		return ErrSignatureMismatch
	}
	return nil
}

// parseSignature reads the timestamp and every signature the header carries.
//
// Several v1 entries is the normal state during a rotation, so they are all
// collected. Keeping only the last — which is what a single-value parse does —
// would make the sender's ordering decide whether the receiver worked.
func parseSignature(header string) (ts int64, signatures []string, err error) {
	if header == "" {
		return 0, nil, ErrMalformedSignature
	}

	for _, part := range strings.Split(header, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			return 0, nil, ErrMalformedSignature
		}
		switch key {
		case "t":
			ts, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, nil, ErrMalformedSignature
			}
		case "v1":
			if value != "" {
				signatures = append(signatures, value)
			}
		}
	}

	if ts == 0 || len(signatures) == 0 {
		return 0, nil, ErrMalformedSignature
	}
	return ts, signatures, nil
}
