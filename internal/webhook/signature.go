// Package webhook signs, schedules and delivers outbound notifications (§7.2).
package webhook

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
	ErrMalformedSignature = errors.New("webhook: malformed signature header")
	ErrSignatureMismatch  = errors.New("webhook: signature does not match")
	ErrSignatureExpired   = errors.New("webhook: signature timestamp outside tolerance")
)

// Sign returns the header value for a payload.
//
// The scheme is Stripe's on purpose: signing "{timestamp}.{body}" rather than
// the body alone is what stops a captured request being replayed with a fresh
// timestamp, and it is a design receivers already understand.
func Sign(secret string, at time.Time, body []byte) string {
	ts := at.Unix()
	return fmt.Sprintf("t=%d,v1=%s", ts, computeSignature(secret, ts, body))
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

	want := computeSignature(secret, ts, body)
	// Compared as raw bytes in constant time; a byte-by-byte compare would leak
	// how much of the signature was correct.
	if !hmac.Equal([]byte(want), []byte(provided)) {
		return ErrSignatureMismatch
	}
	return nil
}

func parseSignature(header string) (ts int64, signature string, err error) {
	if header == "" {
		return 0, "", ErrMalformedSignature
	}

	for _, part := range strings.Split(header, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(part), "=")
		if !found {
			return 0, "", ErrMalformedSignature
		}
		switch key {
		case "t":
			ts, err = strconv.ParseInt(value, 10, 64)
			if err != nil {
				return 0, "", ErrMalformedSignature
			}
		case "v1":
			signature = value
		}
	}

	if ts == 0 || signature == "" {
		return 0, "", ErrMalformedSignature
	}
	return ts, signature, nil
}
