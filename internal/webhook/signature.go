// Package webhook signs, schedules and delivers outbound notifications (§7.2).
package webhook

import (
	"time"

	"github.com/nobledeveloper01/ReconSync/pkg/reconsync"
)

// The signing scheme lives in pkg/reconsync, the public client library.
//
// One implementation, not two. A receiver has to verify exactly what the sender
// signed, and the surest way to have those disagree is to write them twice.
// These aliases keep the server's own call sites unchanged.
const (
	SignatureHeader  = reconsync.SignatureHeader
	EventHeader      = reconsync.EventHeader
	DeliveryHeader   = reconsync.DeliveryHeader
	DrillHeader      = reconsync.DrillHeader
	DefaultTolerance = reconsync.DefaultTolerance
)

var (
	ErrMalformedSignature = reconsync.ErrMalformedSignature
	ErrSignatureMismatch  = reconsync.ErrSignatureMismatch
	ErrSignatureExpired   = reconsync.ErrSignatureExpired
)

// Sign returns the header value for a payload.
func Sign(secret string, at time.Time, body []byte) string {
	return reconsync.Sign(secret, at, body)
}

// SignWith signs with several secrets, emitting one v1 per secret.
func SignWith(secrets []string, at time.Time, body []byte) string {
	return reconsync.SignWith(secrets, at, body)
}

// VerifyAny checks the header against several candidate secrets.
func VerifyAny(secrets []string, header string, body []byte, now time.Time, tolerance time.Duration) error {
	return reconsync.VerifyAny(secrets, header, body, now, tolerance)
}

// Verify checks a signature header against a payload.
func Verify(secret, header string, body []byte, now time.Time, tolerance time.Duration) error {
	return reconsync.Verify(secret, header, body, now, tolerance)
}
