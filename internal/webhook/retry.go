package webhook

import (
	"math/rand/v2"
	"time"
)

// retrySchedule is the backoff from §7.2 C1: immediate, then widening until a
// human is involved. The final entry is roughly six hours after the first
// attempt, which is the delivery SLO window in §11.3.
// An array, not a slice, so len() below is a compile-time constant.
var retrySchedule = [...]time.Duration{
	0,
	30 * time.Second,
	2 * time.Minute,
	10 * time.Minute,
	time.Hour,
	6 * time.Hour,
}

// MaxAttempts is how many times a delivery is tried before dead-lettering.
const MaxAttempts = len(retrySchedule)

// jitterFraction spreads retries so a customer endpoint recovering from an
// outage is not hit by every pending delivery at the same instant.
const jitterFraction = 0.2

// RetryDelay returns the base wait before the given attempt, counting from 0.
// Attempts beyond the schedule return the final delay.
func RetryDelay(attempt int) time.Duration {
	switch {
	case attempt < 0:
		return 0
	case attempt >= len(retrySchedule):
		return retrySchedule[len(retrySchedule)-1]
	default:
		return retrySchedule[attempt]
	}
}

// ShouldRetry reports whether another attempt remains after this one.
func ShouldRetry(attempt int) bool {
	return attempt+1 < MaxAttempts
}

// NextRetryAt returns when an attempt should next be tried, with jitter applied.
func NextRetryAt(now time.Time, attempt int) time.Time {
	return now.Add(jitter(RetryDelay(attempt)))
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	// Symmetric around the base delay, so the schedule neither drifts later on
	// every retry nor bunches earlier.
	spread := float64(d) * jitterFraction

	// Retry spread carries no security property — it exists so a recovering
	// endpoint is not hit by every pending delivery at once. math/rand/v2 is the
	// right tool; crypto/rand here would be cargo-culting.
	offset := (rand.Float64()*2 - 1) * spread //nolint:gosec // scheduling jitter, not a secret

	out := time.Duration(float64(d) + offset)
	if out < 0 {
		return 0
	}
	return out
}

// RetryableStatus reports whether an HTTP status is worth retrying.
//
// 4xx other than 408 and 429 mean the request itself is wrong: retrying a 400 or
// a 401 six times over six hours just repeats the same rejection.
func RetryableStatus(code int) bool {
	switch {
	case code >= 500:
		return true
	case code == 408, code == 429:
		return true
	default:
		return false
	}
}
