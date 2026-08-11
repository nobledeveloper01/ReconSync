package domain

import (
	"fmt"
	"strings"
)

// Server-side half of the §8.4 data rules. The SDK strips these at source; this
// repeats the work, because a control that only runs in the customer's code is
// not a control.

const (
	minPANLen = 13 // ISO/IEC 7812 account number bounds
	maxPANLen = 19

	maxScanLen   = 4096 // bound work on attacker-controlled input
	maxScanDepth = 8    // bound recursion into nested JSON
)

// denylistedFields are metadata keys that must never carry a value in. Matched
// on a normalised key, so card_number / cardNumber / Card-Number all collide.
var denylistedFields = map[string]struct{}{
	"pan": {}, "cardnumber": {}, "card": {}, "cardno": {}, "accountnumber": {},
	"cvv": {}, "cvv2": {}, "cvc": {}, "csc": {}, "securitycode": {},
	"expiry": {}, "expirydate": {}, "expmonth": {}, "expyear": {},
	"password": {}, "passwd": {}, "pin": {}, "otp": {},
	"bvn": {}, "nin": {}, "ssn": {}, "dateofbirth": {}, "dob": {},
	"secret": {}, "token": {}, "accesstoken": {}, "refreshtoken": {},
	"authorization": {}, "apikey": {}, "privatekey": {},
	"mothersmaiden": {}, "trackdata": {}, "magstripe": {}, "cardholdername": {},
}

// normaliseKey lowercases and strips separators so naming style can't defeat
// the denylist.
func normaliseKey(k string) string {
	var b strings.Builder
	b.Grow(len(k))
	for _, r := range k {
		switch {
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + ('a' - 'A'))
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
		}
	}
	return b.String()
}

// SensitiveDataError names the path that tripped the screen, never the value —
// echoing it into logs would defeat the control.
type SensitiveDataError struct {
	Path   string
	Reason string
}

func (e SensitiveDataError) Error() string {
	return fmt.Sprintf("payload rejected at %q: %s", e.Path, e.Reason)
}

// ScreenMetadata rejects a metadata bag carrying a denylisted field or a
// card-like value.
func ScreenMetadata(m map[string]any) error {
	return screenValue("metadata", m, 0)
}

// ScreenString checks a single client-populated field, such as
// provider_reference.
func ScreenString(field, v string) error {
	if ContainsCardNumber(v) {
		return SensitiveDataError{Path: field, Reason: "value contains what appears to be a card number"}
	}
	return nil
}

func screenValue(path string, v any, depth int) error {
	if depth > maxScanDepth {
		return SensitiveDataError{Path: path, Reason: "nested more than 8 levels deep"}
	}

	switch val := v.(type) {
	case map[string]any:
		for k, child := range val {
			if _, blocked := denylistedFields[normaliseKey(k)]; blocked {
				return SensitiveDataError{Path: path + "." + k, Reason: "field name is on the never-collect list"}
			}
			if err := screenValue(path+"."+k, child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for i, child := range val {
			if err := screenValue(fmt.Sprintf("%s[%d]", path, i), child, depth+1); err != nil {
				return err
			}
		}
	case string:
		if ContainsCardNumber(val) {
			return SensitiveDataError{Path: path, Reason: "value contains what appears to be a card number"}
		}
	}
	return nil
}

// ContainsCardNumber reports whether s holds a 13-19 digit Luhn-valid run (§8.4).
// Spaces and hyphens between digits are treated as formatting.
func ContainsCardNumber(s string) bool {
	if len(s) > maxScanLen {
		s = s[:maxScanLen]
	}

	var run []byte
	flush := func() bool {
		defer func() { run = run[:0] }()
		return runIsPAN(run)
	}

	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9':
			run = append(run, c)
		case (c == ' ' || c == '-') && len(run) > 0:
			if i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9' {
				continue // formatting inside a number
			}
			if flush() {
				return true
			}
		default:
			if flush() {
				return true
			}
		}
	}
	return flush()
}

// runIsPAN matches the whole digit run, not every substring of it.
//
// Sliding a window gives a 16-digit run ~10 chances to pass Luhn, which rejects
// most long numeric identifiers. Whole-run matching holds the false-positive
// rate at ~1 in 10 for identifiers that are themselves 13-19 digits. The cost is
// that a PAN buried in a longer digit blob isn't caught here; the field denylist
// and SDK stripping cover that. A screen that rejects real traffic gets turned
// off, and a control that's off detects nothing.
func runIsPAN(run []byte) bool {
	if len(run) < minPANLen || len(run) > maxPANLen {
		return false
	}
	return luhnValid(run)
}

func luhnValid(digits []byte) bool {
	if len(digits) == 0 {
		return false
	}
	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		c := digits[i]
		if c < '0' || c > '9' {
			return false
		}
		d := int(c - '0')
		if double {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}
