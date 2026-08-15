package account

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha1" //nolint:gosec // RFC 6238 specifies SHA-1; authenticators implement that
	"crypto/subtle"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// TOTP, implemented against RFC 6238 rather than pulled in.
//
// It is HMAC over a counter and a modulo — about sixty lines — and this project
// treats every dependency as one a customer's security team has to approve.
// SHA-1 is not a choice here: the RFC specifies it and every authenticator app
// implements that, so a "stronger" hash would simply not work with Google
// Authenticator, 1Password or Authy.

// TOTPPeriod is the RFC default step.
const TOTPPeriod = 30 * time.Second

// TOTPSkew is how many steps either side of now are accepted.
//
// One step, so a code stays valid for at most ninety seconds. Wider windows are
// tempting when someone's phone clock drifts, but each extra step is another
// code an attacker may guess and another minute a shoulder-surfed code lives.
const TOTPSkew = 1

var totpEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

// NewTOTPSecret mints a secret for enrolment.
func NewTOTPSecret() (string, error) {
	var b [20]byte // 160 bits, the RFC 4226 recommendation for SHA-1
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("account: generate totp secret: %w", err)
	}
	return totpEncoding.EncodeToString(b[:]), nil
}

// TOTPCode computes the code for a secret at a time.
func TOTPCode(secret string, at time.Time) (string, error) {
	key, err := totpEncoding.DecodeString(strings.ToUpper(strings.TrimSpace(secret)))
	if err != nil {
		return "", fmt.Errorf("account: totp secret is not base32: %w", err)
	}

	// A clock before 1970 would wrap the counter to an enormous value and
	// produce a code nothing can match, with no hint as to why.
	seconds := at.UTC().Unix()
	if seconds < 0 {
		return "", fmt.Errorf("account: the clock reads %s, which is before the TOTP epoch", at.UTC())
	}
	counter := uint64(seconds) / uint64(TOTPPeriod.Seconds())
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], counter)

	mac := hmac.New(sha1.New, key)
	mac.Write(buf[:])
	sum := mac.Sum(nil)

	// Dynamic truncation, RFC 4226 §5.3.
	offset := sum[len(sum)-1] & 0x0f
	value := binary.BigEndian.Uint32(sum[offset:offset+4]) & 0x7fffffff
	return fmt.Sprintf("%06d", value%1_000_000), nil
}

// VerifyTOTP checks a code against the secret, allowing for clock skew.
func VerifyTOTP(secret, code string, now time.Time) bool {
	code = strings.TrimSpace(code)
	if len(code) != 6 {
		return false
	}

	for step := -TOTPSkew; step <= TOTPSkew; step++ {
		want, err := TOTPCode(secret, now.Add(time.Duration(step)*TOTPPeriod))
		if err != nil {
			return false
		}
		// Constant time: a byte-by-byte comparison leaks how much of the code
		// was right, which turns a million guesses into a handful.
		if subtle.ConstantTimeCompare([]byte(want), []byte(code)) == 1 {
			return true
		}
	}
	return false
}

// TOTPURI is what an authenticator app scans.
func TOTPURI(issuer, email, secret string) string {
	label := url.PathEscape(issuer + ":" + email)
	q := url.Values{}
	q.Set("secret", secret)
	q.Set("issuer", issuer)
	q.Set("algorithm", "SHA1")
	q.Set("digits", "6")
	q.Set("period", fmt.Sprintf("%d", int(TOTPPeriod.Seconds())))
	return "otpauth://totp/" + label + "?" + q.Encode()
}

// RecoveryCodeCount is how many single-use codes are issued with 2FA.
const RecoveryCodeCount = 10

// NewRecoveryCodes mints codes for the lost-phone case.
//
// Without these, enabling 2FA is a way to lose your own account. They are shown
// once and stored hashed, because a recovery code is a credential that bypasses
// the second factor entirely.
func NewRecoveryCodes() ([]string, error) {
	// Crockford-ish alphabet: no O/0 or I/1 confusion when someone writes one
	// on paper, which is exactly what people do with recovery codes.
	const alphabet = "abcdefghjkmnpqrstuvwxyz23456789"

	codes := make([]string, 0, RecoveryCodeCount)
	for i := 0; i < RecoveryCodeCount; i++ {
		var raw [10]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, fmt.Errorf("account: generate recovery code: %w", err)
		}
		var sb strings.Builder
		for j, b := range raw {
			if j == 5 {
				sb.WriteByte('-')
			}
			sb.WriteByte(alphabet[int(b)%len(alphabet)])
		}
		codes = append(codes, sb.String())
	}
	return codes, nil
}
