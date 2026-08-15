package tests

import (
	"strings"
	"testing"
	"time"

	"github.com/nobledeveloper01/ReconSync/internal/account"
	"github.com/nobledeveloper01/ReconSync/internal/auth"
)

// Roles reuse the scopes machine credentials already use, so there is one
// answer to "may this caller do X" whether the caller is a person or a service.
func TestRolesMapOntoExistingScopes(t *testing.T) {
	if !account.RoleViewer.Can(auth.ScopeReportsRead) {
		t.Error("a viewer cannot read reports")
	}
	// The whole point of the split: reading a report and changing where
	// reversals are delivered are not the same privilege.
	if account.RoleViewer.Can(auth.ScopeEndpointsWrite) {
		t.Error("a viewer can change delivery targets")
	}
	if account.RoleOperator.Can(auth.ScopeEndpointsWrite) {
		t.Error("an operator can change delivery targets")
	}
	if !account.RoleAdmin.Can(auth.ScopeEndpointsWrite) {
		t.Error("an admin cannot change delivery targets")
	}
	// Every role can read: an account that can see nothing has no reason to
	// exist.
	for _, r := range account.Roles() {
		if !r.Can(auth.ScopeReportsRead) {
			t.Errorf("%s cannot read reports", r)
		}
		if !r.Valid() {
			t.Errorf("%s is not valid", r)
		}
	}
	if account.Role("superuser").Valid() {
		t.Error("an unknown role validated")
	}
}

func TestPasswordHashingRoundTrips(t *testing.T) {
	const pw = "correct horse battery staple"

	hash, err := account.HashPassword(pw)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if strings.Contains(hash, pw) {
		t.Fatal("the password appears in its own hash")
	}
	if !account.VerifyPassword(hash, pw) {
		t.Error("the right password did not verify")
	}
	if account.VerifyPassword(hash, pw+" ") {
		t.Error("a wrong password verified")
	}

	// Salted: the same password twice must not produce the same hash, or a
	// stolen table reveals which users share a password.
	other, err := account.HashPassword(pw)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if other == hash {
		t.Error("two hashes of the same password are identical — the salt is not working")
	}

	// Garbage must not verify rather than panic.
	for _, bad := range []string{"", "not-a-hash", "argon2id$x$y$z$a$b"} {
		if account.VerifyPassword(bad, pw) {
			t.Errorf("%q verified as a hash", bad)
		}
	}
}

// Length, not composition rules: requiring a symbol produces "Password1!",
// which is memorable to nobody and guessable by everything.
func TestPasswordPolicyIsLengthBased(t *testing.T) {
	if err := account.ValidatePassword("Sh0rt!"); err == nil {
		t.Error("a six character password with a symbol was accepted")
	}
	if err := account.ValidatePassword("a whole sentence of lowercase"); err != nil {
		t.Errorf("a long passphrase was rejected: %v", err)
	}
	// Unbounded input would let anyone spend the server's CPU on hashing.
	if err := account.ValidatePassword(strings.Repeat("x", 2000)); err == nil {
		t.Error("an unbounded password was accepted")
	}
}

// Checked against a published RFC 6238 vector rather than against itself: an
// implementation that only agrees with its own output would still not work with
// anyone's authenticator app.
func TestTOTPMatchesTheRFCVector(t *testing.T) {
	// RFC 6238 appendix B, SHA-1, secret "12345678901234567890".
	secret := "GEZDGNBVGY3TQOJQGEZDGNBVGY3TQOJQ"

	for _, tc := range []struct {
		unix int64
		want string
	}{
		{59, "287082"},
		{1111111109, "081804"},
		{1234567890, "005924"},
		{2000000000, "279037"},
	} {
		got, err := account.TOTPCode(secret, time.Unix(tc.unix, 0).UTC())
		if err != nil {
			t.Fatalf("TOTPCode: %v", err)
		}
		if got != tc.want {
			t.Errorf("at %d = %s, want %s", tc.unix, got, tc.want)
		}
	}
}

func TestTOTPVerificationWindow(t *testing.T) {
	secret, err := account.NewTOTPSecret()
	if err != nil {
		t.Fatalf("NewTOTPSecret: %v", err)
	}
	now := time.Now().UTC()

	code, err := account.TOTPCode(secret, now)
	if err != nil {
		t.Fatalf("TOTPCode: %v", err)
	}
	if !account.VerifyTOTP(secret, code, now) {
		t.Error("the current code did not verify")
	}
	// One step of tolerance for a phone whose clock has drifted.
	if !account.VerifyTOTP(secret, code, now.Add(account.TOTPPeriod)) {
		t.Error("a code one step old did not verify")
	}
	// But not more: every extra step is another minute a shoulder-surfed code
	// stays usable.
	if account.VerifyTOTP(secret, code, now.Add(5*account.TOTPPeriod)) {
		t.Error("a code five steps old still verified")
	}
	for _, bad := range []string{"", "12345", "1234567", "abcdef"} {
		if account.VerifyTOTP(secret, bad, now) {
			t.Errorf("%q verified as a code", bad)
		}
	}

	// The enrolment URI has to carry what an authenticator needs.
	uri := account.TOTPURI("ReconSync", "ada@example.com", secret)
	for _, want := range []string{"otpauth://totp/", "secret=" + secret, "issuer=ReconSync", "digits=6"} {
		if !strings.Contains(uri, want) {
			t.Errorf("URI %q is missing %q", uri, want)
		}
	}
}

// Without recovery codes, enabling 2FA is a way to lose your own account.
func TestRecoveryCodesAreDistinctAndReadable(t *testing.T) {
	codes, err := account.NewRecoveryCodes()
	if err != nil {
		t.Fatalf("NewRecoveryCodes: %v", err)
	}
	if len(codes) != account.RecoveryCodeCount {
		t.Fatalf("got %d codes, want %d", len(codes), account.RecoveryCodeCount)
	}

	seen := map[string]bool{}
	for _, c := range codes {
		if seen[c] {
			t.Fatalf("duplicate recovery code %q", c)
		}
		seen[c] = true
		// People write these on paper, so the alphabet excludes the characters
		// that are read back wrongly.
		if strings.ContainsAny(c, "oOiIlL01") {
			t.Errorf("code %q contains an ambiguous character", c)
		}
	}
}

func TestEmailIsNormalisedSoOneAddressIsOneAccount(t *testing.T) {
	if account.NormaliseEmail("  Ada@Example.COM ") != "ada@example.com" {
		t.Error("email was not normalised")
	}
	for _, bad := range []string{"", "no-at-sign", "@nolocal", "trailing@", "has space@x.com"} {
		if err := account.ValidateEmail(bad); err == nil {
			t.Errorf("%q was accepted as an email", bad)
		}
	}
	if err := account.ValidateEmail("ada@example.com"); err != nil {
		t.Errorf("a real address was rejected: %v", err)
	}
}

func TestTokensAreStoredOnlyAsHashes(t *testing.T) {
	token, hash, err := account.NewToken()
	if err != nil {
		t.Fatalf("NewToken: %v", err)
	}
	if token == hash {
		t.Fatal("the token and its stored form are identical")
	}
	if account.HashToken(token) != hash {
		t.Error("hashing the token does not reproduce the stored form")
	}
	// Two tokens must not collide.
	other, _, err := account.NewToken()
	if err != nil {
		t.Fatalf("NewToken: %v", err)
	}
	if other == token {
		t.Error("two tokens were identical")
	}
}
