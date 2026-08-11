package tests

import (
	"strings"
	"testing"

	"github.com/nobledeveloper01/ReconSync/internal/domain"
)

func TestContainsCardNumberDetectsRealPANs(t *testing.T) {
	// Published test numbers: valid Luhn, never issued.
	cards := []string{
		"4111111111111111", // Visa
		"5500005555555559", // Mastercard
		"378282246310005",  // Amex, 15
		"6011000990139424", // Discover
		"3530111333300000", // JCB
		"4222222222222",    // Visa, 13 — lower bound
	}
	for _, c := range cards {
		if !domain.ContainsCardNumber(c) {
			t.Errorf("failed to detect card number %s", c)
		}
	}
}

func TestContainsCardNumberHandlesFormatting(t *testing.T) {
	for _, s := range []string{
		"4111 1111 1111 1111",
		"4111-1111-1111-1111",
		"card: 4111 1111 1111 1111 thanks",
		"4111111111111111 ",
		" 4111111111111111",
	} {
		if !domain.ContainsCardNumber(s) {
			t.Errorf("failed to detect formatted card number %q", s)
		}
	}
}

// Documents the known gap: a PAN concatenated into a longer digit run is not
// caught here. Covered by the field denylist and SDK stripping.
func TestContainsCardNumberDoesNotScanInsideLongerRuns(t *testing.T) {
	if domain.ContainsCardNumber("99994111111111111119999") {
		t.Error("matched inside a >19 digit run; whole-run matching was the point")
	}
}

func TestContainsCardNumberIgnoresNonCardNumbers(t *testing.T) {
	for _, s := range []string{
		"",
		"no digits here",
		"12345",               // too short
		"TXN-2026-08-11-8842", // ordinary reference
		"1234567890",          // 10 digits
		"4111111111111112",    // 16 digits, fails Luhn
	} {
		if domain.ContainsCardNumber(s) {
			t.Errorf("false positive on %q", s)
		}
	}
}

func TestContainsCardNumberBoundsScanLength(t *testing.T) {
	// The PAN sits past the scan bound, so it is not reached.
	long := strings.Repeat("a", maxScanLen+100) + "4111111111111111"
	if domain.ContainsCardNumber(long) {
		t.Error("scanned beyond the documented scan bound")
	}
}

func TestScreenMetadataRejectsDenylistedFields(t *testing.T) {
	for _, key := range []string{"cvv", "CVV", "cvv2", "card_number", "cardNumber", "Card-Number", "bvn", "password", "pin"} {
		err := domain.ScreenMetadata(map[string]any{key: "anything"})
		if err == nil {
			t.Errorf("metadata key %q was accepted", key)
			continue
		}
		if _, ok := err.(domain.SensitiveDataError); !ok {
			t.Errorf("key %q returned %T, want SensitiveDataError", key, err)
		}
	}
}

func TestScreenMetadataRejectsCardNumberInValue(t *testing.T) {
	err := domain.ScreenMetadata(map[string]any{"note": "customer said 4111 1111 1111 1111"})
	if err == nil {
		t.Fatal("card number in a metadata value was accepted")
	}
	sde, ok := err.(domain.SensitiveDataError)
	if !ok {
		t.Fatalf("got %T, want SensitiveDataError", err)
	}
	if sde.Path != "metadata.note" {
		t.Errorf("path = %q, want metadata.note", sde.Path)
	}
	if strings.Contains(err.Error(), "4111") {
		t.Error("error message leaked the value it rejected")
	}
}

func TestScreenMetadataWalksNestedStructures(t *testing.T) {
	nested := map[string]any{
		"outer": map[string]any{
			"inner": []any{"harmless", map[string]any{"cvv": "123"}},
		},
	}
	if err := domain.ScreenMetadata(nested); err == nil {
		t.Error("nested denylisted field was accepted")
	}

	err := domain.ScreenMetadata(map[string]any{"items": []any{"ok", "4111111111111111"}})
	if err == nil {
		t.Fatal("card number nested in a slice was accepted")
	}
	if sde, ok := err.(domain.SensitiveDataError); ok && sde.Path != "metadata.items[1]" {
		t.Errorf("path = %q, want metadata.items[1]", sde.Path)
	}
}

func TestScreenMetadataRejectsExcessiveNesting(t *testing.T) {
	deep := map[string]any{"k": "v"}
	for i := 0; i < maxScanDepth+3; i++ {
		deep = map[string]any{"k": deep}
	}
	if err := domain.ScreenMetadata(deep); err == nil {
		t.Error("excessively nested metadata was accepted")
	}
}

func TestScreenMetadataAcceptsLegitimateMetadata(t *testing.T) {
	ok := map[string]any{
		"channel":  "mobile",
		"kyc_tier": 2,
		"retry":    false,
		"branch":   "IKEJA-01",
		"note":     "customer initiated from savings account",
	}
	if err := domain.ScreenMetadata(ok); err != nil {
		t.Errorf("legitimate metadata rejected: %v", err)
	}
	if err := domain.ScreenMetadata(nil); err != nil {
		t.Errorf("nil metadata rejected: %v", err)
	}
}

func TestScreenString(t *testing.T) {
	if err := domain.ScreenString("provider_reference", "ps_ref_88213"); err != nil {
		t.Errorf("legitimate provider reference rejected: %v", err)
	}
	if err := domain.ScreenString("provider_reference", "4111111111111111"); err == nil {
		t.Error("card number in provider_reference was accepted")
	}
}

// Luhn is exercised through the public screen rather than directly.
func TestLuhnBehaviourThroughScreen(t *testing.T) {
	cases := map[string]bool{
		"4111111111111111": true,
		"4111111111111112": false,
		"378282246310005":  true,
		"0000000000000000": true, // degenerate but genuinely Luhn-valid
	}
	for digits, want := range cases {
		if got := domain.ContainsCardNumber(digits); got != want {
			t.Errorf("ContainsCardNumber(%q) = %v, want %v", digits, got, want)
		}
	}
}
