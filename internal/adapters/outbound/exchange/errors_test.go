package exchange

import (
	"errors"
	"strings"
	"testing"
)

// TestTranslate_ClassifiesWithoutLeakingTheVendorType pins the two halves of the
// boundary: the class is readable with errors.Is, and the vendor error's value is
// only text (its concrete type is not recoverable with errors.As).
func TestTranslate_ClassifiesWithoutLeakingTheVendorType(t *testing.T) {
	t.Parallel()

	vendor := &vendorError{msg: "binance 429 Too Many Requests"}

	err := Translate(vendor)
	if err == nil {
		t.Fatal("Translate returned nil for a vendor error")
	}
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("429 error was not classified as rate limited: %v", err)
	}
	if !strings.Contains(err.Error(), vendor.msg) {
		t.Fatalf("the venue wording was lost: %q", err.Error())
	}

	var extracted *vendorError
	if errors.As(err, &extracted) {
		t.Fatal("the vendor error type escaped through errors.As — it must stay inside the adapter")
	}
}

func TestTranslate_ClassifiesTransientAndPermanent(t *testing.T) {
	t.Parallel()

	if !errors.Is(Translate(errors.New("dial tcp: i/o timeout")), ErrTransient) {
		t.Fatal("a timeout was not classified as transient")
	}
	if !errors.Is(Translate(errors.New("HTTP 503 service unavailable")), ErrTransient) {
		t.Fatal("a 503 was not classified as transient")
	}
	permanent := Translate(errors.New("invalid symbol -1121"))
	if !errors.Is(permanent, ErrPermanent) {
		t.Fatalf("a symbol verdict was not classified as permanent: %v", permanent)
	}
	if got := Translate(nil); got != nil {
		t.Fatalf("Translate(nil) = %v, want nil", got)
	}
}

type vendorError struct{ msg string }

func (e *vendorError) Error() string { return e.msg }
