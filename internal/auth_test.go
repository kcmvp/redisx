package internal

import (
	"testing"
)

func TestAuthKeyStable(t *testing.T) {
	first := AuthKey()
	second := AuthKey()

	if first == "" {
		t.Fatal("expected auth key to be generated")
	}
	if first != second {
		t.Fatalf("expected stable auth key, got %q and %q", first, second)
	}
}
