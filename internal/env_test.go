package internal

import "testing"

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

func TestMemKey(t *testing.T) {
	if got := MemKey("user:1"); got != "_m_user:1" {
		t.Fatalf("expected prefixed memory key, got %q", got)
	}
	if got := MemKey("_m_user:1"); got != "_m_user:1" {
		t.Fatalf("expected idempotent memory key, got %q", got)
	}
}
