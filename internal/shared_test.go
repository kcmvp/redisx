package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type sharedUserDoc string

func (sharedUserDoc) Namespace() string  { return "user" }
func (sharedUserDoc) Mem() bool          { return false }
func (sharedUserDoc) KeyAttrs() []string { return []string{"id"} }
func (d sharedUserDoc) RawJSON() string  { return string(d) }
func (sharedUserDoc) TTL() time.Duration { return time.Hour }

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

func TestValidateKeyPattern(t *testing.T) {
	t.Run("accepts document scoped pattern", func(t *testing.T) {
		full, err := ValidateKeyPattern[sharedUserDoc]("*")
		require.NoError(t, err)
		require.Equal(t, "user:*", full)
	})

	t.Run("rejects full namespace", func(t *testing.T) {
		_, err := ValidateKeyPattern[sharedUserDoc]("user")
		require.EqualError(t, err, "key pattern must be document-scoped, got storage pattern: user")
	})

	t.Run("rejects prefixed storage pattern", func(t *testing.T) {
		_, err := ValidateKeyPattern[sharedUserDoc]("user:*")
		require.EqualError(t, err, "key pattern must be document-scoped, got storage pattern: user:*")
	})
}

func TestValidateIdxName(t *testing.T) {
	t.Run("accepts logical name", func(t *testing.T) {
		full, err := ValidateIdxName[sharedUserDoc]("age")
		require.NoError(t, err)
		require.Equal(t, "user_age", full)
	})

	t.Run("rejects empty name", func(t *testing.T) {
		_, err := ValidateIdxName[sharedUserDoc]("")
		require.EqualError(t, err, "index name is required")
	})

	t.Run("rejects fully qualified name", func(t *testing.T) {
		_, err := ValidateIdxName[sharedUserDoc]("user_age")
		require.EqualError(t, err, "idx name must be logical, got fully-qualified index name: user_age")
	})
}
