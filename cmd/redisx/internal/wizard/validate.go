package wizard

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/internal/naming"
)

var ErrCancel = tui.ErrUserAborted

func ValidateIdentifier(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("value is required")
	}
	if strings.HasPrefix(s, "!") {
		return errors.New("cannot start with '!' (reserved for sugar commands)")
	}
	if err := naming.ValidateLogicalIndexName(s); err != nil {
		return fmt.Errorf("invalid identifier: %w", err)
	}
	return nil
}

func ValidateNS(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("namespace is required")
	}
	if err := naming.ValidateDocLogicalNamespace(s); err != nil {
		return fmt.Errorf("invalid doc namespace: %w", err)
	}
	return nil
}

func DedupCSVList(v string) ([]string, error) {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, pp := range parts {
		ppp := strings.TrimSpace(pp)
		if ppp == "" {
			continue
		}
		if _, dup := seen[ppp]; dup {
			continue
		}
		seen[ppp] = struct{}{}
		out = append(out, ppp)
	}
	return out, nil
}

func ValidateCSVListMin(minItems int) func(string) error {
	return func(v string) error {
		out, err := DedupCSVList(v)
		if err != nil {
			return err
		}
		if len(out) < minItems {
			return fmt.Errorf("expected at least %d distinct non-empty items (got %d after dedupe)", minItems, len(out))
		}
		return nil
	}
}

func ValidateJSONArrayMaybeEmpty(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var out []any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return fmt.Errorf("JSON array parse failed: %w", err)
	}
	return nil
}

func requireNonEmpty(s string) error {
	if strings.TrimSpace(s) == "" {
		return errors.New("value is required")
	}
	return nil
}
