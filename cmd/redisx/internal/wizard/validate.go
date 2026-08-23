package wizard

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/tui"
)

var ErrCancel = tui.ErrUserAborted

var identifierRE = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func ValidateIdentifier(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("value is required")
	}
	if !identifierRE.MatchString(s) {
		return errors.New("contains invalid characters (only [-_.A-Za-z0-9] allowed)")
	}
	if strings.HasPrefix(s, "!") {
		return errors.New("cannot start with '!' (reserved for sugar commands)")
	}
	return nil
}

func ValidateNS(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return errors.New("namespace is required")
	}
	if strings.ContainsAny(s, " \t;\"'<>|&*?[]{}()\\/`~!@#$%^+=") {
		return errors.New("namespace contains invalid characters")
	}
	if strings.HasPrefix(s, "_") {
		return errors.New("namespace cannot start with '_' (reserved for system NsPrefix)")
	}
	if strings.Count(s, ":") > 2 {
		return errors.New("too many ':' separators (>2)")
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
