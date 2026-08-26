package wizard

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/kcmvp/redisx/cmd/_shared/session"
	"github.com/kcmvp/redisx/cmd/_shared/tui"
	"github.com/kcmvp/redisx/cmd/_shared/tui/widgets"
)

type FieldsChoice int

const (
	FieldsSkip FieldsChoice = iota
	FieldsInteractive
	FieldsPaste
)

func cBold(s string) string    { return tui.Bold(s) }
func cCyan(s string) string    { return tui.Cyan(s) }
func cYellow(s string) string  { return tui.Yellow(s) }
func cGreen(s string) string   { return tui.Green(s) }
func cRed(s string) string     { return tui.Red(s) }
func cMagenta(s string) string { return tui.Magenta(s) }

type DocFormResult struct {
	Namespace string
	Mem       bool
	KeyAttrs  []string
	TTL       string
	Fields    []map[string]any
}

func RunCreateDocWizard(sess *session.Session, cache *session.Cache) error {
	if sess == nil {
		return fmt.Errorf("shell not connected")
	}
	p := tui.NewPrompter()
	res, err := runCreateDocForm(p)
	if err != nil {
		if errors.Is(err, ErrCancel) {
			fmt.Println(cYellow("cancelled."))
			return nil
		}
		return err
	}
	rawFields := ""
	if len(res.Fields) == 0 {
		var arr []map[string]any
		choice, errC := chooseFieldsMode(p, "Fields empty — pick next step")
		if errC != nil {
			if errors.Is(errC, ErrCancel) {
				fmt.Println(cYellow("cancelled."))
				return nil
			}
			return errC
		}
		switch choice {
		case FieldsInteractive:
			_, built, errB := buildFieldsInteractive(p)
			if errB != nil {
				if errors.Is(errB, ErrCancel) {
					fmt.Println(cYellow("cancelled."))
					return nil
				}
				return errB
			}
			arr = built
		case FieldsPaste:
			ok, built, errB := pasteFieldsJSON(p)
			if errB != nil {
				if errors.Is(errB, ErrCancel) {
					fmt.Println(cYellow("cancelled."))
					return nil
				}
				return errB
			}
			if ok {
				arr = built
			}
		}
		res.Fields = arr
	} else {
		raw, _ := json.Marshal(res.Fields)
		rawFields = string(raw)
	}
	_ = rawFields

	fieldNames := map[string]struct{}{}
	for _, f := range res.Fields {
		if n, ok := f["name"].(string); ok {
			fieldNames[n] = struct{}{}
		}
	}
	var keyAttrWarnings []string
	for _, k := range res.KeyAttrs {
		if _, ok := fieldNames[k]; !ok {
			keyAttrWarnings = append(keyAttrWarnings, k)
		}
	}
	spec := map[string]any{
		"namespace": res.Namespace,
		"mem":       res.Mem,
		"keyAttrs":  res.KeyAttrs,
	}
	if len(res.Fields) > 0 {
		spec["fields"] = res.Fields
	}
	if res.TTL != "" && res.TTL != "-1" {
		spec["ttl"] = res.TTL
	}
	specRaw, _ := json.MarshalIndent(spec, "", "  ")

	fmt.Println(cBold(cCyan("── Review ──")))
	reviewRows := [][]string{
		{"Namespace", res.Namespace},
		{"Storage", map[bool]string{true: "mem", false: "disk"}[res.Mem]},
		{"KeyAttrs", strings.Join(res.KeyAttrs, ", ")},
		{"Default TTL", map[bool]string{true: "never", false: res.TTL}[res.TTL == "" || res.TTL == "-1"]},
	}
	if len(res.Fields) == 0 {
		reviewRows = append(reviewRows, []string{"Fields", "(none)"})
	} else {
		reviewRows = append(reviewRows,
			[]string{"Field count", fmt.Sprintf("%d", len(res.Fields))},
			[]string{"Fields (first 3)", FirstNFieldNames(res.Fields, 3)},
		)
	}
	if len(keyAttrWarnings) > 0 {
		reviewRows = append(reviewRows, []string{"⚠ KeyAttrs missing", strings.Join(keyAttrWarnings, ", ")})
	}
	widgets.PrintTable(os.Stdout, []string{"Item", "Value"}, reviewRows)
	fmt.Println()
	fmt.Println(cBold(cCyan("── Payload ──")))
	fmt.Println(string(specRaw))
	fmt.Println()

	ok, err := p.ConfirmKeyword("YES", fmt.Sprintf("register Doc ns=%s KeyAttrs=%s", res.Namespace, strings.Join(res.KeyAttrs, ",")))
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println(cYellow("cancelled."))
		return nil
	}
	v, err := sess.RawDo([]any{"regsch", string(specRaw)}).Result()
	if err != nil {
		return err
	}
	cache.Invalidate()
	fmt.Println(cGreen("✓ submitted regsch — server reply:"))
	PrintAny(os.Stdout, v)
	return nil
}

func runCreateDocForm(p *tui.Prompter) (*DocFormResult, error) {
	ns, err := p.Input(tui.InputField{Title: "Namespace", Required: true, Validate: ValidateNS})
	if err != nil {
		return nil, err
	}
	mem, err := p.Confirm("Memory Only", false)
	if err != nil {
		return nil, err
	}
	kaCSV, err := p.Input(tui.InputField{
		Title:       "KeyAttrs (CSV, dedup applied)",
		Default:     "id",
		Placeholder: "id,user_id",
		Validate:    ValidateCSVListMin(1),
	})
	if err != nil {
		return nil, err
	}
	ttl, err := p.Input(tui.InputField{
		Title:       "Default TTL (1m/1h/1d, -1 = never)",
		Default:     "-1",
		Placeholder: "-1",
	})
	if err != nil {
		return nil, err
	}
	fields, err := p.Text("Fields — JSON array (empty = choose next step)",
		`e.g. [{"name":"id","type":"string"}]`,
		"",
		ValidateJSONArrayMaybeEmpty)
	if err != nil {
		return nil, err
	}
	kaList, _ := DedupCSVList(kaCSV)
	var fieldsArr []map[string]any
	if fields != "" {
		_ = json.Unmarshal([]byte(fields), &fieldsArr)
	}
	return &DocFormResult{
		Namespace: strings.TrimSpace(ns),
		Mem:       mem,
		KeyAttrs:  kaList,
		TTL:       strings.TrimSpace(ttl),
		Fields:    fieldsArr,
	}, nil
}

func chooseFieldsMode(p *tui.Prompter, header string) (FieldsChoice, error) {
	choices := []tui.Choice{
		{Label: "Skip — commit WITHOUT field definitions", Value: "skip"},
		{Label: "Build interactively", Value: "interactive"},
		{Label: "Paste JSON array", Value: "paste"},
	}
	v, err := p.Select(header, choices, "skip")
	if err != nil {
		return FieldsSkip, err
	}
	switch v {
	case "skip":
		return FieldsSkip, nil
	case "interactive":
		return FieldsInteractive, nil
	case "paste":
		return FieldsPaste, nil
	}
	return FieldsSkip, nil
}

func pasteFieldsJSON(p *tui.Prompter) (bool, []map[string]any, error) {
	raw, err := p.Text("Paste JSON array of field definitions",
		`e.g. [{"name":"id","type":"string"}]`,
		"",
		func(s string) error {
			s = strings.TrimSpace(s)
			if s == "" {
				return nil
			}
			var out []map[string]any
			if e := json.Unmarshal([]byte(s), &out); e != nil {
				return fmt.Errorf("parse failed: %w", e)
			}
			return nil
		})
	if err != nil {
		return false, nil, err
	}
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false, nil, nil
	}
	var out []map[string]any
	if errU := json.Unmarshal([]byte(raw), &out); errU != nil {
		fmt.Fprintln(os.Stderr, cRed("JSON parse failed: "+errU.Error()))
		return false, nil, nil
	}
	return true, out, nil
}

func buildFieldsInteractive(p *tui.Prompter) (bool, []map[string]any, error) {
	typeChoices := []tui.Choice{
		{Label: "string", Value: "string"},
		{Label: "int", Value: "int"},
		{Label: "float", Value: "float"},
		{Label: "bool", Value: "bool"},
		{Label: "json", Value: "json"},
	}
	var out []map[string]any
	for {
		name, err := p.Input(tui.InputField{
			Title:       fmt.Sprintf("Field #%d name", len(out)+1),
			Placeholder: "<enter> with empty = finish",
			Validate: func(s string) error {
				s = strings.TrimSpace(s)
				if s == "" {
					return nil
				}
				if strings.ContainsAny(s, " \t:;\"'<>|&*?[]{}()\\/`~!@#$%^+=") {
					return errors.New("invalid characters (only [-_.A-Za-z0-9] allowed)")
				}
				for _, f := range out {
					if n, _ := f["name"].(string); n == s {
						return errors.New("duplicate field name")
					}
				}
				return nil
			},
		})
		if err != nil {
			if errors.Is(err, ErrCancel) {
				return false, nil, err
			}
			return false, nil, err
		}
		name = strings.TrimSpace(name)
		if name == "" {
			if len(out) == 0 {
				return false, nil, nil
			}
			break
		}
		tSel, err := p.Select(fmt.Sprintf("Field #%d (%s) type", len(out)+1, name), typeChoices, "string")
		if err != nil {
			if errors.Is(err, ErrCancel) {
				return false, nil, err
			}
			return false, nil, err
		}
		out = append(out, map[string]any{"name": name, "type": tSel})
		fmt.Printf(cGreen("  + %s (%s)\n"), name, tSel)
	}
	return true, out, nil
}

func FirstNFieldNames(fields []map[string]any, n int) string {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		if name, ok := f["name"].(string); ok {
			names = append(names, name)
		}
	}
	if len(names) > n {
		return strings.Join(names[:n], ", ") + ", …"
	}
	return strings.Join(names, ", ")
}

func PrintAny(out *os.File, v any) {
	switch t := v.(type) {
	case nil:
		_, _ = fmt.Fprintln(out, "(nil)")
	case string:
		_, _ = fmt.Fprintln(out, t)
	case []any:
		for i, e := range t {
			_, _ = fmt.Fprintf(out, "[%d] %v\n", i, e)
		}
	case []string:
		for i, e := range t {
			_, _ = fmt.Fprintf(out, "[%d] %s\n", i, e)
		}
	case error:
		_, _ = fmt.Fprintln(out, cRed(t.Error()))
	default:
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			_, _ = fmt.Fprintf(out, "%v\n", v)
			return
		}
		_, _ = fmt.Fprintln(out, string(b))
	}
}
