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
	"github.com/kcmvp/redisx/internal/naming"
)

var IndexTypeChoices = []string{
	"TEXT    — case-sensitive string match & prefix-range",
	"NUMERIC — float64 ordering & range scans",
	"BOOL    — true/false equality",
	"JSON    — raw JSON value (any, for structured indexing)",
}

var IndexTypeWire = map[int]string{0: "TEXT", 1: "NUMERIC", 2: "BOOL", 3: "JSON"}

type IdxFormResult struct {
	OwnerIdx int
	OwnerNS  string
	Logical  string
	Path     string
	TypeIdx  int
	Unique   bool
}

func RunCreateIdxWizard(sess *session.Session, cache *session.Cache) error {
	if sess == nil {
		return fmt.Errorf("shell not connected")
	}
	p := tui.NewPrompter()
	docs, err := FetchDocList(sess)
	if err != nil {
		return err
	}
	if len(docs) == 0 {
		return fmt.Errorf("no Doc schemas registered yet — run regdoc / !createdoc first before creating any index")
	}
	ownerRows := make([][]string, 0, len(docs))
	for i, d := range docs {
		ns := DocStringField(d, "namespace")
		keyAttrs := DocKeyAttrsSummary(d)
		mem := DocStringField(d, "mem")
		storage := "disk"
		if mem == "true" || strings.EqualFold(mem, "yes") {
			storage = "mem"
		}
		ownerRows = append(ownerRows, []string{fmt.Sprintf("%d", i+1), ns, keyAttrs, storage})
	}
	fmt.Println(cBold(cCyan("── Docs ──")))
	widgets.PrintTable(os.Stdout, []string{"#", "Namespace", "KeyAttrs", "Storage"}, ownerRows)
	fmt.Println()

	res, err := runCreateIdxForm(p, docs)
	if err != nil {
		if errors.Is(err, ErrCancel) {
			fmt.Println(cYellow("cancelled."))
			return nil
		}
		return err
	}
	if res.OwnerIdx < 0 || res.OwnerIdx >= len(docs) {
		return fmt.Errorf("invalid owner doc selection")
	}
	ownerNS := DocStringField(docs[res.OwnerIdx], "namespace")
	if res.TypeIdx < 0 || res.TypeIdx >= len(IndexTypeChoices) {
		return fmt.Errorf("invalid index type selection")
	}
	wireType := IndexTypeWire[res.TypeIdx]

	fmt.Println(cBold(cCyan("── Review ──")))
	idxFullReview := naming.BuildIdxFullName(ownerNS, res.Logical)
	reviewRows := [][]string{
		{"Owner Doc", ownerNS},
		{"Logical name", res.Logical},
		{"Full name (SSoT <owner>_<logical>)", idxFullReview},
		{"Shell display (UX)", ownerNS + "." + res.Logical},
		{"JSON path", res.Path},
		{"Type", wireType},
		{"UNIQUE", map[bool]string{true: "yes", false: "no"}[res.Unique]},
	}
	widgets.PrintTable(os.Stdout, []string{"Item", "Value"}, reviewRows)
	fmt.Println()
	fmt.Println(cBold(cCyan("── RegIdx Payload (RESP wire) ──")))
	fmt.Printf("  regidx %s %s %s", ownerNS, res.Logical, res.Path)
	if res.Unique {
		fmt.Print(" UNIQUE")
	}
	fmt.Printf(" TYPE %s\n\n", wireType)

	confirmAction := fmt.Sprintf("build %s INDEX on %s %s UNIQUE=%t", wireType, ownerNS, res.Path, res.Unique)
	ok, err := p.ConfirmKeyword("CREATE", confirmAction)
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println(cYellow("cancelled."))
		return nil
	}
	callArgs := []any{"regidx", ownerNS, res.Logical, res.Path}
	if res.Unique {
		callArgs = append(callArgs, "UNIQUE")
	}
	callArgs = append(callArgs, "TYPE", wireType)

	v, err := sess.RawDo(callArgs).Result()
	if err != nil {
		return err
	}
	cache.Invalidate()
	fmt.Println(cGreen("✓ submitted regidx to admin-port — server reply:"))
	PrintAny(os.Stdout, v)
	return nil
}

func runCreateIdxForm(p *tui.Prompter, docs []map[string]any) (*IdxFormResult, error) {
	typeChoices := IndexTypeChoices
	ownerChoices := make([]tui.Choice, 0, len(docs))
	for _, d := range docs {
		ns := DocStringField(d, "namespace")
		keyAttrs := DocKeyAttrsSummary(d)
		mem := DocStringField(d, "mem")
		storage := "disk"
		if mem == "true" || strings.EqualFold(mem, "yes") {
			storage = "mem"
		}
		label := fmt.Sprintf("%s (KeyAttrs=%s, storage=%s)", ns, keyAttrs, storage)
		ownerChoices = append(ownerChoices, tui.Choice{Label: label, Value: label})
	}
	typeLabelChoices := make([]tui.Choice, 0, len(typeChoices))
	for i, tc := range typeChoices {
		val := IndexTypeWire[i]
		typeLabelChoices = append(typeLabelChoices, tui.Choice{Label: tc, Value: val})
	}

	ownerLabel, err := p.Select("Owner Doc", ownerChoices, ownerChoices[0].Value)
	if err != nil {
		return nil, err
	}
	ownerIdx := -1
	for i, c := range ownerChoices {
		if c.Value == ownerLabel {
			ownerIdx = i
			break
		}
	}
	if ownerIdx < 0 {
		ownerIdx = 0
	}

	logical, err := p.Input(tui.InputField{Title: "Logical name", Validate: ValidateIdentifier})
	if err != nil {
		return nil, err
	}
	path, err := p.Input(tui.InputField{
		Title:       "JSON Path (e.g. $.tags, $.user.id)",
		Placeholder: "$.",
		Validate:    requireNonEmpty,
	})
	if err != nil {
		return nil, err
	}
	typeVal, err := p.Select("Type", typeLabelChoices, IndexTypeWire[0])
	if err != nil {
		return nil, err
	}
	typeIdx := 0
	for i, w := range IndexTypeWire {
		if w == typeVal {
			typeIdx = i
			break
		}
	}
	unique, err := p.Confirm("Unique", false)
	if err != nil {
		return nil, err
	}
	ownerNS := ""
	if ownerIdx >= 0 && ownerIdx < len(docs) {
		ownerNS = DocStringField(docs[ownerIdx], "namespace")
	}
	return &IdxFormResult{
		OwnerIdx: ownerIdx,
		OwnerNS:  ownerNS,
		Logical:  strings.TrimSpace(logical),
		Path:     strings.TrimSpace(path),
		TypeIdx:  typeIdx,
		Unique:   unique,
	}, nil
}

func FetchDocList(sess *session.Session) ([]map[string]any, error) {
	v, err := sess.RawDo([]any{"lsdoc"}).Result()
	if err != nil {
		return nil, err
	}
	s, ok := v.(string)
	if !ok {
		return nil, fmt.Errorf("lsdoc returned unexpected type %T", v)
	}
	var out []map[string]any
	if err := json.Unmarshal([]byte(s), &out); err != nil {
		return nil, fmt.Errorf("lsdoc JSON parse failed: %w", err)
	}
	return out, nil
}

func DocStringField(d map[string]any, key string) string {
	if d == nil {
		return ""
	}
	switch key {
	case "namespace", "ns":
		for _, k := range []string{"namespace", "ns", "Namespace", "Ns"} {
			if v, ok := d[k].(string); ok && v != "" {
				return v
			}
		}
	case "mem":
		for _, k := range []string{"mem", "memory", "Mem"} {
			switch vv := d[k].(type) {
			case bool:
				if vv {
					return "true"
				}
				return "false"
			case string:
				return vv
			}
		}
	}
	if v, ok := d[key].(string); ok {
		return v
	}
	return ""
}

func DocKeyAttrsSummary(d map[string]any) string {
	if d == nil {
		return "-"
	}
	var items []string
	for _, k := range []string{"keyAttrs", "KeyAttrs", "keyattrs"} {
		switch vv := d[k].(type) {
		case []string:
			items = vv
		case []any:
			for _, e := range vv {
				if s, ok := e.(string); ok {
					items = append(items, s)
				}
			}
		}
		if len(items) > 0 {
			break
		}
	}
	if len(items) == 0 {
		return "-"
	}
	if len(items) > 4 {
		return strings.Join(items[:4], ",") + ",…"
	}
	return strings.Join(items, ",")
}

func RunDropIdxWizard(sess *session.Session, cache *session.Cache) error {
	if sess == nil {
		return fmt.Errorf("shell not connected")
	}
	p := tui.NewPrompter()

	fmt.Println(cBold(cMagenta("─── !dropindex — Phase 1/2: Identify (irreversible after phase 2)")))
	fmt.Println(cYellow("  Indexes are addressed as <owner_ns>.<logical_name>."))
	ownerNS, err := p.Input(tui.InputField{Title: "Owner Doc namespace", Validate: ValidateNS})
	if err != nil {
		return err
	}
	logical, err := p.Input(tui.InputField{Title: "Index logical name", Validate: ValidateIdentifier})
	if err != nil {
		return err
	}
	fullName := ownerNS + "." + logical
	v, err := p.Input(tui.InputField{
		Title:    "Type the exact full-name to confirm: " + cBold(cYellow(fullName)),
		Validate: requireNonEmpty,
	})
	if err != nil {
		return err
	}
	if v != fullName {
		fmt.Println(cRed("full-name mismatch; cancelled."))
		return nil
	}

	fmt.Println(cBold(cMagenta("\n─── !dropindex — Phase 2/2: DESTROY confirm")))
	fmt.Println(cRed("  After this, index data is dropped and cannot be rebuilt automatically."))
	ok, err := p.ConfirmKeyword("DESTROY", fmt.Sprintf("drop index %s", fullName))
	if err != nil {
		return err
	}
	if !ok {
		fmt.Println(cYellow("cancelled."))
		return nil
	}

	res, err := sess.RawDo([]any{"delidx", ownerNS, logical}).Result()
	if err != nil {
		return err
	}
	cache.Invalidate()
	fmt.Println(cGreen("✓ submitted delidx — server reply:"))
	PrintAny(os.Stdout, res)
	return nil
}
