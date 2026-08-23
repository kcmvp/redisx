package tui

import (
	"errors"
	"fmt"
	"strings"

	"github.com/charmbracelet/huh"
)

type Prompter struct{}

func NewPrompter() *Prompter { return &Prompter{} }

var ErrUserAborted = huh.ErrUserAborted

type InputField struct {
	Name        string
	Title       string
	Placeholder string
	Default     string
	Required    bool
	Validate    func(string) error
}

func (p *Prompter) Input(f InputField) (string, error) {
	v := f.Default
	var valFn func(string) error
	if f.Validate != nil {
		valFn = f.Validate
	} else if f.Required {
		valFn = func(s string) error {
			if strings.TrimSpace(s) == "" {
				return errors.New("value is required")
			}
			return nil
		}
	}
	h := huh.NewInput().Title(f.Title).Value(&v)
	if f.Placeholder != "" {
		h.Placeholder(f.Placeholder)
	}
	if valFn != nil {
		h.Validate(valFn)
	}
	err := h.Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return "", ErrUserAborted
		}
		return "", err
	}
	return strings.TrimSpace(v), nil
}

func (p *Prompter) Confirm(title string, defYes bool) (bool, error) {
	v := defYes
	aff, neg := "Yes", "No"
	if !defYes {
		aff, neg = "No", "Yes"
	}
	err := huh.NewConfirm().Title(title).Affirmative(aff).Negative(neg).Value(&v).Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return false, ErrUserAborted
		}
		return false, err
	}
	return v, nil
}

type Choice struct {
	Label string
	Value string
}

func (p *Prompter) Select(title string, choices []Choice, def string) (string, error) {
	opts := make([]huh.Option[string], 0, len(choices))
	for _, c := range choices {
		o := huh.NewOption(c.Label, c.Value)
		if def == c.Value {
			o.Selected(true)
		}
		opts = append(opts, o)
	}
	var v string
	err := huh.NewSelect[string]().Title(title).Options(opts...).Value(&v).Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return "", ErrUserAborted
		}
		return "", err
	}
	return v, nil
}

func (p *Prompter) SelectNth(title string, choices []Choice, defIdx int) (int, error) {
	opts := make([]huh.Option[int], 0, len(choices))
	for i, c := range choices {
		o := huh.NewOption(c.Label, i)
		if i == defIdx {
			o.Selected(true)
		}
		opts = append(opts, o)
	}
	var v int
	err := huh.NewSelect[int]().Title(title).Options(opts...).Value(&v).Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return -1, ErrUserAborted
		}
		return -1, err
	}
	return v, nil
}

func (p *Prompter) Text(title string, desc string, def string, validate func(string) error) (string, error) {
	v := def
	h := huh.NewText().Title(title).Value(&v)
	if desc != "" {
		h.Description(desc)
	}
	if validate != nil {
		h.Validate(validate)
	}
	err := h.Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return "", ErrUserAborted
		}
		return "", err
	}
	return strings.TrimSpace(v), nil
}

func (p *Prompter) ConfirmKeyword(keyword string, action string) (bool, error) {
	var got string
	err := huh.NewInput().
		Title(fmt.Sprintf("Confirm %s", action)).
		Description(fmt.Sprintf("Type %q exactly to confirm.", keyword)).
		Value(&got).
		Run()
	if err != nil {
		if errors.Is(err, huh.ErrUserAborted) {
			return false, ErrUserAborted
		}
		return false, err
	}
	return strings.TrimSpace(got) == keyword, nil
}
