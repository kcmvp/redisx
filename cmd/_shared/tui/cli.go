package tui

import (
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

type RunContext struct {
	RawArgs  []string
	IO       IO
	UserData any
}

type IO struct {
	In    io.Reader
	Out   io.Writer
	Err   io.Writer
	FSOut io.Writer
}

func DefaultIO() IO {
	return IO{In: os.Stdin, Out: os.Stdout, Err: os.Stderr, FSOut: os.Stderr}
}

type CLICommand struct {
	Use     string
	Aliases []string
	Short   string
	Long    string
	Run     func(ctx RunContext, args []string) error
	MinArgs int
	MaxArgs int
	Hidden  bool
}

type FlagSet struct {
	fs *flag.FlagSet
}

func (f *FlagSet) StringVar(p *string, long, short, def, usage string) {
	f.fs.StringVar(p, long, def, usage)
	if short != "" && short != long {
		f.fs.StringVar(p, short, def, usage)
	}
}

func (f *FlagSet) IntVar(p *int, long, short string, def int, usage string) {
	f.fs.IntVar(p, long, def, usage)
	if short != "" && short != long {
		f.fs.IntVar(p, short, def, usage)
	}
}

func (f *FlagSet) BoolVar(p *bool, long, short string, def bool, usage string) {
	f.fs.BoolVar(p, long, def, usage)
	if short != "" && short != long {
		f.fs.BoolVar(p, short, def, usage)
	}
}

type AppHooks struct {
	BeforeRun     func(ctx RunContext, name string, args []string) error
	OnUnknown     func(ctx RunContext, args []string) (handled bool, err error)
	NeedsContext  func(name string, args []string) bool
	Banner        func(ctx RunContext) string
	GlobalHelp    func(ctx RunContext, groups map[string][]GroupEntry, flagHelpText string) string
	HelpFor       func(ctx RunContext, c *CLICommand, canonical string) string
	Prompt        func(ctx RunContext) string
	REPLQuit      func(word string) bool
	DispatchError func(ctx RunContext, err error)
	EnterREPL     func(ctx RunContext) error
}

type GroupEntry struct {
	Use   string
	Short string
}

type CLIApp struct {
	name         string
	version      string
	flags        FlagSet
	commands     map[string]*CLICommand
	cmdOrder     []string
	cmdAliases   map[string]string
	hooks        AppHooks
	ctx          RunContext
	helpFlag     bool
	versionFlag  bool
	flagDefaults []func()
}

func NewCLIApp(name, version string, hooks AppHooks) *CLIApp {
	io := DefaultIO()
	app := &CLIApp{
		name:       name,
		version:    version,
		commands:   map[string]*CLICommand{},
		cmdAliases: map[string]string{},
		hooks:      hooks,
		ctx:        RunContext{IO: io},
	}
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.FSOut)
	app.flags.fs = fs
	return app
}

func (a *CLIApp) Flags() *FlagSet { return &a.flags }

func (a *CLIApp) InstallHelpVersionFlags() {
	a.flags.BoolVar(&a.helpFlag, "help", "h", false, "print help")
	a.flags.BoolVar(&a.versionFlag, "version", "v", false, "print version")
	a.flags.fs.Usage = func() {
		text := a.renderGlobalHelp()
		fmt.Fprint(a.ctx.IO.Err, text)
	}
}

func (a *CLIApp) SetUserData(ud any) { a.ctx.UserData = ud }
func (a *CLIApp) UserData() any      { return a.ctx.UserData }
func (a *CLIApp) Hooks() AppHooks    { return a.hooks }

func (a *CLIApp) Register(c *CLICommand) {
	name := strings.SplitN(c.Use, " ", 2)[0]
	if _, dup := a.commands[name]; dup {
		panic("duplicate CLI command registered: " + name)
	}
	a.commands[name] = c
	a.cmdOrder = append(a.cmdOrder, name)
	for _, al := range c.Aliases {
		if al == name {
			continue
		}
		if _, dup := a.cmdAliases[al]; dup {
			panic("duplicate CLI alias registered: " + al)
		}
		a.cmdAliases[al] = name
	}
}

func (a *CLIApp) resolve(name string) (*CLICommand, string) {
	if name == "" {
		return nil, ""
	}
	if c, ok := a.commands[name]; ok {
		return c, name
	}
	if n, ok := a.cmdAliases[name]; ok {
		return a.commands[n], n
	}
	for cn, c := range a.commands {
		if strings.EqualFold(cn, name) {
			return c, cn
		}
	}
	for al, cn := range a.cmdAliases {
		if strings.EqualFold(al, name) {
			return a.commands[cn], cn
		}
	}
	return nil, ""
}

func (a *CLIApp) Resolve(name string) (*CLICommand, string) { return a.resolve(name) }

func (a *CLIApp) HelpForCmd(c *CLICommand, canonical string) string {
	if a.hooks.HelpFor != nil {
		if s := a.hooks.HelpFor(a.ctx, c, canonical); s != "" {
			return s
		}
	}
	return a.renderHelpForCmd(c, canonical)
}

func (a *CLIApp) listKnownNames() []string {
	out := make([]string, 0, len(a.commands)+len(a.cmdAliases))
	for cn := range a.commands {
		out = append(out, cn)
	}
	for al := range a.cmdAliases {
		out = append(out, al)
	}
	return out
}

func (a *CLIApp) IsKnownCommand(word string) bool {
	if word == "" {
		return false
	}
	for _, n := range a.listKnownNames() {
		if strings.EqualFold(n, word) {
			return true
		}
	}
	return false
}

func (a *CLIApp) Execute(cliArgs []string) error {
	a.ctx.RawArgs = append([]string(nil), cliArgs...)
	err := a.flags.fs.Parse(cliArgs)
	if err != nil {
		if err == flag.ErrHelp {
			return nil
		}
		return err
	}
	if a.versionFlag {
		fmt.Fprintf(a.ctx.IO.Out, "%s %s\n", a.name, a.version)
		return nil
	}
	if a.helpFlag {
		fmt.Fprint(a.ctx.IO.Out, a.renderGlobalHelp())
		return nil
	}
	rest := a.flags.fs.Args()
	if len(rest) == 0 {
		if a.hooks.BeforeRun != nil {
			if err := a.hooks.BeforeRun(a.ctx, "", nil); err != nil {
				return err
			}
		}
		if a.hooks.EnterREPL != nil {
			return a.hooks.EnterREPL(a.ctx)
		}
		return nil
	}
	name := rest[0]
	args := rest[1:]
	if name == "help" || name == "?" {
		want := ""
		if len(args) > 0 {
			want = args[0]
		}
		fmt.Fprint(a.ctx.IO.Out, a.renderHelpFor(want))
		return nil
	}
	return a.dispatch(name, args, false)
}

func (a *CLIApp) dispatch(name string, args []string, repl bool) error {
	c, canonical := a.resolve(name)
	if c == nil {
		if a.hooks.OnUnknown != nil {
			handled, err := a.hooks.OnUnknown(a.ctx, append([]string{name}, args...))
			if handled || err != nil {
				return err
			}
		}
		return fmt.Errorf("unknown command %q — run help for usage", name)
	}
	if c.Use != "" {
		first := strings.SplitN(c.Use, " ", 2)[0]
		if first != "raw" {
			for _, r := range args {
				if strings.HasPrefix(r, "-") && len(r) > 1 {
					return fmt.Errorf("unexpected flag %q after subcommand %q (global flags must come before subcommand name)", r, canonical)
				}
			}
		}
	}
	if c.MinArgs >= 0 && len(args) < c.MinArgs {
		return fmt.Errorf("%s requires at least %d arg(s) (got %d)", canonical, c.MinArgs, len(args))
	}
	if c.MaxArgs >= 0 && len(args) > c.MaxArgs {
		return fmt.Errorf("%s accepts at most %d arg(s) (got %d)", canonical, c.MaxArgs, len(args))
	}
	needCtx := true
	if a.hooks.NeedsContext != nil {
		needCtx = a.hooks.NeedsContext(canonical, args)
	} else if repl {
		needCtx = true
	}
	if needCtx && a.hooks.BeforeRun != nil {
		if err := a.hooks.BeforeRun(a.ctx, canonical, args); err != nil {
			return err
		}
	}
	return c.Run(a.ctx, args)
}

func (a *CLIApp) DispatchREPLLine(lineArgs []string) (quit bool, err error) {
	if len(lineArgs) == 0 {
		return false, nil
	}
	name := lineArgs[0]
	if a.hooks.REPLQuit != nil && a.hooks.REPLQuit(name) {
		return true, nil
	}
	if name == "quit" || name == "exit" || name == "logout" {
		return true, nil
	}
	rest := lineArgs[1:]
	if name == "help" || name == "?" {
		want := ""
		if len(rest) > 0 {
			want = rest[0]
		}
		fmt.Fprint(a.ctx.IO.Out, a.renderHelpFor(want))
		return false, nil
	}
	err = a.dispatch(name, rest, true)
	if err != nil && a.hooks.DispatchError != nil {
		a.hooks.DispatchError(a.ctx, err)
		return false, nil
	}
	return false, err
}

func (a *CLIApp) FlagHelpText() string {
	var sb strings.Builder
	a.flags.fs.SetOutput(&sb)
	a.flags.fs.PrintDefaults()
	return sb.String()
}

func (a *CLIApp) renderGlobalHelp() string {
	var sb strings.Builder
	groups := map[string][]GroupEntry{}
	order := []string{}
	seen := map[string]struct{}{}
	for _, name := range a.cmdOrder {
		c := a.commands[name]
		if c.Hidden {
			continue
		}
		group := ""
		if a.hooks.GlobalHelp != nil {
		}
		grp := "Commands"
		group = grp
		if _, ok := seen[group]; !ok {
			seen[group] = struct{}{}
			order = append(order, group)
		}
		groups[group] = append(groups[group], GroupEntry{Use: c.Use, Short: c.Short})
	}
	if a.hooks.GlobalHelp != nil {
		return a.hooks.GlobalHelp(a.ctx, groups, a.FlagHelpText())
	}
	sb.WriteString(a.name + "\n\n")
	sb.WriteString("USAGE:\n")
	fmt.Fprintf(&sb, "  %s [flags] <subcommand> [args...]\n", a.name)
	fmt.Fprintf(&sb, "  %s [flags]                       (enter REPL)\n\n", a.name)
	sb.WriteString("GLOBAL FLAGS:\n")
	sb.WriteString(a.FlagHelpText())
	sb.WriteString("\n")
	for _, g := range order {
		list := groups[g]
		if len(list) == 0 {
			continue
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Use < list[j].Use })
		fmt.Fprintf(&sb, "%s:\n", Bold(g))
		width := 0
		for _, e := range list {
			if len(e.Use) > width {
				width = len(e.Use)
			}
		}
		for _, e := range list {
			pad := strings.Repeat(" ", width-len(e.Use)+2)
			fmt.Fprintf(&sb, "  %s%s%s\n", e.Use, pad, e.Short)
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func (a *CLIApp) renderHelpFor(want string) string {
	if want == "" {
		return a.renderGlobalHelp()
	}
	c, canonical := a.resolve(want)
	if c == nil {
		return fmt.Sprintf("unknown command %q — run help for list\n", want)
	}
	return a.renderHelpForCmd(c, canonical)
}

func (a *CLIApp) renderHelpForCmd(c *CLICommand, canonical string) string {
	if a.hooks.HelpFor != nil {
		return a.hooks.HelpFor(a.ctx, c, canonical)
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s\n", Bold(canonical))
	if len(c.Aliases) > 0 {
		fmt.Fprintf(&sb, "  aliases: %s\n", strings.Join(c.Aliases, ", "))
	}
	first := ""
	if c.Use != "" {
		first = strings.SplitN(c.Use, " ", 2)[0]
	}
	if first != canonical {
		fmt.Fprintf(&sb, "  usage:   %s\n", c.Use)
	}
	if c.Short != "" {
		fmt.Fprintf(&sb, "  summary: %s\n", c.Short)
	}
	if c.Long != "" {
		sb.WriteString("\n")
		for _, ln := range strings.Split(strings.TrimRight(c.Long, "\n"), "\n") {
			fmt.Fprintf(&sb, "  %s\n", ln)
		}
	}
	switch {
	case c.MinArgs >= 0 && c.MaxArgs >= 0:
		fmt.Fprintf(&sb, "  argc:    %d..%d\n", c.MinArgs, c.MaxArgs)
	case c.MinArgs >= 0:
		fmt.Fprintf(&sb, "  argc:    >=%d\n", c.MinArgs)
	case c.MaxArgs >= 0:
		fmt.Fprintf(&sb, "  argc:    <=%d\n", c.MaxArgs)
	}
	return sb.String()
}
