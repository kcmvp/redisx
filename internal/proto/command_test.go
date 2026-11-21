package proto

import (
	"strings"
	"testing"
)

func TestLowerName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"lowercase already", "set", "set"},
		{"uppercase", "SET", "set"},
		{"mixed case", "Set", "set"},
		{"multiword", "SEARCHINDEX", "searchindex"},
		{"empty", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := LowerName(tc.in); got != tc.want {
				t.Errorf("LowerName(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestAllLowerCmdNames(t *testing.T) {
	names := AllLowerCmdNames()

	// Every uppercase command constant must map to its lowercased form.
	consts := map[string]string{
		CmdAuth:         LowerCmdAuth,
		CmdHello:        LowerCmdHello,
		CmdClient:       LowerCmdClient,
		CmdPing:         LowerCmdPing,
		CmdQuit:         LowerCmdQuit,
		CmdSelect:       LowerCmdSelect,
		CmdCommand:      LowerCmdCommand,
		CmdReset:        LowerCmdReset,
		CmdSubscribe:    LowerCmdSubscribe,
		CmdUnsubscribe:  LowerCmdUnsubscribe,
		CmdPSubscribe:   LowerCmdPSubscribe,
		CmdPUnsubscribe: LowerCmdPUnsubscribe,
		CmdPublish:      LowerCmdPublish,
		CmdSet:          LowerCmdSet,
		CmdSetEx:        LowerCmdSetEx,
		CmdSetNX:        LowerCmdSetNX,
		CmdGet:          LowerCmdGet,
		CmdDel:          LowerCmdDel,
		CmdExists:       LowerCmdExists,
		CmdKeys:         LowerCmdKeys,
		CmdUpdate:       LowerCmdUpdate,
		CmdSearchIndex:  LowerCmdSearchIndex,
		CmdSearchKey:    LowerCmdSearchKey,
		CmdSchema:       LowerCmdSchema,
	}

	for upper, lower := range consts {
		got, ok := names[upper]
		if !ok {
			t.Errorf("AllLowerCmdNames() missing key %q", upper)
			continue
		}
		if got != lower {
			t.Errorf("AllLowerCmdNames()[%q] = %q, want %q", upper, got, lower)
		}
		// Every value must be the lowercase form of its key.
		if got != strings.ToLower(upper) {
			t.Errorf("AllLowerCmdNames()[%q] = %q, want lowercase %q", upper, got, strings.ToLower(upper))
		}
	}

	if len(names) != len(consts) {
		t.Errorf("AllLowerCmdNames() returned %d entries, want %d", len(names), len(consts))
	}
}
