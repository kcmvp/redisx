package internal

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// LocalConfigFile is the per-directory CLI endpoint config. Top-level keys
// become `!<name>` connect shortcuts inside the shell.
const LocalConfigFile = "redisx.yaml"

// LocalEndpoint mirrors one top-level entry of ./redisx.yaml (e.g. "app" or
// "ctrl"): host comes from `bind` (empty bind = localhost), plus port and auth.
type LocalEndpoint struct {
	Name string
	Host string
	Port int
	Auth string
}

func (e LocalEndpoint) Addr() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

// LoadLocalConfig reads <dir>/redisx.yaml when present and returns its
// top-level entries as endpoints, in file order. A missing file is not an
// error (returns nil, nil). Entries without a port are skipped.
func LoadLocalConfig(dir string) ([]LocalEndpoint, error) {
	path := filepath.Join(dir, LocalConfigFile)
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	var root yaml.Node
	if err := yaml.Unmarshal(b, &root); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if len(root.Content) == 0 {
		return nil, nil
	}
	top := root.Content[0]
	if top.Kind != yaml.MappingNode {
		return nil, fmt.Errorf("%s: top level must be a mapping of endpoints", path)
	}
	var eps []LocalEndpoint
	for i := 0; i+1 < len(top.Content); i += 2 {
		keyNode, valNode := top.Content[i], top.Content[i+1]
		if valNode.Kind != yaml.MappingNode {
			continue
		}
		var raw struct {
			Port int    `yaml:"port"`
			Bind string `yaml:"bind"`
			Auth string `yaml:"auth"`
		}
		if err := valNode.Decode(&raw); err != nil || raw.Port == 0 {
			continue
		}
		host := raw.Bind
		if host == "" {
			host = "127.0.0.1"
		}
		eps = append(eps, LocalEndpoint{Name: keyNode.Value, Host: host, Port: raw.Port, Auth: raw.Auth})
	}
	return eps, nil
}

// FindEndpoint returns the endpoint whose name matches name
// (case-insensitive), or nil.
func FindEndpoint(eps []LocalEndpoint, name string) *LocalEndpoint {
	if name == "" {
		return nil
	}
	for i := range eps {
		if equalFold(eps[i].Name, name) {
			return &eps[i]
		}
	}
	return nil
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// ShortcutNames renders the `!app` / `!ctrl` style list of available local
// shortcuts, e.g. "!app / !ctrl".
func ShortcutNames(eps []LocalEndpoint) string {
	out := ""
	for i, e := range eps {
		if i > 0 {
			out += " / "
		}
		out += "!" + e.Name
	}
	return out
}
