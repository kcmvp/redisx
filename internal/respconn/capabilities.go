package respconn

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type CommandEntry struct {
	Name    string
	Aliases []string
	Usage   string
	Role    string
	MinArgs int
	MaxArgs int
}

type CommandGroups struct {
	GroupOrder    []string
	Groups        map[string][]CommandEntry
	MetaAdminOnly bool
}

type Capabilities struct {
	IsRedisx     bool
	ServerVer    string
	AdminRole    bool
	DualPort     bool
	LiveRebuild  bool
	WriteHooks   bool
	SearchIndex  bool
	PubSub       bool
	TypedDocs    bool
	TypedIndexes bool
	StatsNs      int
	StatsIdx     int
	StorageMode  string
	Commands     *CommandGroups
}

func ProbeCapabilities(ctx context.Context, client *redis.Client, probeTimeout time.Duration) Capabilities {
	defer func() { _ = recover() }()
	if client == nil {
		return Capabilities{}
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	raw := client.Do(probeCtx, "HELLO", 3)
	if raw.Err() != nil {
		return Capabilities{}
	}
	v, err := raw.Result()
	if err != nil {
		return Capabilities{}
	}
	return ParseHelloCapabilities(v)
}

func ProbeCapabilitiesWithRetry(ctx context.Context, client *redis.Client, totalTimeout time.Duration) Capabilities {
	defer func() { _ = recover() }()
	if client == nil {
		return Capabilities{}
	}
	if totalTimeout <= 0 {
		totalTimeout = 3 * time.Second
	}
	single := totalTimeout / 3
	if single < 500*time.Millisecond {
		single = 500 * time.Millisecond
	}
	for try := 0; try < 3; try++ {
		if try > 0 {
			wait, waitCancel := context.WithTimeout(ctx, single/2)
			<-wait.Done()
			waitCancel()
		}
		caps := ProbeCapabilities(ctx, client, single)
		if caps.IsRedisx {
			return caps
		}
	}
	return Capabilities{}
}

func asString(v any) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	}
	return "", false
}

func asBool(v any) bool {
	switch b := v.(type) {
	case bool:
		return b
	case int:
		return b != 0
	case int32:
		return b != 0
	case int64:
		return b != 0
	case uint:
		return b != 0
	case uint32:
		return b != 0
	case uint64:
		return b != 0
	case float64:
		return b != 0
	case string:
		switch b {
		case "1", "true", "TRUE", "True", "yes", "on":
			return true
		}
	case []byte:
		switch string(b) {
		case "1", "true", "TRUE", "True", "yes", "on":
			return true
		}
	}
	return false
}

func asMapAny(v any) (map[any]any, bool) {
	if v == nil {
		return nil, false
	}
	switch m := v.(type) {
	case map[any]any:
		return m, true
	case map[string]any:
		out := make(map[any]any, len(m))
		for k, val := range m {
			out[k] = val
		}
		return out, true
	}
	return nil, false
}

func asFlatArrayMap(v any) (map[any]any, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 || len(arr)%2 != 0 {
		return nil, false
	}
	out := make(map[any]any, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		k := arr[i]
		keyStr, okK := asString(k)
		if !okK {
			continue
		}
		out[keyStr] = arr[i+1]
	}
	return out, true
}

func asNestedMap(v any) (map[any]any, bool) {
	mm, ok := asMapAny(v)
	if ok {
		return mm, true
	}
	return asFlatArrayMap(v)
}

func ParseHelloCapabilities(v any) Capabilities {
	caps := Capabilities{}
	mm, ok := asMapAny(v)
	if !ok {
		mm, ok = asFlatArrayMap(v)
	}
	if !ok {
		return caps
	}
	if srv, okS := asString(mm["server"]); !okS || srv != "redisx" {
		return caps
	}
	caps.IsRedisx = true
	caps.ServerVer, _ = asString(mm["version"])
	caps.AdminRole = asBool(mm["admin_role"])
	caps.DualPort = asBool(mm["dual_port"])
	if fm, okF := asNestedMap(mm["features"]); okF {
		caps.TypedDocs = asBool(fm["typed_docs"])
		caps.TypedIndexes = asBool(fm["typed_indexes"])
		caps.LiveRebuild = asBool(fm["live_rebuild"])
		caps.WriteHooks = asBool(fm["write_hooks"])
		caps.SearchIndex = asBool(fm["search_index"])
		caps.PubSub = asBool(fm["pubsub"])
	}
	if sm, okS := asNestedMap(mm["stats"]); okS {
		caps.StatsNs = asInt(sm["namespaces"])
		caps.StatsIdx = asInt(sm["indexes"])
	}
	if sm, okS := asNestedMap(mm["storage"]); okS {
		caps.StorageMode, _ = asString(sm["mode"])
	}
	if cm, okC := asNestedMap(mm["commands"]); okC {
		grp := &CommandGroups{Groups: map[string][]CommandEntry{}}
		grp.MetaAdminOnly = asBool(cm["meta_admin_only"])
		if order, okOrd := cm["group_order"].([]any); okOrd {
			for _, o := range order {
				if s, okS := asString(o); okS {
					grp.GroupOrder = append(grp.GroupOrder, s)
				}
			}
		}
		if groups, okG := asNestedMap(cm["groups"]); okG {
			for gNameIf := range groups {
				gName, okN := asString(gNameIf)
				if !okN {
					continue
				}
				itemsIf, okArr := groups[gNameIf].([]any)
				if !okArr {
					continue
				}
				entries := make([]CommandEntry, 0, len(itemsIf))
				for _, it := range itemsIf {
					im, okM := asNestedMap(it)
					if !okM {
						continue
					}
					e := CommandEntry{}
					var okN bool
					e.Name, okN = asString(im["name"])
					if !okN || e.Name == "" {
						continue
					}
					e.Usage, _ = asString(im["usage"])
					e.Role, _ = asString(im["role"])
					e.MinArgs = asInt(im["min_args"])
					maxRaw, hasMax := im["max_args"]
					if hasMax {
						e.MaxArgs = asInt(maxRaw)
					} else {
						e.MaxArgs = -1
					}
					if aliases, okA := im["aliases"].([]any); okA {
						for _, a := range aliases {
							if s, okS := asString(a); okS {
								e.Aliases = append(e.Aliases, s)
							}
						}
					}
					entries = append(entries, e)
				}
				grp.Groups[gName] = entries
			}
		}
		caps.Commands = grp
	}
	return caps
}

func asInt(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}
