package xcmd

import (
	"errors"

	"github.com/kcmvp/redisx/x"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// MarshalUpdate converts mutations into the update JSON accepted by UPDATE.
func MarshalUpdate(values ...x.Mutation) ([]byte, error) {
	if len(values) == 0 {
		return nil, errors.New("no update values provided")
	}

	doc := "{}"
	var err error
	for _, value := range values {
		doc, err = sjson.Set(doc, value.Path(), value.Value())
		if err != nil {
			return nil, err
		}
	}
	return []byte(doc), nil
}

// ParseUpdate converts an update JSON object into mutations.
func ParseUpdate(jsonStr string) ([]x.Mutation, error) {
	if !gjson.Valid(jsonStr) {
		return nil, errors.New("invalid update json format")
	}

	root := gjson.Parse(jsonStr)
	if root.Type != gjson.JSON {
		return nil, errors.New("update must be a json object")
	}

	var pairs []x.Mutation
	var walk func(prefix string, node gjson.Result)
	walk = func(prefix string, node gjson.Result) {
		switch node.Type {
		case gjson.String:
			pairs = append(pairs, x.Set(prefix, node.String()))
		case gjson.Number:
			pairs = append(pairs, x.Set(prefix, node.Float()))
		case gjson.True:
			pairs = append(pairs, x.Set(prefix, true))
		case gjson.False:
			pairs = append(pairs, x.Set(prefix, false))
		case gjson.JSON:
			if node.IsObject() {
				node.ForEach(func(key, value gjson.Result) bool {
					next := key.String()
					if prefix != "" {
						next = prefix + "." + next
					}
					walk(next, value)
					return true
				})
			}
		}
	}

	root.ForEach(func(key, value gjson.Result) bool {
		walk(key.String(), value)
		return true
	})

	if len(pairs) == 0 {
		return nil, errors.New("no valid updates provided")
	}
	return pairs, nil
}
