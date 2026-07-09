package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/kcmvp/indx/storage"
	"github.com/kcmvp/indx/x"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/gjson"
	"github.com/tidwall/redcon"
)

// parseFilter recursively parses a MongoDB-style JSON string into an x.Filter.
func parseFilter(jsonStr string) (x.Filter, error) {
	if jsonStr == "" || jsonStr == "{}" {
		return nil, nil // empty filter passes everything
	}

	if !gjson.Valid(jsonStr) {
		return nil, errors.New("invalid JSON filter format")
	}

	root := gjson.Parse(jsonStr)
	return parseNode(root)
}

func parseNode(node gjson.Result) (x.Filter, error) {
	if node.Type != gjson.JSON {
		return nil, errors.New("expected JSON object")
	}

	var filters []x.Filter
	var parseErr error

	node.ForEach(func(key, value gjson.Result) bool {
		k := key.String()

		switch k {
		case "$and":
			if !value.IsArray() {
				parseErr = errors.New("$and must be an array")
				return false
			}
			var subFilters []x.Filter
			value.ForEach(func(_, subNode gjson.Result) bool {
				f, err := parseNode(subNode)
				if err != nil {
					parseErr = err
					return false
				}
				subFilters = append(subFilters, f)
				return true
			})
			if parseErr != nil {
				return false
			}
			filters = append(filters, x.And(subFilters...))
		case "$or":
			if !value.IsArray() {
				parseErr = errors.New("$or must be an array")
				return false
			}
			var subFilters []x.Filter
			value.ForEach(func(_, subNode gjson.Result) bool {
				f, err := parseNode(subNode)
				if err != nil {
					parseErr = err
					return false
				}
				subFilters = append(subFilters, f)
				return true
			})
			if parseErr != nil {
				return false
			}
			filters = append(filters, x.Or(subFilters...))
		default:
			// Field comparison. E.g. "age": {"$gt": 18} or "status": "active"
			if value.Type == gjson.JSON {
				// Object with operators
				value.ForEach(func(opKey, opVal gjson.Result) bool {
					op := opKey.String()
					switch op {
					case "$eq":
						filters = append(filters, x.Eq(k, opVal.Value()))
					case "$neq":
						filters = append(filters, x.Neq(k, opVal.Value()))
					case "$gt":
						filters = append(filters, x.Gt(k, opVal.Float()))
					case "$gte":
						filters = append(filters, x.Gte(k, opVal.Float()))
					case "$lt":
						filters = append(filters, x.Lt(k, opVal.Float()))
					case "$lte":
						filters = append(filters, x.Lte(k, opVal.Float()))
					case "$contains":
						filters = append(filters, x.Contains(k, opVal.String()))
					case "$in":
						if !opVal.IsArray() {
							parseErr = fmt.Errorf("$in operator requires an array for field %s", k)
							return false
						}
						var inValues []any
						opVal.ForEach(func(_, v gjson.Result) bool {
							inValues = append(inValues, v.Value())
							return true
						})
						filters = append(filters, x.In(k, inValues...))
					default:
						parseErr = fmt.Errorf("unsupported operator: %s", op)
						return false
					}
					return true
				})
			} else {
				// Implicit $eq. E.g. "status": "active"
				filters = append(filters, x.Eq(k, value.Value()))
			}
		}

		return parseErr == nil
	})

	if parseErr != nil {
		return nil, parseErr
	}

	if len(filters) == 0 {
		return nil, nil
	}
	if len(filters) == 1 {
		return filters[0], nil
	}
	// Implicit AND for multiple keys in the same object
	return x.And(filters...), nil
}

func searchIndexCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 4 || len(cmd.Args) > 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	schemaName := string(cmd.Args[1])
	indexAttr := string(cmd.Args[2])
	filterJSON := string(cmd.Args[3])

	var desc bool
	if len(cmd.Args) == 5 {
		order := strings.ToUpper(string(cmd.Args[4]))
		if order != "ASC" && order != "DESC" {
			conn.WriteError("ERR invalid order: " + string(cmd.Args[4]))
			return
		}
		desc = order == "DESC"
	}

	// Parse the MongoDB-style JSON string into an x.Filter
	filter, err := parseFilter(filterJSON)
	if err != nil {
		conn.WriteError("ERR invalid query: " + err.Error())
		return
	}

	res := db.SearchIndex(schemaName, indexAttr, filter, desc)
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR " + res.Error().Error())
		}
	} else {
		results := res.MustGet()
		conn.WriteArray(len(results))
		for _, val := range results {
			conn.WriteBulk([]byte(val))
		}
	}
}

func updateCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	// UPDATE schema filter_json update_json
	if len(cmd.Args) != 4 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}

	schemaName := string(cmd.Args[1])
	filterJSON := string(cmd.Args[2])
	updateJSON := string(cmd.Args[3])

	// Parse filter
	filter, err := parseFilter(filterJSON)
	if err != nil {
		conn.WriteError("ERR invalid query: " + err.Error())
		return
	}

	// Parse update JSON into JsonPairs
	if !gjson.Valid(updateJSON) {
		conn.WriteError("ERR invalid update json format")
		return
	}

	updateNode := gjson.Parse(updateJSON)
	if updateNode.Type != gjson.JSON {
		conn.WriteError("ERR update must be a json object")
		return
	}

	var pairs []storage.JsonPair
	updateNode.ForEach(func(key, value gjson.Result) bool {
		switch value.Type {
		case gjson.String:
			pairs = append(pairs, storage.Pair(key.String(), value.String()))
		case gjson.Number:
			pairs = append(pairs, storage.Pair(key.String(), value.Float()))
		case gjson.True:
			pairs = append(pairs, storage.Pair(key.String(), true))
		case gjson.False:
			pairs = append(pairs, storage.Pair(key.String(), false))
		default:
			// For simplicity we only support basic types for now as DB only supports Type interface
		}
		return true
	})

	if len(pairs) == 0 {
		conn.WriteError("ERR no valid updates provided")
		return
	}

	res := db.Update(schemaName, filter, pairs...)
	if res.IsError() {
		conn.WriteError("ERR " + res.Error().Error())
	} else {
		updatedKeys := res.MustGet()
		conn.WriteArray(len(updatedKeys))
		for _, key := range updatedKeys {
			conn.WriteBulk([]byte(key))
		}
	}
}

func searchKeyCommand(conn redcon.Conn, cmd redcon.Command, db storage.DB, ps *redcon.PubSub) {
	if len(cmd.Args) < 4 || len(cmd.Args) > 5 {
		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
		return
	}
	schemaName := string(cmd.Args[1])
	pattern := string(cmd.Args[2])
	filterJSON := string(cmd.Args[3])

	var desc bool
	if len(cmd.Args) == 5 {
		order := strings.ToUpper(string(cmd.Args[4]))
		if order != "ASC" && order != "DESC" {
			conn.WriteError("ERR invalid order: " + string(cmd.Args[4]))
			return
		}
		desc = order == "DESC"
	}

	filter, err := parseFilter(filterJSON)
	if err != nil {
		conn.WriteError("ERR invalid query: " + err.Error())
		return
	}

	res := db.SearchKey(schemaName, pattern, filter, desc)
	if res.IsError() {
		if errors.Is(res.Error(), buntdb.ErrNotFound) {
			conn.WriteArray(0)
		} else {
			conn.WriteError("ERR " + res.Error().Error())
		}
	} else {
		results := res.MustGet()
		conn.WriteArray(len(results))
		for _, val := range results {
			conn.WriteBulk([]byte(val))
		}
	}
}
