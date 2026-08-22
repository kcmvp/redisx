package server

import (
	"fmt"

	"github.com/tidwall/redcon"
)

// ──────────────────────────────────────────────────────────────────────────
// Skeleton Admin wire-command handlers (D2 Step1).
//
// These are the 6 admin-only commands registered into commandRegistry via
// srv.go variables cmdRegDoc / cmdLsDoc / cmdDesDoc / cmdLsIdx / cmdRegIdx /
// cmdDelIdx. Gate3 (gate.go) already pre-validates argument count / shape
// using proto.Registry SSoT before these handlers are ever invoked — so
// argc is guaranteed to be within [MinArgs..MaxArgs] on entry.
//
// They intentionally perform ZERO reads / writes against the real DB and
// return a consistent, user-friendly "not implemented yet" RESP error.
// Real dispatch into *DB registry APIs happens in D2 Step2 once D5 Spec
// structs + DB Registry maps + 10 non-generic *DB methods are in place.
// ──────────────────────────────────────────────────────────────────────────

const adminSkeletonFmt = "ERR %s is not implemented yet — schema registry (D5) and admin command wiring still pending"

func regDocCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "regdoc"))
}

func lsDocCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "lsdoc"))
}

func desDocCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "desdoc"))
}

func lsIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "lsidx"))
}

func regIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "regidx"))
}

func delIdxCommand(conn redcon.Conn, cmd redcon.Command, db *DB, ps *redcon.PubSub) {
	conn.WriteError(fmt.Sprintf(adminSkeletonFmt, "delidx"))
}
