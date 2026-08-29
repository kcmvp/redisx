package server

import (
	"path/filepath"
	"strings"
	"testing"

	"github.com/kcmvp/redisx/internal/naming"
	"github.com/tidwall/buntdb"
	"github.com/tidwall/redcon"
)

func mustTempDBFile(t *testing.T, nameHint string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, nameHint+".db")
	return p
}

func mustOpenDB(t *testing.T, dbPath string) *DB {
	t.Helper()
	db := openDB(dbPath)
	if db == nil {
		t.Fatalf("openDB returned nil for %s", dbPath)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func writeKey(t *testing.T, db *DB, slot string, val string) {
	t.Helper()
	if db == nil || db.disk == nil {
		t.Fatalf("cannot write partial key to nil db")
	}
	err := db.disk.Update(func(tx *buntdb.Tx) error {
		_, _, serr := tx.Set(naming.AuthStorageKey(slot), val, nil)
		return serr
	})
	if err != nil {
		t.Fatalf("writeKey slot=%s: %v", slot, err)
	}
}

func readKeyOrEmpty(t *testing.T, db *DB, slot string) string {
	t.Helper()
	var v string
	_ = db.disk.View(func(tx *buntdb.Tx) error {
		gv, gerr := tx.Get(naming.AuthStorageKey(slot))
		if gerr != nil && gerr != buntdb.ErrNotFound {
			return gerr
		}
		v = gv
		return nil
	})
	return v
}

// ---------------- case1: first-boot (empty DB) ----------------

func TestBootstrapAuth_Case1_4_EmptySeeds(t *testing.T) {
	// Both N → both gen, anyGenerated=true, persisted distinct 32-hex.
	db := mustOpenDB(t, mustTempDBFile(t, "c1-4"))
	app, ctrl, gen, err := db.bootstrapAuth("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !gen {
		t.Fatalf("want anyGenerated=true")
	}
	if app == "" || ctrl == "" {
		t.Fatalf("empty result: app=%q ctrl=%q", app, ctrl)
	}
	if len(app) != 32 || len(ctrl) != 32 {
		t.Fatalf("want 32-char (128-bit hex) keys, got app=%d ctrl=%d", len(app), len(ctrl))
	}
	if readKeyOrEmpty(t, db, "app_0") != app || readKeyOrEmpty(t, db, "ctrl_0") != ctrl {
		t.Fatalf("persisted mismatch")
	}
	// Second call → no gen, identical values.
	app2, ctrl2, gen2, err2 := db.bootstrapAuth("", "")
	if err2 != nil {
		t.Fatalf("err2: %v", err2)
	}
	if gen2 {
		t.Fatalf("second call should not generate")
	}
	if app2 != app || ctrl2 != ctrl {
		t.Fatalf("values drifted: want %q/%q got %q/%q", app, ctrl, app2, ctrl2)
	}
}

func TestBootstrapAuth_Case1_1_AppSeedOnly(t *testing.T) {
	// App=Y ctrl=N → app equals seed, ctrl is new-generated; anyGenerated=true
	// because ctrl slot was missing (DB empty) and ctrl seed is empty.
	db := mustOpenDB(t, mustTempDBFile(t, "c1-1"))
	seed := "fixed-app-seed-11"
	app, ctrl, gen, err := db.bootstrapAuth(seed, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if app != seed {
		t.Fatalf("want app=%q, got %q", seed, app)
	}
	if ctrl == "" {
		t.Fatalf("ctrl not generated")
	}
	if !gen {
		t.Fatalf("want anyGenerated=true because ctrl was empty and seed ctrl is empty")
	}
	if readKeyOrEmpty(t, db, "app_0") != seed || readKeyOrEmpty(t, db, "ctrl_0") != ctrl {
		t.Fatalf("persisted mismatch")
	}
}

func TestBootstrapAuth_Case1_2_CtrlSeedOnly(t *testing.T) {
	// App=N ctrl=Y → ctrl equals seed, app auto-gen; anyGenerated=true.
	db := mustOpenDB(t, mustTempDBFile(t, "c1-2"))
	seed := "fixed-ctrl-seed-12"
	app, ctrl, gen, err := db.bootstrapAuth("", seed)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ctrl != seed {
		t.Fatalf("want ctrl=%q, got %q", seed, ctrl)
	}
	if app == "" {
		t.Fatalf("app not generated")
	}
	if !gen {
		t.Fatalf("want anyGenerated=true because app empty + no seed app")
	}
	if readKeyOrEmpty(t, db, "app_0") != app || readKeyOrEmpty(t, db, "ctrl_0") != seed {
		t.Fatalf("persisted mismatch")
	}
}

func TestBootstrapAuth_Case1_3_BothSeeds_Distinct(t *testing.T) {
	// Both Y → both directly take seed values; anyGenerated=false (DB empty but
	// neither slot went through the "gen missing" branch because both seeds non-empty).
	db := mustOpenDB(t, mustTempDBFile(t, "c1-3-distinct"))
	a, c := "s13-a", "s13-c"
	app, ctrl, gen, err := db.bootstrapAuth(a, c)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if app != a || ctrl != c {
		t.Fatalf("want %q/%q got %q/%q", a, c, app, ctrl)
	}
	if gen {
		t.Fatalf("want anyGenerated=false because both seeds provided directly")
	}
	if readKeyOrEmpty(t, db, "app_0") != a || readKeyOrEmpty(t, db, "ctrl_0") != c {
		t.Fatalf("persisted mismatch")
	}
}

func TestBootstrapAuth_Case1_3_BothSeeds_Equal(t *testing.T) {
	// Both Y and equal → allowed (per simplified rules 2). anyGenerated=false.
	db := mustOpenDB(t, mustTempDBFile(t, "c1-3-equal"))
	equal := "equal-both-seeds"
	app, ctrl, gen, err := db.bootstrapAuth(equal, equal)
	if err != nil {
		t.Fatalf("expected no err for equal seeds (rule 2), got %v", err)
	}
	if app != equal || ctrl != equal {
		t.Fatalf("want equal %q/%q, got %q/%q", equal, equal, app, ctrl)
	}
	if gen {
		t.Fatalf("want anyGenerated=false")
	}
	if readKeyOrEmpty(t, db, "app_0") != equal || readKeyOrEmpty(t, db, "ctrl_0") != equal {
		t.Fatalf("persisted mismatch")
	}
}

// ---------------- case2: second-boot (DB already has pair) ----------------

func TestBootstrapAuth_Case2_4_PersistedNoSeeds(t *testing.T) {
	// Both N → resolve to stored values, no write, anyGenerated=false.
	db := mustOpenDB(t, mustTempDBFile(t, "c2-4"))
	a0, c0, _, err1 := db.bootstrapAuth("", "")
	if err1 != nil {
		t.Fatalf("first err: %v", err1)
	}
	app, ctrl, gen, err := db.bootstrapAuth("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if gen {
		t.Fatalf("want anyGenerated=false on persisted DB")
	}
	if app != a0 || ctrl != c0 {
		t.Fatalf("values drifted")
	}
}

func TestBootstrapAuth_Case2_1_AppSeedOverwrite(t *testing.T) {
	// App=Y ctrl=N → overwrite app slot, ctrl stays persisted; no generation (ctrl already had value).
	db := mustOpenDB(t, mustTempDBFile(t, "c2-1"))
	_, c0, _, err1 := db.bootstrapAuth("", "")
	if err1 != nil {
		t.Fatalf("first err: %v", err1)
	}
	newA := "new-app-seed-c21"
	app, ctrl, gen, err := db.bootstrapAuth(newA, "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if app != newA || ctrl != c0 {
		t.Fatalf("want %q/%q, got %q/%q", newA, c0, app, ctrl)
	}
	if gen {
		t.Fatalf("want anyGenerated=false — ctrl slot already had persisted value and ctrl seed empty, no missing => no generation")
	}
	if readKeyOrEmpty(t, db, "app_0") != newA || readKeyOrEmpty(t, db, "ctrl_0") != c0 {
		t.Fatalf("persisted mismatch")
	}
}

func TestBootstrapAuth_Case2_2_CtrlSeedOverwrite(t *testing.T) {
	// App=N ctrl=Y → overwrite ctrl, app unchanged; anyGenerated=false.
	db := mustOpenDB(t, mustTempDBFile(t, "c2-2"))
	a0, _, _, err1 := db.bootstrapAuth("", "")
	if err1 != nil {
		t.Fatalf("first err: %v", err1)
	}
	newC := "new-ctrl-seed-c22"
	app, ctrl, gen, err := db.bootstrapAuth("", newC)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if app != a0 || ctrl != newC {
		t.Fatalf("want %q/%q got %q/%q", a0, newC, app, ctrl)
	}
	if gen {
		t.Fatalf("want anyGenerated=false")
	}
	if readKeyOrEmpty(t, db, "app_0") != a0 || readKeyOrEmpty(t, db, "ctrl_0") != newC {
		t.Fatalf("persisted mismatch")
	}
}

func TestBootstrapAuth_Case2_3_BothSeeds(t *testing.T) {
	// Both Y → overwrite both to seed values; no generation.
	db := mustOpenDB(t, mustTempDBFile(t, "c2-3"))
	_, _, _, err1 := db.bootstrapAuth("", "")
	if err1 != nil {
		t.Fatalf("first err: %v", err1)
	}
	a, c := "c23-seed-a", "c23-seed-c"
	app, ctrl, gen, err := db.bootstrapAuth(a, c)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if app != a || ctrl != c {
		t.Fatalf("want %q/%q got %q/%q", a, c, app, ctrl)
	}
	if gen {
		t.Fatalf("want anyGenerated=false when both seeds provided")
	}
	if readKeyOrEmpty(t, db, "app_0") != a || readKeyOrEmpty(t, db, "ctrl_0") != c {
		t.Fatalf("persisted mismatch")
	}
	// Same seeds applied again → no write, still anyGenerated=false.
	app2, ctrl2, gen2, err2 := db.bootstrapAuth(a, c)
	if err2 != nil {
		t.Fatalf("err2: %v", err2)
	}
	if gen2 || app2 != a || ctrl2 != c {
		t.Fatalf("re-apply equal seeds should be no-op idempotent")
	}
}

// ---------------- partial repair + edge ----------------

func TestBootstrapAuth_PartialRepair_AppMissing(t *testing.T) {
	// DB has only ctrl persisted, no seeds → app is generated, ctrl reused;
	// anyGenerated=true → banner prints.
	db := mustOpenDB(t, mustTempDBFile(t, "partial-app-missing"))
	writeKey(t, db, "ctrl_0", "ctrl-stored-x")
	app, ctrl, gen, err := db.bootstrapAuth("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if ctrl != "ctrl-stored-x" {
		t.Fatalf("ctrl should be preserved %q, got %q", "ctrl-stored-x", ctrl)
	}
	if app == "" {
		t.Fatalf("app should be regenerated for missing half")
	}
	if !gen {
		t.Fatalf("want anyGenerated=true for generated missing app half")
	}
}

func TestBootstrapAuth_PartialRepair_CtrlMissing(t *testing.T) {
	// DB has only app persisted, no seeds → ctrl generated, app preserved.
	db := mustOpenDB(t, mustTempDBFile(t, "partial-ctrl-missing"))
	writeKey(t, db, "app_0", "app-stored-y")
	app, ctrl, gen, err := db.bootstrapAuth("", "")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if app != "app-stored-y" {
		t.Fatalf("app preserved %q, got %q", "app-stored-y", app)
	}
	if ctrl == "" {
		t.Fatalf("ctrl not generated for missing half")
	}
	if !gen {
		t.Fatalf("want anyGenerated=true")
	}
}

func TestBootstrapAuth_IdempotentSeedsEqualPersisted(t *testing.T) {
	// Seed values equal what's already persisted → no writes, anyGenerated=false.
	db := mustOpenDB(t, mustTempDBFile(t, "idempotent"))
	a, c := "static-a", "static-c"
	_, _, _, err1 := db.bootstrapAuth(a, c)
	if err1 != nil {
		t.Fatalf("err1: %v", err1)
	}
	app, ctrl, gen, err := db.bootstrapAuth(a, c)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if gen {
		t.Fatalf("anyGenerated should be false when seeds match persisted")
	}
	if app != a || ctrl != c {
		t.Fatalf("values should match seeds")
	}
}

// ─── gate2_AuthKeyMatch ───

// gate2MockConn is a minimal mock for gate2 testing.
type gate2MockConn struct {
	redcon.Conn
	ctx      any
	lastErr  string
}

func (m *gate2MockConn) Context() any            { return m.ctx }
func (m *gate2MockConn) WriteError(msg string)   { m.lastErr = msg }
func (m *gate2MockConn) RemoteAddr() string      { return "127.0.0.1:0" }

func TestGate2_AuthKeyMatch(t *testing.T) {
	db := mustOpenDB(t, mustTempDBFile(t, "gate2"))
	// Bootstrap with known keys
	_, _, _, err := db.bootstrapAuth("app-key-123", "ctrl-key-456")
	if err != nil {
		t.Fatalf("bootstrapAuth: %v", err)
	}

	tests := []struct {
		name       string
		cmdName    string
		ctx        any
		role       portRole
		wantReject bool
		errPhrase  string
	}{
		{
			name:       "AUTH command always passes",
			cmdName:    "AUTH",
			ctx:        "",
			role:       portRoleApp,
			wantReject: false,
		},
		{
			name:       "HELLO command always passes",
			cmdName:    "HELLO",
			ctx:        "",
			role:       portRoleApp,
			wantReject: false,
		},
		{
			name:       "CLIENT command always passes",
			cmdName:    "CLIENT",
			ctx:        "",
			role:       portRoleCtrl,
			wantReject: false,
		},
		{
			name:       "no auth context passes",
			cmdName:    "SET",
			ctx:        "",
			role:       portRoleApp,
			wantReject: false,
		},
		{
			name:       "internal key passes on any port",
			cmdName:    "SET",
			ctx:        "internal-test-key",
			role:       portRoleApp,
			wantReject: false,
		},
		{
			name:       "correct app key on app port passes",
			cmdName:    "SET",
			ctx:        "app-key-123",
			role:       portRoleApp,
			wantReject: false,
		},
		{
			name:       "correct ctrl key on ctrl port passes",
			cmdName:    "REGSCH",
			ctx:        "ctrl-key-456",
			role:       portRoleCtrl,
			wantReject: false,
		},
		{
			name:       "ctrl key on app port rejected",
			cmdName:    "SET",
			ctx:        "ctrl-key-456",
			role:       portRoleApp,
			wantReject: true,
			errPhrase:  "WRONGPASS",
		},
		{
			name:       "app key on ctrl port rejected",
			cmdName:    "REGSCH",
			ctx:        "app-key-123",
			role:       portRoleCtrl,
			wantReject: true,
			errPhrase:  "WRONGPASS",
		},
		{
			name:       "external key (not in app/ctrl values) passes",
			cmdName:    "SET",
			ctx:        "external-limit-key",
			role:       portRoleApp,
			wantReject: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockConn := &gate2MockConn{ctx: tt.ctx}
			// Set the port role
			connRoleMu.Lock()
			connRoleMap[mockConn] = tt.role
			connRoleMu.Unlock()
			defer func() {
				connRoleMu.Lock()
				delete(connRoleMap, mockConn)
				connRoleMu.Unlock()
			}()

			// Set internal key for this test
			globalMu.Lock()
			origKey := internalAuthKey
			internalAuthKey = "internal-test-key"
			globalMu.Unlock()
			defer func() {
				globalMu.Lock()
				internalAuthKey = origKey
				globalMu.Unlock()
			}()

			rejected := gate2_AuthKeyMatch(mockConn, tt.cmdName, db)
			if tt.wantReject {
				if !rejected {
					t.Fatal("expected rejection")
				}
				if tt.errPhrase != "" && !strings.Contains(mockConn.lastErr, tt.errPhrase) {
					t.Fatalf("error %q does not contain %q", mockConn.lastErr, tt.errPhrase)
				}
			} else {
				if rejected {
					t.Fatalf("unexpected rejection: %s", mockConn.lastErr)
				}
			}
		})
	}
}

// TestGate2_NoAuthConfigured tests that when no auth is configured, all commands pass.
func TestGate2_NoAuthConfigured(t *testing.T) {
	db := mustOpenDB(t, mustTempDBFile(t, "gate2-noauth"))
	// Don't bootstrap auth — DB has no _auth_ keys

	mockConn := &gate2MockConn{ctx: "any-key"}
	connRoleMu.Lock()
	connRoleMap[mockConn] = portRoleApp
	connRoleMu.Unlock()
	defer func() {
		connRoleMu.Lock()
		delete(connRoleMap, mockConn)
		connRoleMu.Unlock()
	}()

	rejected := gate2_AuthKeyMatch(mockConn, "SET", db)
	if rejected {
		t.Fatal("expected no rejection when auth not configured")
	}
}
