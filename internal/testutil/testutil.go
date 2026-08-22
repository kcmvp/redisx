package testutil

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// DBPath returns one temporary database file path for tests.
func DBPath(tb testing.TB) string {
	tb.Helper()
	return filepath.Join(tb.TempDir(), "redisx.db")
}

// AllocateFreePort picks a free TCP port on 127.0.0.1 by binding to :0 then
// immediately releasing the socket. Callers should be aware of the inherent
// TIME_WAIT race on Darwin: re-binding the returned port in the same process
// within a few milliseconds can occasionally fail with "address already in
// use" when the kernel hands it to another concurrent goroutine before the
// original listener's close fully propagates. In practice the 10 ms sleep
// injected by server.StartWithConfig plus per-port randomization inside Go's
// `net` package is enough to keep the window tiny.
func AllocateFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = l.Close() }()
	tcpAddr, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("AllocateFreePort: unexpected addr type %T", l.Addr())
	}
	return tcpAddr.Port, nil
}

// AllocateTwoFreePorts returns two distinct free TCP ports on 127.0.0.1. The
// two ports are NEVER equal even if a transient listener release leaves an
// overlap window for the second allocate.
func AllocateTwoFreePorts(tb testing.TB) (appPort, adminPort int) {
	tb.Helper()
	var err error
	appPort, err = AllocateFreePort()
	if err != nil {
		tb.Fatalf("allocate free app port: %v", err)
	}
	for tries := 0; tries < 10; tries++ {
		adminPort, err = AllocateFreePort()
		if err != nil {
			tb.Fatalf("allocate free admin port: %v", err)
		}
		if adminPort != appPort {
			return
		}
	}
	tb.Fatalf("AllocateTwoFreePorts: admin port collided with appPort=%d after 10 retries", appPort)
	return 0, 0
}

// LoadFeature dynamically loads a JSON file based on the calling test's context.
// The file name format is: {SuiteName}_{TestMethodName}_{CaseName}.json
// All spaces in the case name are replaced with underscores.
// Example: UpdateSuite_TestUpdateCases_Update_existing_property.json
func LoadFeature(t *testing.T) string {
	t.Helper()

	nameParts := strings.Split(t.Name(), "/")
	if len(nameParts) == 0 {
		t.Fatal("Could not determine test name")
	}

	fileName := strings.Join(nameParts, "_")
	fileName = strings.ReplaceAll(fileName, " ", "_")
	fileName = fmt.Sprintf("%s.json", fileName)

	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		t.Fatal("Could not determine caller information")
	}

	dir := filepath.Dir(filename)
	filePath := filepath.Join(dir, "testdata", fileName)

	data, err := os.ReadFile(filePath)
	if err != nil {
		t.Fatalf("Failed to read feature file %s: %v", filePath, err)
	}

	return string(data)
}
