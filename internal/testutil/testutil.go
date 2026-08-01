package testutil

import (
	"fmt"
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
