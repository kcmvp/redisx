package testutil

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// LoadFeature dynamically loads a JSON file based on the calling test's context.
// The file name format is: {SuiteName}_{TestMethodName}_{CaseName}.json
// All spaces in the case name are replaced with underscores.
// Example: UpdateSuite_TestUpdateCases_Update_existing_property.json
func LoadFeature(t *testing.T) string {
	t.Helper()

	// t.Name() typically returns something like "TestUpdateSuite/TestUpdateCases/Update_existing_property"
	nameParts := strings.Split(t.Name(), "/")
	if len(nameParts) == 0 {
		t.Fatal("Could not determine test name")
	}

	// We format the filename by joining the parts with underscores and replacing any remaining spaces or problematic chars
	fileName := strings.Join(nameParts, "_")
	fileName = strings.ReplaceAll(fileName, " ", "_")
	fileName = fmt.Sprintf("%s.json", fileName)

	// In test execution, the current working directory is the package directory (e.g., /Users/.../sd/storage)
	// We'll put the features in a "testdata" folder relative to the calling test
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
