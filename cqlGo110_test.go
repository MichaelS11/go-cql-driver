// +build go1.10

package cql

import (
	"testing"
)

func TestDriverOpenConnector(t *testing.T) {
	CqlDriver.Logger = nil
	connector, err := CqlDriver.OpenConnector("")
	if err != nil {
		t.Fatalf("OpenConnector error - received: %v - expected: %v ", err, nil)
	}
	if connector == nil {
		t.Fatalf("connector is nil")
	}

	CqlDriver.Logger = TestLogStderr
	connector, err = CqlDriver.OpenConnector("")
	if err != nil {
		t.Fatalf("OpenConnector error - received: %v - expected: %v ", err, nil)
	}
	if connector == nil {
		t.Fatalf("connector is nil")
	}

	connector, err = CqlDriver.OpenConnector("?blah")
	expectedError := "ConfigStringToClusterConfig error: missing ="
	if err == nil || err.Error() != expectedError {
		t.Fatalf("OpenConnector error - received: %v - expected: %v ", err, expectedError)
	}
	if connector != nil {
		t.Fatalf("OpenConnector connector - received: %v - expected: %v ", connector, nil)
	}
}
