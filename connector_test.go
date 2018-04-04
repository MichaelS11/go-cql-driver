// +build go1.10

package cql

import (
	"context"
	"testing"
)

func TestConnectorDriver(t *testing.T) {
	connector, err := CqlDriver.OpenConnector("")
	if err != nil {
		t.Fatalf("OpenConnector error - received: %v - expected: %v ", err, nil)
	}
	if connector == nil {
		t.Fatalf("connector is nil")
	}

	driver := connector.Driver()
	if driver == nil {
		t.Fatalf("driver is nil")
	}
}

func TestConnectorConnect(t *testing.T) {
	CqlDriver.Logger = nil
	connector, err := CqlDriver.OpenConnector("")
	if err != nil {
		t.Fatalf("OpenConnector error - received: %v - expected: %v ", err, nil)
	}
	if connector == nil {
		t.Fatalf("connector is nil")
	}

	conn, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect error - received: %v - expected: %v ", err, nil)
	}
	if conn == nil {
		t.Fatalf("conn is nil")
	}

	CqlDriver.Logger = TestLogStderr
	connector, err = CqlDriver.OpenConnector("")
	if err != nil {
		t.Fatalf("OpenConnector error - received: %v - expected: %v ", err, nil)
	}
	if connector == nil {
		t.Fatalf("connector is nil")
	}

	conn, err = connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("Connect error - received: %v - expected: %v ", err, nil)
	}
	if conn == nil {
		t.Fatalf("conn is nil")
	}
}
