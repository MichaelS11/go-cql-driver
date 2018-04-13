package cql

import (
	"database/sql/driver"
	"io"
	"testing"
)

func TestRowsColumns(t *testing.T) {
	conn, stmt, rows := testGetRowsHostValid(t, "select cql_version from system.local")
	if rows == nil {
		t.Fatal("rows is nil")
	}

	columns := rows.Columns()
	if len(columns) != 1 {
		t.Fatalf("Columns len - received: %v - expected: %v ", len(columns), 1)
	}
	if columns[0] != "cql_version" {
		t.Fatalf("Columns[0] - received: %v - expected: %v ", columns[0], "cql_version")
	}

	err := rows.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func TestRowsNext(t *testing.T) {
	conn, stmt, rows := testGetRowsHostValid(t, "select cql_version from system.local")
	if rows == nil {
		t.Fatal("rows is nil")
	}

	dest := make([]driver.Value, 1)
	err := rows.Next(dest)
	if err != nil {
		t.Fatalf("Next error - received: %v - expected: %v ", err, nil)
	}
	if len(dest) != 1 {
		t.Fatalf("Next len - received: %v - expected: %v ", len(dest), 1)
	}
	data, ok := dest[0].(string)
	if !ok {
		t.Fatal("Next type not string")
	}
	if len(data) < 3 {
		t.Fatalf("Next string len too small - received: %v ", len(data))
	}

	dest = make([]driver.Value, 0)
	err = rows.Next(dest)
	if err == nil || err != io.EOF {
		t.Fatalf("Next error - received: %v - expected: %v ", err, io.EOF)
	}
	if len(dest) != 0 {
		t.Fatalf("Next len - received: %v - expected: %v ", len(dest), 1)
	}

	err = rows.Next(dest)
	if err == nil || err != io.EOF {
		t.Fatalf("Next error - received: %v - expected: %v ", err, io.EOF)
	}
	if len(dest) != 0 {
		t.Fatalf("Next len - received: %v - expected: %v ", len(dest), 1)
	}

	err = rows.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
	err = stmt.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}
