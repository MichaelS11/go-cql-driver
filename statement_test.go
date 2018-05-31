package cql

import (
	"context"
	"database/sql/driver"
	"testing"
)

func TestStatementNumInput(t *testing.T) {
	conn, stmt := testGetStatementHostValid(t, "")
	if stmt == nil {
		t.Fatal("stmt is nil")
	}

	numInput := stmt.NumInput()
	if numInput != -1 {
		t.Fatalf("NumInput - received: %v - expected: %v ", numInput, -1)
	}

	err := stmt.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func TestStatementExec(t *testing.T) {
	conn, stmt := testGetStatementHostValid(t, "create keyspace if not exists system with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }")
	if stmt == nil {
		t.Fatal("stmt is nil")
	}

	result, err := stmt.Exec([]driver.Value{})
	expectedError := "system keyspace is not user-modifiable"
	if err == nil || err.Error() != expectedError {
		if EnableAuthentication && err != nil && len(err.Error()) > len(Username)+33 {
			receivedError := err.Error()[:len(Username)+33]
			expectedError = "User " + Username + " has no CREATE permission on"
			if receivedError != expectedError {
				t.Fatalf("Exec error - received: %v - expected: %v", receivedError, expectedError)
			}
		} else {
			t.Fatalf("Exec error - received: %v - expected: %v", err, expectedError)
		}
	}
	if result != nil {
		t.Fatal("result is not nil")
	}

	result, err = stmt.Exec([]driver.Value{driver.Value(1)})
	expectedError = "system keyspace is not user-modifiable"
	if err == nil || err.Error() != expectedError {
		if EnableAuthentication && err != nil && len(err.Error()) > len(Username)+33 {
			receivedError := err.Error()[:len(Username)+33]
			expectedError = "User " + Username + " has no CREATE permission on"
			if receivedError != expectedError {
				t.Fatalf("Exec error - received: %v - expected: %v", receivedError, expectedError)
			}
		} else {
			t.Fatalf("Exec error - received: %v - expected: %v", err, expectedError)
		}
	}
	if result != nil {
		t.Fatal("result is not nil")
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

func TestStatementExecContext(t *testing.T) {
	conn, stmt := testGetStatementHostValid(t, "create table if not exists system.local (test text primary key)")
	if stmt == nil {
		t.Fatal("stmt is nil")
	}
	cqlStmt := stmt.(*CqlStmt)

	result, err := cqlStmt.ExecContext(context.Background(), []driver.NamedValue{})
	expectedError := "system keyspace is not user-modifiable."
	if err == nil || err.Error() != expectedError {
		t.Fatalf("ExecContext error - received: %v - expected: %v ", err, expectedError)
	}
	if result != nil {
		t.Fatal("result is not nil")
	}

	result, err = cqlStmt.ExecContext(context.Background(), []driver.NamedValue{{Ordinal: 1, Value: 1}})
	if err == nil || err.Error() != expectedError {
		t.Fatalf("ExecContext error - received: %v - expected: %v ", err, expectedError)
	}
	if result != nil {
		t.Fatal("result is not nil")
	}

	result, err = cqlStmt.ExecContext(context.Background(), []driver.NamedValue{{Name: "a"}})
	if err == nil || err != ErrNamedValuesNotSupported {
		t.Fatalf("ExecContext error - received: %v - expected: %v ", err, ErrNamedValuesNotSupported)
	}
	if result != nil {
		t.Fatal("result is not nil")
	}

	result, err = cqlStmt.ExecContext(context.Background(), []driver.NamedValue{{Ordinal: 2}})
	if err == nil || err != ErrOrdinalOutOfRange {
		t.Fatalf("ExecContext error - received: %v - expected: %v ", err, ErrOrdinalOutOfRange)
	}
	if result != nil {
		t.Fatal("result is not nil")
	}

	result, err = cqlStmt.ExecContext(context.Background(), []driver.NamedValue{{Ordinal: 0}})
	if err == nil || err != ErrOrdinalOutOfRange {
		t.Fatalf("ExecContext error - received: %v - expected: %v ", err, ErrOrdinalOutOfRange)
	}
	if result != nil {
		t.Fatal("result is not nil")
	}

	err = cqlStmt.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}

	result, err = cqlStmt.ExecContext(context.Background(), []driver.NamedValue{})
	if err == nil || err != ErrQueryIsNil {
		t.Fatalf("ExecContext error - received: %v - expected: %v ", err, ErrQueryIsNil)
	}
	if result != nil {
		t.Fatal("result is not nil")
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

func TestStatementQuery(t *testing.T) {
	conn, stmt := testGetStatementHostValid(t, "select release_version from system.local}")
	if stmt == nil {
		t.Fatal("stmt is nil")
	}

	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		t.Fatalf("Query error - received: %v - expected: %v ", err, nil)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}

	rows, err = stmt.Query([]driver.Value{driver.Value(1)})
	if err != nil {
		t.Fatalf("Query error - received: %v - expected: %v ", err, nil)
	}
	if rows == nil {
		t.Fatal("rows is nil")
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

func TestStatementQueryContext(t *testing.T) {
	conn, stmt := testGetStatementHostValid(t, "select release_version from system.local}")
	if stmt == nil {
		t.Fatal("stmt is nil")
	}
	cqlStmt := stmt.(*CqlStmt)

	rows, err := cqlStmt.QueryContext(context.Background(), []driver.NamedValue{})
	if err != nil {
		t.Fatalf("QueryContext error - received: %v - expected: %v ", err, nil)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}

	rows, err = cqlStmt.QueryContext(context.Background(), []driver.NamedValue{{Ordinal: 1, Value: 1}})
	if err != nil {
		t.Fatalf("QueryContext error - received: %v - expected: %v ", err, nil)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}

	rows, err = cqlStmt.QueryContext(context.Background(), []driver.NamedValue{{Name: "a"}})
	if err == nil || err != ErrNamedValuesNotSupported {
		t.Fatalf("QueryContext error - received: %v - expected: %v ", err, ErrNamedValuesNotSupported)
	}
	if rows != nil {
		t.Fatal("rows is not nil")
	}

	rows, err = cqlStmt.QueryContext(context.Background(), []driver.NamedValue{{Ordinal: 2}})
	if err == nil || err != ErrOrdinalOutOfRange {
		t.Fatalf("QueryContext error - received: %v - expected: %v ", err, ErrOrdinalOutOfRange)
	}
	if rows != nil {
		t.Fatal("rows is not nil")
	}

	rows, err = cqlStmt.QueryContext(context.Background(), []driver.NamedValue{{Ordinal: 0}})
	if err == nil || err != ErrOrdinalOutOfRange {
		t.Fatalf("QueryContext error - received: %v - expected: %v ", err, ErrOrdinalOutOfRange)
	}
	if rows != nil {
		t.Fatal("rows is not nil")
	}

	err = cqlStmt.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}

	rows, err = cqlStmt.QueryContext(context.Background(), []driver.NamedValue{})
	if err == nil || err != ErrQueryIsNil {
		t.Fatalf("QueryContext error - received: %v - expected: %v ", err, ErrQueryIsNil)
	}
	if rows != nil {
		t.Fatal("rows is not nil")
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

func testGetRowsHostValid(t *testing.T, query string) (driver.Conn, driver.Stmt, driver.Rows) {
	conn, stmt := testGetStatementHostValid(t, query)
	if stmt == nil {
		t.Fatal("stmt is nil")
	}

	rows, err := stmt.Query([]driver.Value{})
	if err != nil {
		t.Fatalf("Query error - received: %v - expected: %v ", err, nil)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	return conn, stmt, rows
}
