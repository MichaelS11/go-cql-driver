package cql

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"log"
	"testing"
)

func TestConnectionPing(t *testing.T) {
	// test ping good
	conn := testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn := conn.(*cqlConnStruct)

	err := cqlConn.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, nil)
	}

	// test ping RowData error
	cqlConn.logger = log.New(ioutil.Discard, "", 0)
	cqlConn.pingQuery = cqlConn.session.Query("")
	err = cqlConn.Ping(context.Background())
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, driver.ErrBadConn)
	}
	if cqlConn.session != nil {
		t.Fatal("cqlConn.session is not nil")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}

	// test ping len(Values) error
	conn = testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn = conn.(*cqlConnStruct)
	err = cqlConn.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, nil)
	}

	cqlConn.logger = log.New(ioutil.Discard, "", 0)
	cqlConn.pingQuery = cqlConn.session.Query("select cql_version, release_version from system.local")
	err = cqlConn.Ping(context.Background())
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, driver.ErrBadConn)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}

	// test ping Value not *string
	conn = testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn = conn.(*cqlConnStruct)
	err = cqlConn.Ping(context.Background())
	if err != nil {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, nil)
	}

	cqlConn.logger = log.New(ioutil.Discard, "", 0)
	cqlConn.pingQuery = cqlConn.session.Query("select tokens from system.local")
	err = cqlConn.Ping(context.Background())
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, driver.ErrBadConn)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func TestConnectionPingInvalid(t *testing.T) {
	conn := testGetConnectionHostInvalid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn := conn.(*cqlConnStruct)

	err := cqlConn.Ping(context.Background())
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("Ping error - received: %v - expected: %v ", err, driver.ErrBadConn)
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func TestConnectionPrepare(t *testing.T) {
	conn := testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}

	stmt, err := conn.Prepare("")
	if err != nil {
		t.Fatalf("Prepare error - received: %v - expected: %v ", err, nil)
	}
	if stmt == nil {
		t.Fatal("stmt is nil")
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

func TestConnectionPrepareInvalid(t *testing.T) {
	conn := testGetConnectionHostInvalid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}

	stmt, err := conn.Prepare("")
	if err == nil || err != driver.ErrBadConn {
		t.Fatalf("Prepare error - received: %v - expected: %v ", err, driver.ErrBadConn)
	}
	if stmt != nil {
		t.Fatal("stmt is not nil")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func TestConnectionPrepareContext(t *testing.T) {
	conn := testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn := conn.(*cqlConnStruct)

	stmt, err := cqlConn.PrepareContext(context.Background(), "")
	if err != nil {
		t.Fatalf("PrepareContext error - received: %v - expected: %v ", err, nil)
	}
	if stmt == nil {
		t.Fatal("stmt is nil")
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

func TestConnectionBegin(t *testing.T) {
	conn := testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}

	tx, err := conn.Begin()
	if err == nil || err != ErrNotSupported {
		t.Fatalf("Begin error - received: %v - expected: %v ", err, ErrNotSupported)
	}
	if tx != nil {
		t.Fatal("tx is not nil")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func TestConnectionBeginTx(t *testing.T) {
	conn := testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	cqlConn := conn.(*cqlConnStruct)

	tx, err := cqlConn.BeginTx(context.Background(), driver.TxOptions{})
	if err == nil || err != ErrNotSupported {
		t.Fatalf("BeginTx error - received: %v - expected: %v ", err, ErrNotSupported)
	}
	if tx != nil {
		t.Fatal("tx is not nil")
	}

	err = conn.Close()
	if err != nil {
		t.Fatalf("Close error - received: %v - expected: %v ", err, nil)
	}
}

func testGetStatementHostValid(t *testing.T, query string) (driver.Conn, driver.Stmt) {
	conn := testGetConnectionHostValid(t)
	if conn == nil {
		t.Fatal("conn is nil")
	}
	stmt, err := conn.Prepare(query)
	if err != nil {
		t.Fatalf("Prepare error - received: %v - expected: %v ", err, nil)
	}
	if stmt == nil {
		t.Fatal("stmt is nil")
	}
	return conn, stmt
}
