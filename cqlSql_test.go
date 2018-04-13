package cql

import (
	"context"
	"database/sql"
	"testing"
	"time"
)

func TestSqlOpen(t *testing.T) {
	db, err := sql.Open("cql", TestHostValid+"?timeout=10s&connectTimeout=10s")
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	if DisableDestructiveTests {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	result, err := db.ExecContext(ctx, "drop keyspace if exists cqltest")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqlCreate(t *testing.T) {
	if DisableDestructiveTests {
		return
	}

	db, err := sql.Open("cql", TestHostValid+"?timeout=10s&connectTimeout=10s")
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// create keyspace cqltest
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	result, err := db.ExecContext(ctx, "create keyspace cqltest with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// create table cqltest.cqltest
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, "create table cqltest.cqltest (text_data text PRIMARY KEY, int_data int)")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqlInsertUpdateSelectDelete(t *testing.T) {
	if DisableDestructiveTests {
		return
	}

	db, err := sql.Open("cql", TestHostValid+"?timeout=10s&connectTimeout=10s")
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// insert one
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	result, err := db.ExecContext(ctx, "insert into cqltest.cqltest (text_data, int_data) values (?, ?)", "one", 1)
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}
	num, err := result.LastInsertId()
	if err == nil || err != ErrNotSupported {
		t.Fatalf("LastInsertId error - received: %v - expected: %v ", err, ErrNotSupported)
	}
	if num != -1 {
		t.Fatal("id is not -1")
	}
	num, err = result.RowsAffected()
	if err == nil || err != ErrNotSupported {
		t.Fatalf("LastInsertId error - received: %v - expected: %v ", err, ErrNotSupported)
	}
	if num != -1 {
		t.Fatal("rows affected is not -1")
	}

	// select one
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	rows, err := db.QueryContext(ctx, "select text_data, int_data from cqltest.cqltest")
	cancel()
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}

	if !rows.Next() {
		t.Fatal("no Next rows")
	}

	dest := make([]interface{}, 2)
	destPointer := make([]interface{}, 2)
	destPointer[0] = &dest[0]
	destPointer[1] = &dest[1]

	err = rows.Scan(destPointer...)
	if err != nil {
		t.Fatal("Scan error: ", err)
	}
	if dest[0] != "one" {
		t.Fatalf("text_data - received: %v - expected: %v", dest[0], "one")
	}
	if dest[1] != 1 {
		t.Fatalf("int_data - received: %v - expected: %v", dest[1], 1)
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}

	// insert two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, "insert into cqltest.cqltest (text_data, int_data) values (?, ?)", "two", 2)
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	rows, err = db.QueryContext(ctx, "select text_data, int_data from cqltest.cqltest where text_data = ?", "two")
	cancel()
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if !rows.Next() {
		t.Fatal("no Next rows")
	}
	err = rows.Scan(destPointer...)
	if err != nil {
		t.Fatal("Scan error: ", err)
	}
	if dest[0] != "two" {
		t.Fatalf("text_data - received: %v - expected: %v", dest[0], "two")
	}
	if dest[1] != 2 {
		t.Fatalf("int_data - received: %v - expected: %v", dest[1], 2)
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}

	// update two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, "update cqltest.cqltest set int_data = ? where text_data =?", "3", "two")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	rows, err = db.QueryContext(ctx, "select text_data, int_data from cqltest.cqltest where text_data = ?", "two")
	cancel()
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if !rows.Next() {
		t.Fatal("no Next rows")
	}
	err = rows.Scan(destPointer...)
	if err != nil {
		t.Fatal("Scan error: ", err)
	}
	if dest[0] != "two" {
		t.Fatalf("text_data - received: %v - expected: %v", dest[0], "two")
	}
	if dest[1] != 3 {
		t.Fatalf("int_data - received: %v - expected: %v", dest[1], 3)
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}

	// delete two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, "delete from cqltest.cqltest where text_data =?", "two")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	rows, err = db.QueryContext(ctx, "select text_data, int_data from cqltest.cqltest where text_data = ?", "two")
	cancel()
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}

	// delete two
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, "delete from cqltest.cqltest where text_data =?", "two")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqlSelectLoop(t *testing.T) {
	if DisableDestructiveTests {
		return
	}

	db, err := sql.Open("cql", TestHostValid+"?timeout=10s&connectTimeout=10s")
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	for i := 0; i < 100; i++ {
		// select all
		ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
		rows, err := db.QueryContext(ctx, "select text_data, int_data from cqltest.cqltest")
		cancel()
		if err != nil {
			t.Fatal("QueryContext error: ", err)
		}
		if rows == nil {
			t.Fatal("rows is nil")
		}
		if !rows.Next() {
			t.Fatal("no Next rows")
		}
		dest := make([]interface{}, 2)
		destPointer := make([]interface{}, 2)
		destPointer[0] = &dest[0]
		destPointer[1] = &dest[1]
		err = rows.Scan(destPointer...)
		if err != nil {
			t.Fatal("Scan error: ", err)
		}
		if dest[0] != "one" {
			t.Fatalf("text_data - received: %v - expected: %v", dest[0], "one")
		}
		if dest[1] != 1 {
			t.Fatalf("int_data - received: %v - expected: %v", dest[1], 1)
		}
		if rows.Next() {
			t.Fatal("has Next rows")
		}
		err = rows.Err()
		if err != nil {
			t.Fatal("Err error: ", err)
		}
		err = rows.Close()
		if err != nil {
			t.Fatal("Close error: ", err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqTruncate(t *testing.T) {
	if DisableDestructiveTests {
		return
	}

	db, err := sql.Open("cql", TestHostValid+"?timeout=10s&connectTimeout=10s")
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// truncate table cqltest.cqltest
	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	result, err := db.ExecContext(ctx, "truncate table cqltest.cqltest")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select all
	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	rows, err := db.QueryContext(ctx, "select text_data, int_data from cqltest.cqltest")
	cancel()
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqlDrop(t *testing.T) {
	if DisableDestructiveTests {
		return
	}

	db, err := sql.Open("cql", TestHostValid+"?timeout=10s&connectTimeout=10s")
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	result, err := db.ExecContext(ctx, "drop table cqltest.cqltest")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	result, err = db.ExecContext(ctx, "drop keyspace cqltest")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}
