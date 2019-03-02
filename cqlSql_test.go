package cql

import (
	"context"
	"database/sql"
	"testing"
)

func TestSqlOpen(t *testing.T) {
	openString := TestHostValid + "?timeout=10s&connectTimeout=10s"
	if EnableAuthentication {
		openString += "&username=" + Username + "&password=" + Password
	}

	db, err := sql.Open("cql", openString)
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	if DisableDestructiveTests {
		err = db.Close()
		if err != nil {
			t.Fatal("Close error: ", err)
		}
		t.SkipNow()
	}

	ctx, cancel := context.WithTimeout(context.Background(), TimeoutValid)
	result, err := db.ExecContext(ctx, "drop keyspace if exists "+KeyspaceName)
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
		t.SkipNow()
	}

	openString := TestHostValid + "?timeout=10s&connectTimeout=10s"
	if EnableAuthentication {
		openString += "&username=" + Username + "&password=" + Password
	}

	db, err := sql.Open("cql", openString)
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// create keyspace
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutValid)
	result, err := db.ExecContext(ctx, "create keyspace "+KeyspaceName+" with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// create table
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	result, err = db.ExecContext(ctx, "create table "+KeyspaceName+"."+TableName+" (text_data text PRIMARY KEY, int_data int)")
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
		t.SkipNow()
	}

	openString := TestHostValid + "?timeout=10s&connectTimeout=10s"
	if EnableAuthentication {
		openString += "&username=" + Username + "&password=" + Password
	}

	db, err := sql.Open("cql", openString)
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// insert one
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutValid)
	result, err := db.ExecContext(ctx, "insert into "+KeyspaceName+"."+TableName+" (text_data, int_data) values (?, ?)", "one", 1)
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
	cancel()

	// select one
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err := db.QueryContext(ctx, "select text_data, int_data from "+KeyspaceName+"."+TableName+"")
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
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	// insert two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	result, err = db.ExecContext(ctx, "insert into "+KeyspaceName+"."+TableName+" (text_data, int_data) values (?, ?)", "two", 2)
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err = db.QueryContext(ctx, "select text_data, int_data from "+KeyspaceName+"."+TableName+" where text_data = ?", "two")
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
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	// update two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	result, err = db.ExecContext(ctx, "update "+KeyspaceName+"."+TableName+" set int_data = ? where text_data = ?", "3", "two")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err = db.QueryContext(ctx, "select text_data, int_data from "+KeyspaceName+"."+TableName+" where text_data = ?", "two")
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
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	// delete two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	result, err = db.ExecContext(ctx, "delete from "+KeyspaceName+"."+TableName+" where text_data = ?", "two")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err = db.QueryContext(ctx, "select text_data, int_data from "+KeyspaceName+"."+TableName+" where text_data = ?", "two")
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	// delete two
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	result, err = db.ExecContext(ctx, "delete from "+KeyspaceName+"."+TableName+" where text_data = ?", "two")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select errors
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err = db.QueryContext(ctx, "select int_data from "+KeyspaceName+"."+TableName+" group by int_data")
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows.Close() == nil {
		t.Fatal("QueryContext no error")
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err = db.QueryContext(ctx, "select int_data from "+KeyspaceName+"."+TableName+" where int_data = ?")
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows.Close() == nil {
		t.Fatal("QueryContext no error")
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqlSelectLoop(t *testing.T) {
	if DisableDestructiveTests {
		t.SkipNow()
	}

	openString := TestHostValid + "?timeout=10s&connectTimeout=10s"
	if EnableAuthentication {
		openString += "&username=" + Username + "&password=" + Password
	}

	db, err := sql.Open("cql", openString)
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	for i := 0; i < 100; i++ {
		// select all
		ctx, cancel := context.WithTimeout(context.Background(), TimeoutValid)
		rows, err := db.QueryContext(ctx, "select text_data, int_data from "+KeyspaceName+"."+TableName+"")
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
		cancel()
		err = rows.Err()
		if err != nil {
			t.Fatal("Err error: ", err)
		}
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqTruncate(t *testing.T) {
	if DisableDestructiveTests {
		t.SkipNow()
	}

	openString := TestHostValid + "?timeout=10s&connectTimeout=10s"
	if EnableAuthentication {
		openString += "&username=" + Username + "&password=" + Password
	}

	db, err := sql.Open("cql", openString)
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	// truncate table
	ctx, cancel := context.WithTimeout(context.Background(), TimeoutValid)
	result, err := db.ExecContext(ctx, "truncate table "+KeyspaceName+"."+TableName+"")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	// select all
	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	rows, err := db.QueryContext(ctx, "select text_data, int_data from "+KeyspaceName+"."+TableName+"")
	if err != nil {
		t.Fatal("QueryContext error: ", err)
	}
	if rows == nil {
		t.Fatal("rows is nil")
	}
	if rows.Next() {
		t.Fatal("has Next rows")
	}
	err = rows.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
	cancel()
	err = rows.Err()
	if err != nil {
		t.Fatal("Err error: ", err)
	}

	err = db.Close()
	if err != nil {
		t.Fatal("Close error: ", err)
	}
}

func TestSqlDrop(t *testing.T) {
	if DisableDestructiveTests {
		t.SkipNow()
	}

	openString := TestHostValid + "?timeout=10s&connectTimeout=10s"
	if EnableAuthentication {
		openString += "&username=" + Username + "&password=" + Password
	}

	db, err := sql.Open("cql", openString)
	if err != nil {
		t.Fatal("Open error: ", err)
	}
	if db == nil {
		t.Fatal("db is nil")
	}

	ctx, cancel := context.WithTimeout(context.Background(), TimeoutValid)
	result, err := db.ExecContext(ctx, "drop table "+KeyspaceName+"."+TableName+"")
	cancel()
	if err != nil {
		t.Fatal("ExecContext error: ", err)
	}
	if result == nil {
		t.Fatal("result is nil")
	}

	ctx, cancel = context.WithTimeout(context.Background(), TimeoutValid)
	result, err = db.ExecContext(ctx, "drop keyspace "+KeyspaceName)
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
