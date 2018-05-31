package cql_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/MichaelS11/go-cql-driver"
)

func Example_sqlSelect() {
	// Example shows how to do a basic select

	openString := cql.TestHostValid + "?timeout=" + cql.TimeoutValidString + "&connectTimeout=" + cql.ConnectTimeoutValidString
	if cql.EnableAuthentication {
		openString += "&username=" + cql.Username + "&password=" + cql.Password
	}

	// A normal simple Open to localhost would look like:
	// db, err := sql.Open("cql", "127.0.0.1")
	// For testing, need to use additional variables
	db, err := sql.Open("cql", openString)
	if err != nil {
		fmt.Printf("Open error is not nil: %v", err)
		return
	}
	if db == nil {
		fmt.Println("db is nil")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	rows, err := db.QueryContext(ctx, "select cql_version from system.local")
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}
	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	dest := make([]interface{}, 1)
	destPointer := make([]interface{}, 1)
	destPointer[0] = &dest[0]
	err = rows.Scan(destPointer...)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if len(dest) != 1 {
		fmt.Println("len dest != 1")
		return
	}
	data, ok := dest[0].(string)
	if !ok {
		fmt.Println("dest type not string")
		return
	}
	if len(data) < 3 {
		fmt.Println("data string len too small")
		return
	}

	if rows.Next() {
		fmt.Println("has Next rows")
		return
	}

	err = rows.Err()
	if err != nil {
		fmt.Println("Err error is not nil:", err)
		return
	}
	err = rows.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}
	cancel()

	err = db.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	fmt.Println("received cql_version from system.local")

	// output: received cql_version from system.local
}

func Example_sqlStatement() {
	// Example shows how to use database statement

	openString := cql.TestHostValid + "?timeout=" + cql.TimeoutValidString + "&connectTimeout=" + cql.ConnectTimeoutValidString
	if cql.EnableAuthentication {
		openString += "&username=" + cql.Username + "&password=" + cql.Password
	}

	// A normal simple Open to localhost would look like:
	// db, err := sql.Open("cql", "127.0.0.1")
	// For testing, need to use additional variables
	db, err := sql.Open("cql", openString)
	if err != nil {
		fmt.Printf("Open error is not nil: %v", err)
		return
	}
	if db == nil {
		fmt.Println("db is nil")
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
	stmt, err := db.PrepareContext(ctx, "select cql_version from system.local")
	cancel()
	if err != nil {
		fmt.Println("PrepareContext error is not nil:", err)
		return
	}
	if stmt == nil {
		fmt.Println("stmt is nil")
		return
	}

	ctx, cancel = context.WithTimeout(context.Background(), 55*time.Second)
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		fmt.Println("QueryContext error is not nil:", err)
		return
	}
	if !rows.Next() {
		fmt.Println("no Next rows")
		return
	}

	dest := make([]interface{}, 1)
	destPointer := make([]interface{}, 1)
	destPointer[0] = &dest[0]
	err = rows.Scan(destPointer...)
	if err != nil {
		fmt.Println("Scan error is not nil:", err)
		return
	}

	if len(dest) != 1 {
		fmt.Println("len dest != 1")
		return
	}
	data, ok := dest[0].(string)
	if !ok {
		fmt.Println("dest type not string")
		return
	}
	if len(data) < 3 {
		fmt.Println("data string len too small")
		return
	}

	if rows.Next() {
		fmt.Println("has Next rows")
		return
	}

	err = rows.Err()
	if err != nil {
		fmt.Println("Err error is not nil:", err)
		return
	}
	err = rows.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}
	cancel()

	err = db.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	fmt.Println("received cql_version from system.local")

	// output: received cql_version from system.local
}
