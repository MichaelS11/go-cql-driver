package cql_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/MichaelS11/go-cql-driver"
)

func Example_sqlSelect() {
	// TestHostValid is the host IP address as a string
	db, err := sql.Open("cql", cql.TestHostValid+"?timeout=10s&connectTimeout=10s")
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
	cancel()
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

	err = db.Close()
	if err != nil {
		fmt.Println("Close error is not nil:", err)
		return
	}

	fmt.Println("received cql_version from system.local")

	// output: received cql_version from system.local
}
