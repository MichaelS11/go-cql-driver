// +build go1.10

package cql_test

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/MichaelS11/go-cql-driver"
)

func Example_sqlConnector() {
	// Example shows how to use OpenDB Connector

	// Normal NewConnector to localhost would look like:
	// connector := cql.NewConnector("127.0.0.1")
	// For testing, need to use the variable TestHostValid
	connector := cql.NewConnector(cql.TestHostValid)

	db := sql.OpenDB(connector)

	// If you would like change some of the ClusterConfig options
	// https://godoc.org/github.com/gocql/gocql#ClusterConfig
	// Can do a type cast to get to them
	cqlConnector := connector.(*cql.CqlConnector)
	cqlConnector.ClusterConfig.Timeout = cql.TimeoutValid
	cqlConnector.ClusterConfig.ConnectTimeout = cql.ConnectTimeoutValid

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
