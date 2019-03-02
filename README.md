# Go CQL Driver

[![GoDoc Reference](https://godoc.org/github.com/MichaelS11/go-cql-driver?status.svg)](http://godoc.org/github.com/MichaelS11/go-cql-driver)
[![Build Status](https://travis-ci.org/MichaelS11/go-cql-driver.svg)](https://travis-ci.org/MichaelS11/go-cql-driver)
[![Coverage](https://codecov.io/gh/MichaelS11/go-cql-driver/branch/master/graph/badge.svg)](https://codecov.io/gh/MichaelS11/go-cql-driver)
[![Go Report Card](https://goreportcard.com/badge/github.com/MichaelS11/go-cql-driver)](https://goreportcard.com/report/github.com/MichaelS11/go-cql-driver)

Golang CQL Driver conforming to the built-in database/sql interface

This is a database/sql interface wrapper for https://github.com/gocql/gocql

## Get

go get github.com/MichaelS11/go-cql-driver

## Example

A simple SQL select example can be found on the godoc

https://godoc.org/github.com/MichaelS11/go-cql-driver#example-package--SqlSelect

## Important note:

When done with rows from QueryContext or Query, make sure to check errors from Close and Err
```go
	err = rows.Close()
	if err != nil {
		fmt.Println(err)
		return
	}
	cancel()

	err = rows.Err()
	if err != nil {
		fmt.Println(err)
		return
	}
```