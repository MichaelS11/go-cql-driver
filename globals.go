package cql

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"github.com/gocql/gocql"
)

type (
	// CqlDriverStruct is the sql driver
	CqlDriverStruct struct {
		// Logger is used to log connection ping errors
		Logger *log.Logger
	}

	// CqlConnector is the sql driver connector
	CqlConnector struct {
		// Logger is used to log connection ping errors
		Logger *log.Logger
		// ClusterConfig is used for changing config options
		// https://godoc.org/github.com/gocql/gocql#ClusterConfig
		ClusterConfig *gocql.ClusterConfig
	}

	cqlConnStruct struct {
		logger        *log.Logger
		clusterConfig *gocql.ClusterConfig
		context       context.Context
		session       *gocql.Session
		pingQuery     *gocql.Query
	}

	// CqlStmt is the sql driver statement
	CqlStmt struct {
		// CqlQuery is used for changing query options
		// https://godoc.org/github.com/gocql/gocql#Query
		CqlQuery *gocql.Query
	}

	cqlResultStruct struct {
	}

	cqlRowsStruct struct {
		iter    *gocql.Iter
		columns []string
	}
)

var (
	// ErrNotSupported is returned for any method that is not supported
	ErrNotSupported = fmt.Errorf("not supported")
	// ErrNotImplementedYet is returned for any method that is not implemented yet
	ErrNotImplementedYet = fmt.Errorf("not implemented yet")
	// ErrQueryIsNil is returned when a query is nil
	ErrQueryIsNil = fmt.Errorf("query is nil")
	// ErrArgNamedValuesNotSupported is returned with values are named. Named values are not supported.
	ErrArgNamedValuesNotSupported = fmt.Errorf("arg named values not supported")
	// ErrArgOrdinalOutOfRange is returned when values ordinal is out of range
	ErrArgOrdinalOutOfRange = fmt.Errorf("arg ordinal out of range")

	CqlDriver = &CqlDriverStruct{
		Logger: log.New(os.Stderr, "cql ", log.Ldate|log.Ltime|log.LUTC|log.Llongfile),
	}
)

// DbConsistencyLevels maps string to gocql consistency levels
var DbConsistencyLevels = map[string]gocql.Consistency{
	"any":         gocql.Any,
	"one":         gocql.One,
	"two":         gocql.Two,
	"three":       gocql.Three,
	"quorum":      gocql.Quorum,
	"all":         gocql.All,
	"localQuorum": gocql.LocalQuorum,
	"eachQuorum":  gocql.EachQuorum,
	"localOne":    gocql.LocalOne,
}

func init() {
	sql.Register("cql", CqlDriver)
}
