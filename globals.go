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
		// This will only work if Go sql every gives access to the driver
		CqlQuery *gocql.Query
	}

	cqlResultStruct struct {
	}

	cqlRowsStruct struct {
		iter    *gocql.Iter
		columns []string
	}

	converter struct{}
)

var (
	// ErrNotSupported is returned for any method that is not supported
	ErrNotSupported = fmt.Errorf("not supported")
	// ErrNotImplementedYet is returned for any method that is not implemented yet
	ErrNotImplementedYet = fmt.Errorf("not implemented yet")
	// ErrQueryIsNil is returned when a query is nil
	ErrQueryIsNil = fmt.Errorf("query is nil")
	// ErrNamedValuesNotSupported is returned when values are named. Named values are not supported.
	ErrNamedValuesNotSupported = fmt.Errorf("named values not supported")
	// ErrOrdinalOutOfRange is returned when values ordinal is out of range
	ErrOrdinalOutOfRange = fmt.Errorf("ordinal out of range")

	// CqlDriver is the sql driver
	CqlDriver = &CqlDriverStruct{
		Logger: log.New(os.Stderr, "cql ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile),
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

// DbConsistency maps gocql consistency levels to string
var DbConsistency = map[gocql.Consistency]string{
	gocql.Any:         "any",
	gocql.One:         "one",
	gocql.Two:         "two",
	gocql.Three:       "three",
	gocql.Quorum:      "quorum",
	gocql.All:         "all",
	gocql.LocalQuorum: "localQuorum",
	gocql.EachQuorum:  "eachQuorum",
	gocql.LocalOne:    "localOne",
}

func init() {
	sql.Register("cql", CqlDriver)
}
