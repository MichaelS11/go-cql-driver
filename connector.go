// +build go1.10

package cql

import (
	"context"
	"database/sql/driver"
	"io/ioutil"
	"log"
	"os"
)

// NewConnector returns a new database connector
func NewConnector(hosts ...string) driver.Connector {
	return &CqlConnector{
		Logger:        log.New(os.Stderr, "cql ", log.Ldate|log.Ltime|log.LUTC|log.Lshortfile),
		ClusterConfig: NewClusterConfig(hosts...),
	}
}

// Driver returns the cql driver
func (cqlConnector *CqlConnector) Driver() driver.Driver {
	return CqlDriver
}

// Connect returns a new database connection
func (cqlConnector *CqlConnector) Connect(ctx context.Context) (driver.Conn, error) {
	cqlConn := &cqlConnStruct{
		logger:        cqlConnector.Logger,
		context:       ctx,
		clusterConfig: cqlConnector.ClusterConfig,
	}
	if cqlConn.logger == nil {
		cqlConn.logger = log.New(ioutil.Discard, "", 0)
	}

	return cqlConn, nil
}
