package cql

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"log"
)

// Open returns a new database connection
func (cqlDriver *CqlDriverStruct) Open(configString string) (driver.Conn, error) {
	var err error
	cqlConn := &cqlConnStruct{
		logger:  cqlDriver.Logger,
		context: context.Background(),
	}
	if cqlConn.logger == nil {
		cqlConn.logger = log.New(ioutil.Discard, "", 0)
	}

	cqlConn.clusterConfig, err = ConfigStringToClusterConfig(configString)
	if err != nil {
		return nil, fmt.Errorf("ConfigStringToClusterConfig error: %v", err)
	}

	return cqlConn, nil
}
