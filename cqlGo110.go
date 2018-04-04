// +build go1.10

package cql

import (
	"database/sql/driver"
	"fmt"
)

// OpenConnector returns a new database connector
func (cqlDriver *CqlDriverStruct) OpenConnector(configString string) (driver.Connector, error) {
	var err error
	cqlConnector := &CqlConnector{
		Logger: cqlDriver.Logger,
	}

	cqlConnector.ClusterConfig, err = ConfigStringToClusterConfig(configString)
	if err != nil {
		return nil, fmt.Errorf("ConfigStringToClusterConfig error: %v", err)
	}

	return cqlConnector, nil
}
