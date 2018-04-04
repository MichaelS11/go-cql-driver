package cql

import (
	"context"
	"database/sql/driver"
)

// Close a database connection
func (cqlConn *cqlConnStruct) Close() error {
	if cqlConn.session != nil {
		cqlConn.session.Close()
		cqlConn.session = nil
	}
	return nil
}

// Ping a database connection
func (cqlConn *cqlConnStruct) Ping(ctx context.Context) error {
	var err error

	if cqlConn.session == nil {
		cqlConn.session, err = cqlConn.clusterConfig.CreateSession()
		if err != nil {
			cqlConn.Close()
			cqlConn.logger.Print("Ping CreateSession error: ", err)
			return driver.ErrBadConn
		}
		cqlConn.pingQuery = cqlConn.session.Query("select cql_version from system.local")
	}

	iter := cqlConn.pingQuery.WithContext(ctx).Iter()

	rowData, err := iter.RowData()
	if err != nil {
		iter.Close()
		cqlConn.Close()
		cqlConn.logger.Print("Ping RowData error: ", err)
		return driver.ErrBadConn
	}
	if len(rowData.Values) != 1 {
		iter.Close()
		cqlConn.Close()
		cqlConn.logger.Print("Ping len(Values) != 1")
		return driver.ErrBadConn
	}

	if !iter.Scan(rowData.Values...) {
		err = iter.Close()
		if err != nil {
			cqlConn.Close()
		} else {
			err = cqlConn.Close()
		}
		cqlConn.logger.Print("Ping Scan error: ", err)
		return driver.ErrBadConn
	}
	err = iter.Close()
	if err != nil {
		cqlConn.Close()
		cqlConn.logger.Print("Ping iter Close error: ", err)
		return driver.ErrBadConn
	}

	data, ok := rowData.Values[0].(*string)
	if !ok {
		cqlConn.Close()
		cqlConn.logger.Print("Ping Value not *string")
		return driver.ErrBadConn
	}
	if len(*data) < 1 {
		cqlConn.Close()
		cqlConn.logger.Print("Ping len(data) < 1")
		return driver.ErrBadConn
	}

	return nil
}

// Prepare a query, uses connection conntext
func (cqlConn *cqlConnStruct) Prepare(query string) (driver.Stmt, error) {
	return cqlConn.PrepareContext(cqlConn.context, query)
}

// Prepare a query with context
func (cqlConn *cqlConnStruct) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	var err error

	if cqlConn.session == nil {
		err = cqlConn.Ping(ctx)
		if err != nil {
			return nil, err
		}
	}

	return &CqlStmt{
		CqlQuery: cqlConn.session.Query(query).WithContext(ctx),
	}, nil
}

// Begin not supported
func (cqlConn *cqlConnStruct) Begin() (driver.Tx, error) {
	return nil, ErrNotSupported
}

// BeginTx not supported
func (cqlConn *cqlConnStruct) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return nil, ErrNotSupported
}
