package cql

import (
	"context"
	"database/sql/driver"
	"reflect"
)

// Close a statement
func (cqlStmt *CqlStmt) Close() error {
	if cqlStmt.CqlQuery != nil {
		cqlStmt.CqlQuery.Release()
		cqlStmt.CqlQuery = nil
	}
	return nil
}

// NumInput not supported
func (cqlStmt *CqlStmt) NumInput() int {
	return -1
}

// Exec executes a statement with background context
func (cqlStmt *CqlStmt) Exec(args []driver.Value) (driver.Result, error) {
	return cqlStmt.execContext(context.Background(), valuesToInterface(args))
}

// ExecContext executes a statement with context
func (cqlStmt *CqlStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	values, err := namedValuesToInterface(args)
	if err != nil {
		return nil, err
	}
	return cqlStmt.execContext(ctx, values)
}

// execContext executes a statement with context
func (cqlStmt *CqlStmt) execContext(ctx context.Context, values []interface{}) (driver.Result, error) {
	query := cqlStmt.CqlQuery
	if query == nil {
		return nil, ErrQueryIsNil
	}

	query = query.WithContext(ctx)
	if len(values) > 0 {
		query = query.Bind(values...)
	}
	err := query.Exec()
	if err != nil {
		return nil, err
	}

	return cqlResultStruct{}, nil
}

// Query queries a statement with background context
func (cqlStmt *CqlStmt) Query(args []driver.Value) (driver.Rows, error) {
	return cqlStmt.queryContext(context.Background(), valuesToInterface(args))
}

// QueryContext queries a statement with context
func (cqlStmt *CqlStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	values, err := namedValuesToInterface(args)
	if err != nil {
		return nil, err
	}
	return cqlStmt.queryContext(ctx, values)
}

// queryContext queries a statement with context
func (cqlStmt *CqlStmt) queryContext(ctx context.Context, values []interface{}) (driver.Rows, error) {
	query := cqlStmt.CqlQuery
	if query == nil {
		return nil, ErrQueryIsNil
	}

	query = query.WithContext(ctx)
	if len(values) > 0 {
		query = query.Bind(values...)
	}

	iter := query.Iter()
	return &cqlRowsStruct{
		iter:    iter,
		columns: columnInfoToString(iter.Columns()),
	}, nil
}

// ColumnConverter provides driver ValueConverter for statment
func (cqlStmt *CqlStmt) ColumnConverter(index int) driver.ValueConverter {
	return converter{}
}

// ConvertValue coverts interface value to driver Value
func (c converter) ConvertValue(valueInterface interface{}) (driver.Value, error) {
	valueDriver, err := driver.DefaultParameterConverter.ConvertValue(valueInterface)
	if err == nil {
		return valueDriver, nil
	}

	rv := reflect.ValueOf(valueInterface)
	if rv.Kind() == reflect.Uint64 {
		return rv.Uint(), nil
	}

	return valueDriver, err
}
