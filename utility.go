package cql

import (
	"database/sql/driver"
	"fmt"
	"reflect"

	"github.com/gocql/gocql"
)

// valuesToInterface coverts driver.Value to interface
func valuesToInterface(args []driver.Value) []interface{} {
	values := make([]interface{}, len(args))
	for i := 0; i < len(args); i++ {
		values[i] = args[i]
	}
	return values
}

// namedValuesToInterface coverts driver.NamedValue to interface
func namedValuesToInterface(namedValues []driver.NamedValue) ([]interface{}, error) {
	values := make([]interface{}, len(namedValues))
	for i := 0; i < len(namedValues); i++ {
		if len(namedValues[i].Name) > 0 {
			return []interface{}{}, ErrNamedValuesNotSupported
		}
		if namedValues[i].Ordinal < 1 || namedValues[i].Ordinal > len(namedValues) {
			return []interface{}{}, ErrOrdinalOutOfRange
		}
		values[namedValues[i].Ordinal-1] = namedValues[i].Value
	}
	return values, nil
}

// columnInfoToString coverts gocql.ColumnInfo to string
func columnInfoToString(columnInfo []gocql.ColumnInfo) []string {
	names := make([]string, len(columnInfo))
	for i := 0; i < len(columnInfo); i++ {
		names[i] = columnInfo[i].Name
	}
	return names
}

// interfaceToValue coverts interface to driver.Value
func interfaceToValue(sourceInterface interface{}) (driver.Value, error) {
	source := reflect.ValueOf(sourceInterface)
	if source.Kind() != reflect.Ptr {
		return driver.Value(nil), fmt.Errorf("source is not a pointer")
	}
	return driver.Value(source.Elem().Interface()), nil
}
