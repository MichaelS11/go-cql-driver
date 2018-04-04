package cql

import (
	"database/sql/driver"
	"fmt"
	"io"
)

// Close the rows
func (cqlRows *cqlRowsStruct) Close() error {
	if cqlRows.iter == nil {
		return nil
	}
	err := cqlRows.iter.Close()
	cqlRows.iter = nil
	return err
}

// Columns returns the columns for rows
func (cqlRows *cqlRowsStruct) Columns() []string {
	return cqlRows.columns
}

// Next rows
func (cqlRows *cqlRowsStruct) Next(dest []driver.Value) error {
	if cqlRows.iter == nil {
		return io.EOF
	}

	rowData, err := cqlRows.iter.RowData()
	if err != nil {
		cqlRows.iter.Close()
		return fmt.Errorf("RowData error: %v", err)
	}
	length := len(rowData.Values)
	if length < 1 {
		err = cqlRows.Close()
		if err != nil {
			return err
		}
		return io.EOF
	}

	if !cqlRows.iter.Scan(rowData.Values...) {
		err = cqlRows.Close()
		if err != nil {
			return err
		}
		return io.EOF
	}

	if len(dest) < length {
		length = len(dest)
	}
	for i := 0; i < length; i++ {
		dest[i], err = interfaceToValue(rowData.Values[i])
		if err != nil {
			cqlRows.Close()
			return fmt.Errorf("interfaceToValue error: %v", err)
		}
	}

	return nil
}
