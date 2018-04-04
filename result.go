package cql

// LastInsertId not supported
func (cqlResult cqlResultStruct) LastInsertId() (int64, error) {
	return -1, ErrNotSupported
}

// RowsAffected not supported
func (cqlResult cqlResultStruct) RowsAffected() (int64, error) {
	return -1, ErrNotSupported
}
