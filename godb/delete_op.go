package godb

import (
// "fmt"
)

type DeleteOp struct {
	// TODO: some code goes here
	DeleteFile DBFile
	Child      Operator
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	// return nil // replace me
	return &DeleteOp{deleteFile, child}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// return &TupleDesc{} // replace me
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}
}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// return nil, fmt.Errorf("DeleteOp.Iterator not implemented") // replace me
	return func() (*Tuple, error) {
		it, err := dop.Child.Iterator(tid)
		if err != nil {
			return nil, err
		}
		num := 0
		for tp, err := it(); err == nil; tp, err = it() {
			if tp == nil {
				break
			}
			err := dop.DeleteFile.deleteTuple(tp, tid)
			if err != nil {
				return nil, err
			}
			num++
		}
		return &Tuple{Desc: *dop.Descriptor(), Fields: []DBValue{IntField{Value: int64(num)}}, Rid: 0}, nil
	}, nil
}
