package godb

// import "fmt"

type InsertOp struct {
	// TODO: some code goes here
	insertFile DBFile
	child      Operator
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	// return nil
	return &InsertOp{insertFile, child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// return nil
	return &TupleDesc{[]FieldType{{"count", "", IntType}}}

}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	return func() (*Tuple, error) {
		it,err:= iop.child.Iterator(tid)
		if err != nil {
			return nil,err
		}
		num := 0
		for tp, err := it(); err == nil; tp, err = it() {
			if tp == nil{
				break
			}

			tp1 := tp.copy()
			iop.insertFile.insertTuple(tp1, tid)
			num++
		}
		return &Tuple{Desc: *iop.Descriptor(), Fields: []DBValue{IntField{Value: int64(num)}}, Rid: 0}, nil
	},nil

}
