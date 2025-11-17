package godb

import (
"fmt"
)

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child, lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// return &TupleDesc{} // replace me
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	num,err := l.limitTups.EvalExpr(nil)
	if err != nil {
		return nil, err
	}
	if _, ok := num.(IntField); !ok {
		return nil, fmt.Errorf("limit is not IntField")
	}
	// return nil, fmt.Errorf("LimitOp.Iterator not implemented") // replace me
	currentNum := 0
	it,err := l.child.Iterator(tid)
	if err != nil{
		return nil,err
	}
	return func() (*Tuple, error) {
		if currentNum >= int(num.(IntField).Value) {
			return nil, nil
		}
		tp, err := it()
		if err != nil {
			return nil, err
		}
		if tp == nil {
			return nil, nil
		}
		currentNum++
		return tp, nil
	},nil

}
