package godb

import (
	// "fmt"
	"sort"
)

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	// TODO: You may want to add additional fields here
	ascending []bool
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	// return nil, fmt.Errorf("NewOrderBy not implemented.") //replace me
	return &OrderBy{
		orderBy: orderByFields,
		child:   child,
		ascending: ascending,
	}, nil

}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	// return &TupleDesc{} // replace me
	return o.child.Descriptor()
}

// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	// return nil, fmt.Errorf("OrderBy.Iterator not implemented") // replace me
	tuples := []*Tuple{}
	it ,err := o.child.Iterator(tid)
	if err != nil{
		return nil,err
	}
	for t,err:= it();t != nil && err == nil;t,err = it(){
		tuples = append(tuples,t)
	}
	if err != nil{
		return nil,err
	}
	obp := &orderByProcessor{
		orderBy: o.orderBy,
		tuples: tuples,
		ascending: o.ascending,
		currentIdx: 0,
	}
	obp.sortTuples()
	return obp.iterateFunc(),nil
}

func (obp *orderByProcessor) sortTuples(){
	sort.Sort(obp)
}

func (obp *orderByProcessor) iterateFunc() func() (*Tuple, error){
	return func() (*Tuple, error){
		if obp.currentIdx >= len(obp.tuples){
			return nil,nil
		}
		tuple := obp.tuples[obp.currentIdx]
		obp.currentIdx += 1
		return tuple,nil
	}
}

type orderByProcessor struct {
	orderBy    []Expr
	tuples     []*Tuple
	ascending  []bool
	currentIdx int
}

func (obp *orderByProcessor) Len() int {
	return len(obp.tuples)
}

func (obp *orderByProcessor) Swap(i, j int) {
	obp.tuples[i], obp.tuples[j] = obp.tuples[j], obp.tuples[i]
}

func (obp *orderByProcessor) Less(i, j int) bool {
	for idx, expr := range obp.orderBy {
		valI, err := expr.EvalExpr(obp.tuples[i])
		if err != nil {
			continue
		}
		valJ, err := expr.EvalExpr(obp.tuples[j])
		if err != nil {
			continue
		}
		var cmp int64
		switch vI := valI.(type) {
		case IntField:
			vJ := valJ.(IntField)
			cmp = compareIntFields(vI, vJ)
		case StringField:
			vJ := valJ.(StringField)
			cmp = compareStringFields(vI, vJ)
		default:
			continue
		}
		if cmp < 0 {
			return obp.ascending[idx]
		} else if cmp > 0 {
			return !obp.ascending[idx]
		}
	}
	return false
}