package godb

import (
// "fmt"
)

// interface for an aggregation state
type AggState interface {
	// Initializes an aggregation state. Is supplied with an alias, an expr to
	// evaluate an input tuple into a DBValue, and a getter to extract from the
	// DBValue its int or string field's value.
	Init(alias string, expr Expr) error

	// Makes an copy of the aggregation state.
	Copy() AggState

	// Adds an tuple to the aggregation state.
	AddTuple(*Tuple)

	// Returns the final result of the aggregation as a tuple.
	Finalize() *Tuple

	// Gets the tuple description of the tuple that Finalize() returns.
	GetTupleDesc() *TupleDesc
}

// Implements the aggregation state for COUNT
// We are supplying the implementation of CountAggState as an example. You need to
// implement the rest of the aggregation states.
type CountAggState struct {
	alias string
	expr  Expr
	count int
}

func (a *CountAggState) Copy() AggState {
	return &CountAggState{a.alias, a.expr, a.count}
}

func (a *CountAggState) Init(alias string, expr Expr) error {
	a.count = 0
	a.expr = expr
	a.alias = alias
	return nil
}

func (a *CountAggState) AddTuple(t *Tuple) {
	a.count++
}

func (a *CountAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{int64(a.count)}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

func (a *CountAggState) GetTupleDesc() *TupleDesc {
	ft := FieldType{a.alias, "", IntType}
	fts := []FieldType{ft}
	td := TupleDesc{}
	td.Fields = fts
	return &td
}

// Implements the aggregation state for SUM
type SumAggState struct {
	sum int64
	alias string
	expr  Expr

}

func (a *SumAggState) Copy() AggState {
	return &SumAggState{a.sum,a.alias,a.expr}
}

func (a *SumAggState) Init(alias string, expr Expr) error {
	a.sum = 0
	a.alias = alias
	a.expr = expr
	return nil
}

func (a *SumAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	intVal, ok := val.(IntField)
	if !ok {
		return
	}
	a.sum += intVal.Value
}

func (a *SumAggState) GetTupleDesc() *TupleDesc {
	return &TupleDesc{[]FieldType{
		{a.alias, "", IntType},
	}}
}

func (a *SumAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	f := IntField{a.sum}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for AVG
// Note that we always AddTuple() at least once before Finalize()
// so no worries for divide-by-zero
type AvgAggState struct {
	sum   int64
	count int64
	alias string
	expr  Expr
}

func (a *AvgAggState) Copy() AggState {
	return &AvgAggState{a.sum, a.count, a.alias, a.expr}
}

func (a *AvgAggState) Init(alias string, expr Expr) error {
	a.sum = 0
	a.count = 0
	a.alias = alias
	a.expr = expr
	return nil
}

func (a *AvgAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	intVal, ok := val.(IntField)
	if !ok {
		return
	}
	a.sum += intVal.Value
	a.count++
}

func (a *AvgAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	// return &TupleDesc{} // replace me
	return &TupleDesc{[]FieldType{
		{a.alias, "", IntType},
	}}
}

func (a *AvgAggState) Finalize() *Tuple {
	td := a.GetTupleDesc()
	avg := a.sum / a.count
	f := IntField{avg}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MAX
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN max
type MaxAggState struct {
	max   int64
	alias string
	expr  Expr
}

func (a *MaxAggState) Copy() AggState {
	return &MaxAggState{a.max, a.alias, a.expr}
}

func (a *MaxAggState) Init(alias string, expr Expr) error {
	a.max = -1 << 63 // smallest int64
	a.alias = alias
	a.expr = expr
	return nil
}

func (a *MaxAggState) AddTuple(t *Tuple) {
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	intVal, ok := val.(IntField)
	if !ok {
		return
	}
	if intVal.Value > a.max {
		a.max = intVal.Value
	}
}

func (a *MaxAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	// return &TupleDesc{} // replace me
	return &TupleDesc{[]FieldType{
		{a.alias, "", IntType},
	}}
}

func (a *MaxAggState) Finalize() *Tuple {
	// TODO: some code goes here
	// return &Tuple{} // replace me
	td := a.GetTupleDesc()
	f := IntField{a.max}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}

// Implements the aggregation state for MIN
// Note that we always AddTuple() at least once before Finalize()
// so no worries for NaN min
type MinAggState struct {
	// TODO: some code goes here
	min   int64
	minstring string
	td TupleDesc
	alias string
	expr  Expr
}

func (a *MinAggState) Copy() AggState {
	// TODO: some code goes here
	// return nil // replace me
	// return &MinAggState{a.min, a.alias, a.expr}
	return &MinAggState{a.min, a.minstring, a.td, a.alias, a.expr}
}

func (a *MinAggState) Init(alias string, expr Expr) error {
	// TODO: some code goes here
	// return fmt.Errorf("MinAggState.Init not implemented") // replace me
	// a.min = 1<<63 - 1 // largest int64
	a.alias = alias
	a.expr = expr
	a.td = *a.GetTupleDesc()
	if expr.GetExprType().Ftype == IntType {
		a.min = 1<<63 - 1
	} else if expr.GetExprType().Ftype == StringType {
		a.minstring = string([]byte{127, 127, 127, 127, 127, 127, 127, 127, 127, 127})
	}
	return nil
}

func (a *MinAggState) AddTuple(t *Tuple) {
	// TODO: some code goes here
	val, err := a.expr.EvalExpr(t)
	if err != nil {
		return
	}
	switch v := val.(type) {
	case IntField:
		if v.Value < a.min {
			a.min = v.Value
		}
	case StringField:
		if v.Value < a.minstring {
			a.minstring = v.Value
		}
	}
}

func (a *MinAggState) GetTupleDesc() *TupleDesc {
	// TODO: some code goes here
	// return &TupleDesc{} // replace me
	return &TupleDesc{[]FieldType{
		{a.alias, "", a.expr.GetExprType().Ftype},
	}}

}

func (a *MinAggState) Finalize() *Tuple {
	// TODO: some code goes here
	// return &Tuple{} // replace me
	td := a.GetTupleDesc()
	var f DBValue
	if td.Fields[0].Ftype == IntType {
		f = IntField{a.min}
	} else if td.Fields[0].Ftype == StringType {
		f = StringField{a.minstring}
	}
	fs := []DBValue{f}
	t := Tuple{*td, fs, nil}
	return &t
}
