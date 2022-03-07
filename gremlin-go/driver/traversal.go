/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package gremlingo

import (
	"errors"
)

type Traverser struct {
	bulk  int64
	value interface{}
}

type Traversal struct {
	graph               *Graph
	traversalStrategies *TraversalStrategies
	bytecode            *bytecode
	remote              *DriverRemoteConnection
	results             ResultSet
}

// ToList returns the result in a list.
func (t *Traversal) ToList() ([]*Result, error) {
	// TODO: AN-979 This wont be needed once DriverRemoteConnection is replaced by TraversalStrategy
	if t.remote == nil {
		return nil, errors.New("cannot invoke this method from an anonymous traversal")
	}

	results, err := t.remote.SubmitBytecode(t.bytecode)
	if err != nil {
		return nil, err
	}
	return results.All()
}

// ToSet returns the results in a set.
func (t *Traversal) ToSet() (map[*Result]bool, error) {
	list, err := t.ToList()
	if err != nil {
		return nil, err
	}

	set := map[*Result]bool{}
	for _, r := range list {
		set[r] = true
	}
	return set, nil
}

// Iterate all the Traverser instances in the traversal and returns the empty traversal
func (t *Traversal) Iterate() (*Traversal, <-chan bool, error) {
	// TODO: AN-979 This wont be needed once DriverRemoteConnection is replaced by TraversalStrategy
	if t.remote == nil {
		return nil, nil, errors.New("cannot invoke this method from an anonymous traversal")
	}

	err := t.bytecode.addStep("none")
	if err != nil {
		return nil, nil, err
	}

	res, err := t.remote.SubmitBytecode(t.bytecode)
	if err != nil {
		return nil, nil, err
	}

	r := make(chan bool)
	go func() {
		defer close(r)

		// Force waiting until complete.
		_, _ = res.All()
		r <- true
	}()

	return t, r, nil
}

func (t *Traversal) HasNext() (bool, error) {
	results, err := t.getResults()
	if err != nil {
		return false, err
	}
	return !results.IsEmpty(), nil
}

func (t *Traversal) Next() (*Result, error) {
	results, err := t.getResults()
	if err != nil {
		return nil, err
	} else if results.IsEmpty() {
		return nil, errors.New("there are no results left")
	}
	return results.one()
}

func (t *Traversal) getResults() (ResultSet, error) {
	if t.results == nil {
		var err error
		t.results, err = t.remote.SubmitBytecode(t.bytecode)
		return t.results, err
	}
	return t.results, nil
}

type Barrier string

const (
	NormSack Barrier = "normSack"
)

type Cardinality string

const (
	Single Cardinality = "single"
	List   Cardinality = "list"
	Set_   Cardinality = "set"
)

type Column string

const (
	Keys   Column = "keys"
	Values Column = "values"
)

type Direction string

const (
	In   Direction = "IN"
	Out  Direction = "OUT"
	Both Direction = "BOTH"
)

type Order string

const (
	Shuffle Order = "shuffle"
	Asc     Order = "asc"
	Desc    Order = "desc"
)

type Pick string

const (
	Any  Pick = "any"
	None Pick = "none"
)

type Pop string

const (
	First Pop = "first"
	Last  Pop = "last"
	All   Pop = "all"
	Mixed Pop = "mixed"
)

type Scope string

const (
	Global Scope = "global"
	Local  Scope = "local"
)

type T string

const (
	Id    T = "id"
	Label T = "label"
	Id_   T = "id_"
	Key   T = "key"
	Value T = "value"
)

type Operator string

const (
	Sum     Operator = "sum"
	Minus   Operator = "minus"
	Mult    Operator = "mult"
	Div     Operator = "div"
	Min     Operator = "min"
	Max     Operator = "max"
	Assign  Operator = "assign"
	And     Operator = "and"
	Or      Operator = "or"
	AddAll  Operator = "addAll"
	SumLong Operator = "sumLong"
)

type p struct {
	operator string
	values   []interface{}
}

// Predicate interface
type Predicate interface {
	// Between Predicate to determine if value is within (inclusive) the range of two specified values.
	Between(args ...interface{}) Predicate
	// Eq Predicate to determine if equal to.
	Eq(args ...interface{}) Predicate
	// Gt Predicate to determine if greater than.
	Gt(args ...interface{}) Predicate
	// Gte Predicate to determine if greater than or equal to.
	Gte(args ...interface{}) Predicate
	// Inside Predicate to determine if value is within range of specified values (exclusive).
	Inside(args ...interface{}) Predicate
	// Lt Predicate to determine if less than.
	Lt(args ...interface{}) Predicate
	// Lte Predicate to determine if less than or equal to.
	Lte(args ...interface{}) Predicate
	// Neq Predicate to determine if not equal to.
	Neq(args ...interface{}) Predicate
	// Not Predicate gives the opposite of the specified Predicate.
	Not(args ...interface{}) Predicate
	// Outside Predicate to determine of value is not within range of specified values (exclusive).
	Outside(args ...interface{}) Predicate
	// Test Evalutes Predicate on given argument.
	Test(args ...interface{}) Predicate
	// Within Predicate determines if value is within given list of values.
	Within(args ...interface{}) Predicate
	// Without Predicate determines if value is not within the specified
	Without(args ...interface{}) Predicate
	// And Predicate returns a Predicate composed of two predicates (logical AND of them).
	And(args ...interface{}) Predicate
	// Or Predicate returns a Predicate composed of two predicates (logical OR of them).
	Or(args ...interface{}) Predicate
}

var P Predicate = &p{}

func newP(operator string, args ...interface{}) Predicate {
	values := make([]interface{}, 0)
	for _, arg := range args {
		values = append(values, arg)
	}
	return &p{operator: operator, values: values}
}

func newPWithP(operator string, pp p, args ...interface{}) Predicate {
	values := make([]interface{}, len(args)+1)
	for _, arg := range args {
		values = append(values, arg)
	}
	values[len(values)-1] = pp
	return &p{operator: operator, values: values}
}

func (_ *p) Between(args ...interface{}) Predicate {
	return newP("between", args...)
}

func (_ *p) Eq(args ...interface{}) Predicate {
	return newP("eq", args...)
}

func (_ *p) Gt(args ...interface{}) Predicate {
	return newP("gt", args...)
}

func (_ *p) Gte(args ...interface{}) Predicate {
	return newP("gte", args...)
}

func (_ *p) Inside(args ...interface{}) Predicate {
	return newP("inside", args...)
}

func (_ *p) Lt(args ...interface{}) Predicate {
	return newP("lt", args...)
}

func (_ *p) Lte(args ...interface{}) Predicate {
	return newP("lte", args...)
}

func (_ *p) Neq(args ...interface{}) Predicate {
	return newP("neq", args...)
}

func (_ *p) Not(args ...interface{}) Predicate {
	return newP("not", args...)
}

func (_ *p) Outside(args ...interface{}) Predicate {
	return newP("outside", args...)
}

func (_ *p) Test(args ...interface{}) Predicate {
	return newP("test", args...)
}

func (_ *p) Within(args ...interface{}) Predicate {
	return newP("within", args...)
}

func (_ *p) Without(args ...interface{}) Predicate {
	return newP("without", args...)
}

func (pp *p) And(args ...interface{}) Predicate {
	return newPWithP("and", *pp, args...)
}

func (pp *p) Or(args ...interface{}) Predicate {
	return newPWithP("or", *pp, args...)
}

type TextPredicate interface {
	// Containing TextPredicate determines if a string contains a given value.
	Containing(args ...interface{}) TextPredicate
	// EndingWith TextPredicate determines if a string ends with a given value.
	EndingWith(args ...interface{}) TextPredicate
	// NotContaining TextPredicate determines if a string does not contain a given value.
	NotContaining(args ...interface{}) TextPredicate
	// NotEndingWith TextPredicate determines if a string does not end with a given value.
	NotEndingWith(args ...interface{}) TextPredicate
	// NotStartingWith TextPredicate determines if a string does not start with a given value.
	NotStartingWith(args ...interface{}) TextPredicate
	// StartingWith TextPredicate determines if a string starts with a given value.
	StartingWith(args ...interface{}) TextPredicate
	// And TextPredicate returns a TextPredicate composed of two predicates (logical AND of them).
	And(args ...interface{}) TextPredicate
	// Or TextPredicate returns a TextPredicate composed of two predicates (logical OR of them).
	Or(args ...interface{}) TextPredicate
}

type textP p

var TextP TextPredicate = &textP{}

func newTextP(operator string, args ...interface{}) TextPredicate {
	values := make([]interface{}, 0)
	for _, arg := range args {
		values = append(values, arg)
	}
	return &textP{operator: operator, values: values}
}

func newTextPWithP(operator string, tp textP, args ...interface{}) TextPredicate {
	values := make([]interface{}, len(args)+1)
	for _, arg := range args {
		values = append(values, arg)
	}
	values[len(values)-1] = tp
	return &textP{operator: operator, values: values}
}

func (_ *textP) Containing(args ...interface{}) TextPredicate {
	return newTextP("containing", args...)
}

func (_ *textP) EndingWith(args ...interface{}) TextPredicate {
	return newTextP("endingWith", args...)
}

func (_ *textP) NotContaining(args ...interface{}) TextPredicate {
	return newTextP("notContaining", args...)
}

func (_ *textP) NotEndingWith(args ...interface{}) TextPredicate {
	return newTextP("notEndingWith", args...)
}

func (_ *textP) NotStartingWith(args ...interface{}) TextPredicate {
	return newTextP("notStartingWith", args...)
}

func (_ *textP) StartingWith(args ...interface{}) TextPredicate {
	return newTextP("startingWith", args...)
}

func (tp *textP) And(args ...interface{}) TextPredicate {
	return newTextPWithP("and", *tp, args...)
}

func (tp *textP) Or(args ...interface{}) TextPredicate {
	return newTextPWithP("or", *tp, args...)
}
