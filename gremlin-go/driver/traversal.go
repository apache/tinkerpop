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

// Traverser is the objects propagating through the traversal.
type Traverser struct {
	bulk  int64
	value interface{}
}

// Traversal is the primary way in which graphs are processed.
type Traversal struct {
	graph    *Graph
	bytecode *bytecode
	remote   *DriverRemoteConnection
	results  ResultSet
}

// ToList returns the result in a list.
func (t *Traversal) ToList() ([]*Result, error) {
	if t.remote == nil {
		return nil, newError(err0901ToListAnonTraversalError)
	}

	results, err := t.remote.submitBytecode(t.bytecode)
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

// Iterate all the Traverser instances in the traversal.
func (t *Traversal) Iterate() <-chan error {
	r := make(chan error)

	go func() {
		defer close(r)

		if t.remote == nil {
			r <- newError(err0902IterateAnonTraversalError)
			return
		}

		if err := t.bytecode.addStep("none"); err != nil {
			r <- err
			return
		}

		res, err := t.remote.submitBytecode(t.bytecode)
		if err != nil {
			r <- err
			return
		}

		// Force waiting until complete.
		_, err = res.All()
		r <- err
	}()

	return r
}

// HasNext returns true if the result is not empty.
func (t *Traversal) HasNext() (bool, error) {
	results, err := t.GetResultSet()
	if err != nil {
		return false, err
	}
	return !results.IsEmpty(), nil
}

// Next returns next result.
func (t *Traversal) Next() (*Result, error) {
	results, err := t.GetResultSet()
	if err != nil {
		return nil, err
	}
	if results.IsEmpty() {
		return nil, newError(err0903NextNoResultsLeftError)
	}
	result, _, err := results.One()
	return result, err
}

// GetResultSet submits the traversal and returns the ResultSet.
func (t *Traversal) GetResultSet() (ResultSet, error) {
	if t.results == nil {
		results, err := t.remote.submitBytecode(t.bytecode)
		if err != nil {
			return nil, err
		}
		t.results = results
	}
	return t.results, nil
}

// Barrier is any step that requires all left traversers to be processed prior to emitting result traversers to the right.
type Barrier string

const (
	NormSack Barrier = "normSack"
)

// Cardinality of Vertex Properties.
type Cardinality string

const (
	Single Cardinality = "single"
	List   Cardinality = "list"
	Set_   Cardinality = "set"
)

// Column references a particular type of column in a complex data structure.
type Column string

const (
	// Keys associated with the data structure.
	Keys Column = "keys"
	// Values associated with the data structure.
	Values Column = "values"
)

// Direction is used to denote the direction of an Edge or location of a Vertex on an Edge.
type Direction string

const (
	// In refers to an incoming direction.
	In Direction = "IN"
	// Out refers to an outgoing direction.
	Out Direction = "OUT"
	// Both refers to either direction In or Out.
	Both Direction = "BOTH"
)

// Order provides comparator instances for ordering traversers.
type Order string

const (
	// Shuffle is Order in a random fashion.
	Shuffle Order = "shuffle"
	// Asc is Order in ascending fashion.
	Asc Order = "asc"
	// Desc is Order in descending fashion.
	Desc Order = "desc"
)

type Pick string

const (
	Any  Pick = "any"
	None Pick = "none"
)

// Pop is used to determine whether the first value, last value, or all values are gathered.
type Pop string

const (
	// First is the first item in an ordered collection.
	First Pop = "first"
	// Last is the last item in an ordered collection.
	Last Pop = "last"
	// All the items in an ordered collection.
	All Pop = "all"
	// Mixed is either a list (for multiple) or an object (for singles).
	Mixed Pop = "mixed"
)

// Scope is used in many Step instance can have a variable scope which alter the manner
// in which the step will behave in relation to how the traversers are processed.
type Scope string

const (
	// Global informs the step to operate on the entire traversal.
	Global Scope = "global"
	// Local informs the step to operate on the current object in the step.
	Local Scope = "local"
)

// T is string symbols.
type T string

const (
	Id    T = "id"
	Label T = "label"
	Id_   T = "id_"
	Key   T = "key"
	Value T = "value"
)

// Operator is a set of operations for traversal steps.
type Operator string

const (
	// Sum is an addition.
	Sum Operator = "sum"
	// Minus is a subtraction.
	Minus Operator = "minus"
	// Mult is a multiplication.
	Mult Operator = "mult"
	// Div is a division.
	Div Operator = "div"
	// Min selects the smaller of the values.
	Min Operator = "min"
	// Max selects the larger of the values.
	Max Operator = "max"
	// Assign returns the second value to the function.
	Assign Operator = "assign"
	// And applies "and" to boolean values.
	And Operator = "and"
	// Or applies "or" to boolean values.
	Or Operator = "or"
	// AddAll Takes all objects in the second Slice and adds them to the first. If the first is null,
	// then the second Slice is returned and if the second is null then the first is returned.
	// If both are null then null is returned. Arguments must be of type Map or Slice.
	AddAll Operator = "addAll"
	// SumLong sums and adds long values.
	SumLong Operator = "sumLong"
)

type p struct {
	operator string
	values   []interface{}
}

// Predicate interface.
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
	// Test evaluates Predicate on given argument.
	Test(args ...interface{}) Predicate
	// Within Predicate determines if value is within given list of values.
	Within(args ...interface{}) Predicate
	// Without Predicate determines if value is not within the specified.
	Without(args ...interface{}) Predicate
	// And Predicate returns a Predicate composed of two predicates (logical AND of them).
	And(args ...interface{}) Predicate
	// Or Predicate returns a Predicate composed of two predicates (logical OR of them).
	Or(args ...interface{}) Predicate
}

var P Predicate = &p{}

func newP(operator string, args ...interface{}) Predicate {
	values := make([]interface{}, 0)
	values = append(values, args...)
	return &p{operator: operator, values: values}
}

func newPWithP(operator string, pp p, args ...interface{}) Predicate {
	values := make([]interface{}, 1)
	values[0] = pp
	values = append(values, args...)
	return &p{operator: operator, values: values}
}

// Between Predicate to determine if value is within (inclusive) the range of two specified values.
func (*p) Between(args ...interface{}) Predicate {
	return newP("between", args...)
}

// Eq Predicate to determine if equal to.
func (*p) Eq(args ...interface{}) Predicate {
	return newP("eq", args...)
}

// Gt Predicate to determine if greater than.
func (*p) Gt(args ...interface{}) Predicate {
	return newP("gt", args...)
}

// Gte Predicate to determine if greater than or equal to.
func (*p) Gte(args ...interface{}) Predicate {
	return newP("gte", args...)
}

// Inside Predicate to determine if value is within range of specified values (exclusive).
func (*p) Inside(args ...interface{}) Predicate {
	return newP("inside", args...)
}

// Lt Predicate to determine if less than.
func (*p) Lt(args ...interface{}) Predicate {
	return newP("lt", args...)
}

// Lte Predicate to determine if less than or equal to.
func (*p) Lte(args ...interface{}) Predicate {
	return newP("lte", args...)
}

// Neq Predicate to determine if not equal to.
func (*p) Neq(args ...interface{}) Predicate {
	return newP("neq", args...)
}

// Not Predicate gives the opposite of the specified Predicate.
func (*p) Not(args ...interface{}) Predicate {
	return newP("not", args...)
}

// Outside Predicate to determine of value is not within range of specified values (exclusive).
func (*p) Outside(args ...interface{}) Predicate {
	return newP("outside", args...)
}

// Test evaluates Predicate on given argument.
func (*p) Test(args ...interface{}) Predicate {
	return newP("test", args...)
}

// Within Predicate determines if value is within given list of values.
func (*p) Within(args ...interface{}) Predicate {
	return newP("within", args...)
}

// Without Predicate determines if value is not within the specified.
func (*p) Without(args ...interface{}) Predicate {
	return newP("without", args...)
}

// And Predicate returns a Predicate composed of two predicates (logical AND of them).
func (pp *p) And(args ...interface{}) Predicate {
	return newPWithP("and", *pp, args...)
}

// Or Predicate returns a Predicate composed of two predicates (logical OR of them).
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
	values = append(values, args...)
	return &textP{operator: operator, values: values}
}

func newTextPWithP(operator string, tp textP, args ...interface{}) TextPredicate {
	values := make([]interface{}, 1)
	values[0] = tp
	values = append(values, args...)
	return &textP{operator: operator, values: values}
}

// Containing TextPredicate determines if a string contains a given value.
func (*textP) Containing(args ...interface{}) TextPredicate {
	return newTextP("containing", args...)
}

// EndingWith TextPredicate determines if a string ends with a given value.
func (*textP) EndingWith(args ...interface{}) TextPredicate {
	return newTextP("endingWith", args...)
}

// NotContaining TextPredicate determines if a string does not contain a given value.
func (*textP) NotContaining(args ...interface{}) TextPredicate {
	return newTextP("notContaining", args...)
}

// NotEndingWith TextPredicate determines if a string does not end with a given value.
func (*textP) NotEndingWith(args ...interface{}) TextPredicate {
	return newTextP("notEndingWith", args...)
}

// NotStartingWith TextPredicate determines if a string does not start with a given value.
func (*textP) NotStartingWith(args ...interface{}) TextPredicate {
	return newTextP("notStartingWith", args...)
}

// StartingWith TextPredicate determines if a string starts with a given value.
func (*textP) StartingWith(args ...interface{}) TextPredicate {
	return newTextP("startingWith", args...)
}

// And TextPredicate returns a TextPredicate composed of two predicates (logical AND of them).
func (tp *textP) And(args ...interface{}) TextPredicate {
	return newTextPWithP("and", *tp, args...)
}

// Or TextPredicate returns a TextPredicate composed of two predicates (logical OR of them).
func (tp *textP) Or(args ...interface{}) TextPredicate {
	return newTextPWithP("or", *tp, args...)
}

type withOptions struct {
	Tokens  string
	None    int32
	Ids     int32
	Labels  int32
	Keys    int32
	Values  int32
	All     int32
	Indexer string
	List    int32
	Map     int32
}

var WithOptions withOptions = withOptions{
	Tokens:  "~tinkerpop.valueMap.tokens",
	None:    0,
	Ids:     1,
	Labels:  2,
	Keys:    4,
	Values:  8,
	All:     1 | 2 | 4 | 8,
	Indexer: "~tinkerpop.index.indexer",
	List:    0,
	Map:     1,
}
