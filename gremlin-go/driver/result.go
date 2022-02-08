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

// AN-968 Finish Result implementation.

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// Result Struct to abstract the Result and provide functions to use it.
type Result struct {
	result interface{}
}

func newResult(result interface{}) *Result {
	return &Result{result}
}

// AsString returns the current result item formatted as a string.
func (r *Result) AsString() string {
	return fmt.Sprintf("%v", r.result)
}

// ToString returns the string representation of the Result struct in Go-syntax format
func (r *Result) ToString() string {
	return fmt.Sprintf("result{object=%v class=%T}", r.result, r.result)
}

// GetString gets the string representation of the result, equivalent to AsString()
func (r *Result) GetString() string {
	return r.AsString()
}

// GetInt gets the result by coercing it into an int, else returns an error if not parsable
func (r *Result) GetInt() (int, error) {
	return strconv.Atoi(r.AsString())
}

// GetByte gets the result by coercing it into a byte, else returns an error if not parsable
func (r *Result) GetByte() (byte, error) {
	res, err := strconv.ParseUint(r.AsString(), 10, 8)
	if err != nil {
		return 0, err
	}
	return byte(res), nil
}

// GetShort gets the result by coercing it into an int16, else returns an error if not parsable
func (r *Result) GetShort() (int16, error) {
	res, err := strconv.ParseInt(r.AsString(), 10, 16)
	if err != nil {
		return 0, err
	}
	return int16(res), nil
}

// GetLong gets the result by coercing it into an int64, else returns an error if not parsable
func (r *Result) GetLong() (int64, error) {
	return strconv.ParseInt(r.AsString(), 10, 64)
}

// GetFloat gets the result by coercing it into a float32, else returns an error if not parsable
func (r *Result) GetFloat() (float32, error) {
	res, err := strconv.ParseFloat(r.AsString(), 32)
	if err != nil {
		return 0, err
	}
	return float32(res), nil
}

// GetDouble gets the result by coercing it into a float64, else returns an error if not parsable
func (r *Result) GetDouble() (float64, error) {
	return strconv.ParseFloat(r.AsString(), 64)
}

// GetBoolean gets the result by coercing it into a boolean, else returns an error if not parsable
func (r *Result) GetBoolean() (bool, error) {
	return strconv.ParseBool(r.AsString())
}

// IsNil checks if the result is null
func (r *Result) IsNil() bool {
	return nil == r.result
}

// GetVertex returns the result if it is a vertex, otherwise returns an error
func (r *Result) GetVertex() (*Vertex, error) {
	res, ok := r.result.(Vertex)
	if !ok {
		return nil, errors.New("result is not a Vertex")
	}
	return &res, nil
}

// GetEdge returns the result if it is an edge, otherwise returns an error
func (r *Result) GetEdge() (*Edge, error) {
	res, ok := r.result.(Edge)
	if !ok {
		return nil, errors.New("result is not an Edge")
	}
	return &res, nil
}

// GetElement returns the result if it is an element, otherwise returns an error
func (r *Result) GetElement() (*Element, error) {
	res, ok := r.result.(Element)
	if !ok {
		return nil, errors.New("result is not an Element")
	}
	return &res, nil
}

// GetPath returns the result if it is a path, otherwise returns an error
func (r *Result) GetPath() (*Path, error) {
	res, ok := r.result.(Path)
	if !ok {
		return nil, errors.New("result is not a Path")
	}
	return &res, nil
}

// GetProperty returns the result if it is a property, otherwise returns an error
func (r *Result) GetProperty() (*Property, error) {
	res, ok := r.result.(Property)
	if !ok {
		return nil, errors.New("result is not a Property")
	}
	return &res, nil
}

// GetVertexProperty returns the result if it is a vertex property, otherwise returns an error
func (r *Result) GetVertexProperty() (*VertexProperty, error) {
	res, ok := r.result.(VertexProperty)
	if !ok {
		return nil, errors.New("result is not a VertexProperty")
	}
	return &res, nil
}

// GetType returns the type of the result
func (r *Result) GetType() reflect.Type {
	return reflect.TypeOf(r.result)
}

// GetInterface returns the result item
func (r *Result) GetInterface() interface{} {
	return r.result
}
