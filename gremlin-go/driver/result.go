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
	"fmt"
	"reflect"
	"strconv"
)

// Result Struct to abstract the Result and provide functions to use it.
type Result struct {
	Result interface{}
}

// String returns the string representation of the Result struct in Go-syntax format.
func (r *Result) String() string {
	return fmt.Sprintf("result{object=%v class=%T}", r.Result, r.Result)
}

// GetString gets the string representation of the result.
func (r *Result) GetString() string {
	return fmt.Sprintf("%v", r.Result)
}

// GetInt gets the result by coercing it into an int, else returns an error if not parsable.
func (r *Result) GetInt() (int, error) {
	return strconv.Atoi(r.GetString())
}

// GetByte gets the result by coercing it into a byte (uint8), else returns an error if not parsable.
func (r *Result) GetByte() (byte, error) {
	res, err := strconv.ParseUint(r.GetString(), 10, 8)
	if err != nil {
		return 0, err
	}
	return byte(res), nil
}

// GetUint gets the result by coercing it into an int, else returns an error if not parsable.
func (r *Result) GetUint() (uint, error) {
	res, err := strconv.ParseUint(r.GetString(), 10, 64)
	if err != nil {
		return 0, err
	}
	return uint(res), nil
}

// GetUint16 gets the result by coercing it into an int16, else returns an error if not parsable.
func (r *Result) GetUint16() (uint16, error) {
	res, err := strconv.ParseUint(r.GetString(), 10, 16)
	if err != nil {
		return 0, err
	}
	return uint16(res), nil
}

// GetUint32 gets the result by coercing it into a rune(int32), else returns an error if not parsable.
func (r *Result) GetUint32() (uint32, error) {
	res, err := strconv.ParseUint(r.GetString(), 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(res), nil
}

// GetUint64 gets the result by coercing it into an int64, else returns an error if not parsable.
func (r *Result) GetUint64() (uint64, error) {
	return strconv.ParseUint(r.GetString(), 10, 64)
}

// GetInt8 gets the result by coercing it into an int16, else returns an error if not parsable.
func (r *Result) GetInt8() (int8, error) {
	res, err := strconv.ParseInt(r.GetString(), 10, 8)
	if err != nil {
		return 0, err
	}
	return int8(res), nil
}

// GetInt16 gets the result by coercing it into an int16, else returns an error if not parsable.
func (r *Result) GetInt16() (int16, error) {
	res, err := strconv.ParseInt(r.GetString(), 10, 16)
	if err != nil {
		return 0, err
	}
	return int16(res), nil
}

// GetInt32 gets the result by coercing it into a rune(int32), else returns an error if not parsable.
func (r *Result) GetInt32() (int32, error) {
	res, err := strconv.ParseInt(r.GetString(), 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(res), nil
}

// GetInt64 gets the result by coercing it into an int64, else returns an error if not parsable.
func (r *Result) GetInt64() (int64, error) {
	return strconv.ParseInt(r.GetString(), 10, 64)
}

// GetFloat32 gets the result by coercing it into a float32, else returns an error if not parsable.
func (r *Result) GetFloat32() (float32, error) {
	res, err := strconv.ParseFloat(r.GetString(), 32)
	if err != nil {
		return 0, err
	}
	return float32(res), nil
}

// GetFloat64 gets the result by coercing it into a float64, else returns an error if not parsable.
func (r *Result) GetFloat64() (float64, error) {
	return strconv.ParseFloat(r.GetString(), 64)
}

// GetBool gets the result by coercing it into a boolean, else returns an error if not parsable.
func (r *Result) GetBool() (bool, error) {
	return strconv.ParseBool(r.GetString())
}

// IsNil checks if the result is null.
func (r *Result) IsNil() bool {
	return nil == r.Result
}

// GetVertex returns the result if it is a Vertex, otherwise returns an error.
func (r *Result) GetVertex() (*Vertex, error) {
	res, ok := r.Result.(*Vertex)
	if !ok {
		return nil, newError(err0601ResultNotVertexError)
	}
	return res, nil
}

// GetEdge returns the result if it is an edge, otherwise returns an error.
func (r *Result) GetEdge() (*Edge, error) {
	res, ok := r.Result.(*Edge)
	if !ok {
		return nil, newError(err0602ResultNotEdgeError)
	}
	return res, nil
}

// GetElement returns the result if it is an Element, otherwise returns an error.
func (r *Result) GetElement() (*Element, error) {
	res, ok := r.Result.(*Element)
	if !ok {
		return nil, newError(err0603ResultNotElementError)
	}
	return res, nil
}

// GetPath returns the result if it is a path, otherwise returns an error.
func (r *Result) GetPath() (*Path, error) {
	res, ok := r.Result.(*Path)
	if !ok {
		return nil, newError(err0604ResultNotPathError)
	}
	return res, nil
}

// GetProperty returns the result if it is a property, otherwise returns an error.
func (r *Result) GetProperty() (*Property, error) {
	res, ok := r.Result.(*Property)
	if !ok {
		return nil, newError(err0605ResultNotPropertyError)
	}
	return res, nil
}

// GetVertexProperty returns the result if it is a Vertex property, otherwise returns an error.
func (r *Result) GetVertexProperty() (*VertexProperty, error) {
	res, ok := r.Result.(*VertexProperty)
	if !ok {
		return nil, newError(err0606ResultNotVertexPropertyError)
	}
	return res, nil
}

// GetTraverser returns the Result if it is a Traverser, otherwise returns an error.
func (r *Result) GetTraverser() (*Traverser, error) {
	res, ok := r.Result.(Traverser)
	if !ok {
		return nil, newError(err0607ResultNotTraverserError)
	}
	return &res, nil
}

// GetSlice returns the Result if it is a Slice, otherwise returns an error.
func (r *Result) GetSlice() (*[]interface{}, error) {
	res, ok := r.Result.([]interface{})
	if !ok {
		return nil, newError(err0608ResultNotSliceError)
	}
	return &res, nil
}

// GetType returns the type of the result.
func (r *Result) GetType() reflect.Type {
	return reflect.TypeOf(r.Result)
}

// GetInterface returns the result item.
func (r *Result) GetInterface() interface{} {
	return r.Result
}
