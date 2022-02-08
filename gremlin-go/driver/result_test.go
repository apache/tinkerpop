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
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResult(t *testing.T) {

	t.Run("Test Result.AsString() string", func(t *testing.T) {
		r := Result{"foo"}
		assert.Equal(t, "foo", r.AsString())
	})

	t.Run("Test Result.AsString() slice", func(t *testing.T) {
		r := Result{[]int{1, 2, 3}}
		assert.Equal(t, "[1 2 3]", r.AsString())
	})

	t.Run("Test Result.AsString() int", func(t *testing.T) {
		r := Result{1}
		assert.Equal(t, "1", r.AsString())
	})

	t.Run("Test Result.AsString() float", func(t *testing.T) {
		r := Result{1.2}
		assert.Equal(t, "1.2", r.AsString())
	})

	t.Run("Test Result.GetString()", func(t *testing.T) {
		r := Result{1.2}
		assert.Equal(t, "1.2", r.GetString())
	})

	t.Run("Test Result.GetInt()", func(t *testing.T) {
		r := Result{100}
		res, err := r.GetInt()
		assert.Nil(t, err)
		assert.Equal(t, 100, res)
	})

	t.Run("Test Result.GetInt() error expected", func(t *testing.T) {
		r := Result{"not int"}
		res, err := r.GetInt()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetByte()", func(t *testing.T) {
		r := Result{100}
		res, err := r.GetByte()
		assert.Nil(t, err)
		assert.Equal(t, byte(100), res)
	})

	t.Run("Test Result.GetByte() error expected", func(t *testing.T) {
		r := Result{"not byte"}
		res, err := r.GetByte()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetShort()", func(t *testing.T) {
		r := Result{100}
		res, err := r.GetShort()
		assert.Nil(t, err)
		assert.Equal(t, int16(100), res)
	})

	t.Run("Test Result.GetShort() error expected", func(t *testing.T) {
		r := Result{"not short"}
		res, err := r.GetShort()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetLong()", func(t *testing.T) {
		r := Result{100}
		res, err := r.GetLong()
		assert.Nil(t, err)
		assert.Equal(t, int64(100), res)
	})

	t.Run("Test Result.GetLong() error expected", func(t *testing.T) {
		r := Result{"not long"}
		res, err := r.GetLong()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetFloat()", func(t *testing.T) {
		r := Result{100}
		res, err := r.GetFloat()
		assert.Nil(t, err)
		assert.Equal(t, float32(100), res)
	})

	t.Run("Test Result.GetFloat() error expected", func(t *testing.T) {
		r := Result{"not float"}
		res, err := r.GetFloat()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetDouble()", func(t *testing.T) {
		r := Result{100}
		res, err := r.GetDouble()
		assert.Nil(t, err)
		assert.Equal(t, float64(100), res)
	})

	t.Run("Test Result.GetDouble() error expected", func(t *testing.T) {
		r := Result{"not double"}
		res, err := r.GetDouble()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetBoolean() number representation", func(t *testing.T) {
		r := Result{1}
		res, err := r.GetBoolean()
		assert.Nil(t, err)
		assert.Equal(t, true, res)
	})

	t.Run("Test Result.GetBoolean() bool representation", func(t *testing.T) {
		r := Result{false}
		res, err := r.GetBoolean()
		assert.Nil(t, err)
		assert.Equal(t, false, res)
	})

	t.Run("Test Result.GetBoolean() error expected", func(t *testing.T) {
		r := Result{"not boolean"}
		res, err := r.GetBoolean()
		assert.Error(t, err)
		assert.Zero(t, res)
	})

	t.Run("Test Result.GetVertex()", func(t *testing.T) {
		vertex := Vertex{}
		r := Result{vertex}
		res, err := r.GetVertex()
		assert.Nil(t, err)
		assert.Equal(t, &vertex, res)
	})

	t.Run("Test Result.GetVertex() error expected", func(t *testing.T) {
		r := Result{"not vertex"}
		res, err := r.GetVertex()
		assert.Nil(t, res)
		assert.Error(t, err, "result is not a Vertex")
	})

	t.Run("Test Result.GetEdge()", func(t *testing.T) {
		edge := Edge{}
		r := Result{edge}
		res, err := r.GetEdge()
		assert.Nil(t, err)
		assert.Equal(t, &edge, res)
	})

	t.Run("Test Result.GetEdge() error expected", func(t *testing.T) {
		r := Result{"not edge"}
		res, err := r.GetEdge()
		assert.Nil(t, res)
		assert.Error(t, err, "result is not an Edge")
	})

	t.Run("Test Result.GetElement()", func(t *testing.T) {
		element := Element{}
		r := Result{element}
		res, err := r.GetElement()
		assert.Nil(t, err)
		assert.Equal(t, &element, res)
	})

	t.Run("Test Result.GetElement() error expected", func(t *testing.T) {
		r := Result{"not element"}
		res, err := r.GetElement()
		assert.Nil(t, res)
		assert.Error(t, err, "result is not an Element")
	})

	t.Run("Test Result.GetPath()", func(t *testing.T) {
		path := Path{}
		r := Result{path}
		res, err := r.GetPath()
		assert.Nil(t, err)
		assert.Equal(t, &path, res)
	})

	t.Run("Test Result.GetPath() error expected", func(t *testing.T) {
		r := Result{"not path"}
		res, err := r.GetPath()
		assert.Nil(t, res)
		assert.Error(t, err, "result is not a Path")
	})

	t.Run("Test Result.GetProperty()", func(t *testing.T) {
		property := Property{}
		r := Result{property}
		res, err := r.GetProperty()
		assert.Nil(t, err)
		assert.Equal(t, &property, res)
	})

	t.Run("Test Result.GetProperty() error expected", func(t *testing.T) {
		r := Result{"not property"}
		res, err := r.GetProperty()
		assert.Nil(t, res)
		assert.Error(t, err, "result is not a Property")
	})

	t.Run("Test Result.GetVertexProperty()", func(t *testing.T) {
		vertexProperty := VertexProperty{}
		r := Result{vertexProperty}
		res, err := r.GetVertexProperty()
		assert.Nil(t, err)
		assert.Equal(t, &vertexProperty, res)
	})

	t.Run("Test Result.GetVertexProperty() error expected", func(t *testing.T) {
		r := Result{"not vertex property"}
		res, err := r.GetVertexProperty()
		assert.Nil(t, res)
		assert.Error(t, err, "result is not a VertexProperty")
	})

	t.Run("Test Result.ToString()", func(t *testing.T) {
		r := Result{[]int{1, 2, 3}}
		res := r.ToString()
		assert.Equal(t, "result{object=[1 2 3] class=[]int}", res)
	})

	t.Run("Test Result.ToString() null", func(t *testing.T) {
		r := Result{nil}
		res := r.ToString()
		assert.Equal(t, "result{object=<nil> class=<nil>}", res)
	})

	t.Run("Test Result.ToString() empty custom struct", func(t *testing.T) {
		element := Element{}
		r := Result{element}
		res := r.ToString()
		assert.Equal(t, "result{object={[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0] } class=gremlingo.Element}", res)
	})

	t.Run("Test Result.GetType() simple type", func(t *testing.T) {
		str := "result string"
		r := Result{str}
		res := r.GetType()
		assert.Equal(t, reflect.TypeOf(str), res)
	})

	t.Run("Test Result.GetType() custom defined type", func(t *testing.T) {
		element := Element{}
		r := Result{element}
		res := r.GetType()
		assert.Equal(t, reflect.TypeOf(element), res)
	})

	t.Run("Test Result.GetInterface()", func(t *testing.T) {
		element := Element{}
		r := Result{element}
		res := r.GetInterface()
		assert.Equal(t, element, res)
	})
}
