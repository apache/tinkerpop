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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestGraphStructureFunctions(t *testing.T) {
	t.Run("Test Vertex.String()", func(t *testing.T) {
		uid, _ := uuid.NewUUID()
		v := Vertex{Element{uid, "Vertex-Label", nil}}
		assert.Equal(t, fmt.Sprintf("v[%s]", uid.String()), v.String())
	})

	t.Run("Test Edge.String()", func(t *testing.T) {
		uidEdge, _ := uuid.NewUUID()
		uidIn, _ := uuid.NewUUID()
		uidOut, _ := uuid.NewUUID()
		v := Edge{Element{uidEdge, "edge_label", nil}, Vertex{Element{uidOut, "vertex_out", nil}}, Vertex{Element{uidIn, "vertex_in", nil}}}
		assert.Equal(t, fmt.Sprintf("e[%s][%s-edge_label->%s]", uidEdge.String(), uidOut.String(), uidIn.String()), v.String())
	})

	t.Run("Test VertexProperty.String()", func(t *testing.T) {
		uidVProp, _ := uuid.NewUUID()
		uidV, _ := uuid.NewUUID()
		v := VertexProperty{Element{uidVProp, "Vertex-prop", nil}, "Vertex", []uint32{0, 1}, Vertex{Element{uidV, "Vertex", nil}}}
		assert.Equal(t, "vp[Vertex-prop->[0 1]]", v.String())
	})

	t.Run("Test Property.String()", func(t *testing.T) {
		uidElement, _ := uuid.NewUUID()
		data := []uint32{0, 1}
		p := Property{"property-Key", data, Element{uidElement, "prop", nil}}
		assert.Equal(t, "p[property-Key->[0 1]]", p.String())
	})

	s1 := NewSimpleSet("foo")
	s2 := NewSimpleSet("bar")
	s3 := NewSimpleSet("baz")
	t.Run("Test Path.String()", func(t *testing.T) {
		keys := []Set{s1, s2, s3}
		data := []interface{}{1, 2, "hello"}
		p := Path{keys, data}
		assert.Equal(t, "path[1, 2, hello]", p.String())
	})

	t.Run("Test Path.GetPathObject() with valid Path", func(t *testing.T) {
		keys := []Set{s1, s2, s3}
		data := []interface{}{1, 2, "hello"}
		p := Path{keys, data}
		v, err := p.GetPathObject("foo")
		assert.Nil(t, err)
		assert.Equal(t, 1, v)
		v, err = p.GetPathObject("bar")
		assert.Nil(t, err)
		assert.Equal(t, 2, v)
		v, err = p.GetPathObject("baz")
		assert.Nil(t, err)
		assert.Equal(t, "hello", v)
	})

	t.Run("Test Path.GetPathObject() with invalid Path", func(t *testing.T) {
		keys := []Set{s1, s2, s3}
		data := []interface{}{1, 2}
		p := Path{keys, data}
		val, err := p.GetPathObject("foo")
		assert.Nil(t, val)
		assert.Equal(t, newError(err0301GetPathObjectInvalidPathUnequalLengthsError), err)
	})

	t.Run("Test Path.GetPathObject() with invalid key", func(t *testing.T) {
		keys := []Set{s1, s2}
		data := []interface{}{1, 2}
		p := Path{keys, data}
		val, err := p.GetPathObject("foobar")
		assert.Nil(t, val)
		assert.Equal(t, newError(err0303GetPathNoLabelFoundError, "foobar"), err)
	})

	t.Run("Test Path.GetPathObject() with Non-string value in labels", func(t *testing.T) {
		keys := []Set{s1, s2, NewSimpleSet(1)}
		data := []interface{}{1, 2, 3}
		p := Path{keys, data}
		val, err := p.GetPathObject("bar")
		assert.Nil(t, val)
		assert.Equal(t, newError(err0302GetPathObjectInvalidPathNonStringLabelError), err)
	})

	t.Run("Test Path.GetPathObject() with multiple object return", func(t *testing.T) {
		keys := []Set{s1, s2, NewSimpleSet("foo", "bar"), NewSimpleSet("bar")}
		data := []interface{}{1, 2, 3, 4}
		p := Path{keys, data}
		val, err := p.GetPathObject("foo")
		assert.Nil(t, err)
		assert.Equal(t, val, []interface{}{1, 3})
		val, err = p.GetPathObject("bar")
		assert.Nil(t, err)
		assert.Equal(t, val, []interface{}{2, 3, 4})
	})
}

func TestCustomStructs(t *testing.T) {

	t.Run("Test Set", func(t *testing.T) {
		t.Run("Test NewSimpleSet", func(t *testing.T) {
			set := NewSimpleSet("a", "b", "c", "a", 1, 1, 2, 2, 2, 3, 3, 3, 3)
			sliceAsSet := []interface{}{"a", "b", "c", 1, 2, 3}
			assert.Equal(t, sliceAsSet, set.objects)
		})

		t.Run("Test SimpleSet.Contains", func(t *testing.T) {
			set := NewSimpleSet("a", "b", "c", "a", 1, 1, 2, 2, 2, 3, 3, 3, 3)
			// True
			assert.True(t, set.Contains("a"))
			assert.True(t, set.Contains("b"))
			assert.True(t, set.Contains("c"))
			assert.True(t, set.Contains(1))
			assert.True(t, set.Contains(2))
			assert.True(t, set.Contains(3))
			// False
			assert.False(t, set.Contains("d"))
			assert.False(t, set.Contains(4))
		})

		t.Run("Test SimpleSet.Remove", func(t *testing.T) {
			set := NewSimpleSet("a", "b", "a", 1, 1, 2, 2, 2)
			assert.True(t, set.Contains("a"))
			assert.True(t, set.Contains("b"))
			assert.True(t, set.Contains(1))
			assert.True(t, set.Contains(2))
			set.Remove("a")
			set.Remove(2)
			assert.False(t, set.Contains("a"))
			assert.False(t, set.Contains(2))
			assert.True(t, set.Contains("b"))
			assert.True(t, set.Contains(1))
		})

		t.Run("Test SimpleSet.Add", func(t *testing.T) {
			set := NewSimpleSet()
			assert.False(t, set.Contains("a"))
			set.Add("a")
			assert.True(t, set.Contains("a"))
			set.Add("a")
			slice := set.ToSlice()
			assert.Equal(t, 1, len(slice))
			assert.Equal(t, slice[0], "a")
		})

		t.Run("Test SimpleSet.ToSlice", func(t *testing.T) {
			set := NewSimpleSet()
			assert.Equal(t, []interface{}(nil), set.ToSlice())
			set = NewSimpleSet("a", 1)
			assert.Equal(t, []interface{}{"a", 1}, set.ToSlice())
		})
	})
}
