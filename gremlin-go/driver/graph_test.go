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
		v := Vertex{Element: Element{uid, "Vertex-Label", nil}}
		assert.Equal(t, fmt.Sprintf("v[%s]", uid.String()), v.String())
	})

	t.Run("Test Edge.String()", func(t *testing.T) {
		uidEdge, _ := uuid.NewUUID()
		uidIn, _ := uuid.NewUUID()
		uidOut, _ := uuid.NewUUID()
		v := Edge{Element: Element{uidEdge, "edge_label", nil}, OutV: Vertex{Element: Element{uidOut, "vertex_out", nil}}, InV: Vertex{Element: Element{uidIn, "vertex_in", nil}}}
		assert.Equal(t, fmt.Sprintf("e[%s][%s-edge_label->%s]", uidEdge.String(), uidOut.String(), uidIn.String()), v.String())
	})

	t.Run("Test VertexProperty.String()", func(t *testing.T) {
		uidVProp, _ := uuid.NewUUID()
		uidV, _ := uuid.NewUUID()
		v := VertexProperty{Element{uidVProp, "Vertex-prop", nil}, "Vertex", []uint32{0, 1}, Vertex{Element: Element{uidV, "Vertex", nil}}}
		assert.Equal(t, "vp[Vertex-prop->[0 1]]", v.String())
	})

	t.Run("Test Property.String()", func(t *testing.T) {
		uidElement, _ := uuid.NewUUID()
		data := []uint32{0, 1}
		p := Property{"property-Key", data, Element{uidElement, "prop", nil}}
		assert.Equal(t, "p[property-Key->[0 1]]", p.String())
	})

	t.Run("Test Graph.String() empty graph", func(t *testing.T) {
		g := NewGraph()
		assert.Equal(t, "graph[vertices:0 edges:0]", g.String())
	})

	t.Run("Test Graph.String() with vertices and edges", func(t *testing.T) {
		g := NewGraph()
		v1 := &Vertex{Element: Element{Id: int32(1), Label: "person"}}
		v2 := &Vertex{Element: Element{Id: int32(2), Label: "person"}}
		g.Vertices[v1.Id] = v1
		g.Vertices[v2.Id] = v2
		g.Edges[int32(3)] = &Edge{
			Element: Element{Id: int32(3), Label: "knows"},
			InV:     *v2, OutV: *v1,
		}
		assert.Equal(t, "graph[vertices:2 edges:1]", g.String())
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

func TestVertexPropertyMap(t *testing.T) {
	t.Run("Test Vertex.PropertyMap() groups multi-valued properties by key", func(t *testing.T) {
		v := &Vertex{Element: Element{1, "person", []interface{}{
			&VertexProperty{Element{10, "name", nil}, "name", "marko", Vertex{}},
			&VertexProperty{Element{11, "name", nil}, "name", "marko-a-polo", Vertex{}},
			&VertexProperty{Element{12, "age", nil}, "age", 29, Vertex{}},
		}}}
		propertyMap := v.PropertyMap()
		assert.Equal(t, 2, len(propertyMap))
		assert.Equal(t, 2, len(propertyMap["name"]))
		assert.Equal(t, "marko", propertyMap["name"][0].Value)
		assert.Equal(t, "marko-a-polo", propertyMap["name"][1].Value)
		assert.Equal(t, 1, len(propertyMap["age"]))
		assert.Equal(t, 29, propertyMap["age"][0].Value)
	})

	t.Run("Test Vertex.PropertyMap() single-valued property returns a one-element slice", func(t *testing.T) {
		v := &Vertex{Element: Element{1, "person", []interface{}{
			&VertexProperty{Element{10, "name", nil}, "name", "marko", Vertex{}},
		}}}
		propertyMap := v.PropertyMap()
		assert.Equal(t, 1, len(propertyMap))
		assert.Equal(t, 1, len(propertyMap["name"]))
		assert.Equal(t, "marko", propertyMap["name"][0].Value)
	})

	t.Run("Test Vertex.PropertyMap() with nil Properties returns empty map", func(t *testing.T) {
		v := &Vertex{Element: Element{1, "person", nil}}
		propertyMap := v.PropertyMap()
		assert.NotNil(t, propertyMap)
		assert.Equal(t, 0, len(propertyMap))
	})

	t.Run("Test Vertex.PropertyMap() with empty Properties slice returns empty map", func(t *testing.T) {
		v := &Vertex{Element: Element{1, "person", []interface{}{}}}
		propertyMap := v.PropertyMap()
		assert.NotNil(t, propertyMap)
		assert.Equal(t, 0, len(propertyMap))
	})
}

func TestEdgePropertyMap(t *testing.T) {
	t.Run("Test Edge.PropertyMap() groups multi-valued properties by key", func(t *testing.T) {
		e := &Edge{Element: Element{1, "created", []interface{}{
			&Property{"weight", 0.4, Element{}},
			&Property{"weight", 0.6, Element{}},
			&Property{"since", 2010, Element{}},
		}}, OutV: Vertex{}, InV: Vertex{}}
		propertyMap := e.PropertyMap()
		assert.Equal(t, 2, len(propertyMap))
		assert.Equal(t, 2, len(propertyMap["weight"]))
		assert.Equal(t, 0.4, propertyMap["weight"][0].Value)
		assert.Equal(t, 0.6, propertyMap["weight"][1].Value)
		assert.Equal(t, 1, len(propertyMap["since"]))
		assert.Equal(t, 2010, propertyMap["since"][0].Value)
	})

	t.Run("Test Edge.PropertyMap() single-valued property returns a one-element slice", func(t *testing.T) {
		e := &Edge{Element: Element{1, "created", []interface{}{
			&Property{"weight", 0.4, Element{}},
		}}, OutV: Vertex{}, InV: Vertex{}}
		propertyMap := e.PropertyMap()
		assert.Equal(t, 1, len(propertyMap))
		assert.Equal(t, 1, len(propertyMap["weight"]))
		assert.Equal(t, 0.4, propertyMap["weight"][0].Value)
	})

	t.Run("Test Edge.PropertyMap() with nil Properties returns empty map", func(t *testing.T) {
		e := &Edge{Element: Element{1, "created", nil}, OutV: Vertex{}, InV: Vertex{}}
		propertyMap := e.PropertyMap()
		assert.NotNil(t, propertyMap)
		assert.Equal(t, 0, len(propertyMap))
	})

	t.Run("Test Edge.PropertyMap() with empty Properties slice returns empty map", func(t *testing.T) {
		e := &Edge{Element: Element{1, "created", []interface{}{}}, OutV: Vertex{}, InV: Vertex{}}
		propertyMap := e.PropertyMap()
		assert.NotNil(t, propertyMap)
		assert.Equal(t, 0, len(propertyMap))
	})
}

func TestVertexPropertyMetaPropertyMap(t *testing.T) {
	t.Run("Test VertexProperty.PropertyMap() groups multi-valued properties by key", func(t *testing.T) {
		vp := &VertexProperty{Element{1, "name", []interface{}{
			&Property{"acl", "public", Element{}},
			&Property{"acl", "private", Element{}},
			&Property{"startTime", 2010, Element{}},
		}}, "name", "marko", Vertex{}}
		propertyMap := vp.PropertyMap()
		assert.Equal(t, 2, len(propertyMap))
		assert.Equal(t, 2, len(propertyMap["acl"]))
		assert.Equal(t, "public", propertyMap["acl"][0].Value)
		assert.Equal(t, "private", propertyMap["acl"][1].Value)
		assert.Equal(t, 1, len(propertyMap["startTime"]))
		assert.Equal(t, 2010, propertyMap["startTime"][0].Value)
	})

	t.Run("Test VertexProperty.PropertyMap() single-valued property returns a one-element slice", func(t *testing.T) {
		vp := &VertexProperty{Element{1, "name", []interface{}{
			&Property{"acl", "public", Element{}},
		}}, "name", "marko", Vertex{}}
		propertyMap := vp.PropertyMap()
		assert.Equal(t, 1, len(propertyMap))
		assert.Equal(t, 1, len(propertyMap["acl"]))
		assert.Equal(t, "public", propertyMap["acl"][0].Value)
	})

	t.Run("Test VertexProperty.PropertyMap() with nil Properties returns empty map", func(t *testing.T) {
		vp := &VertexProperty{Element{1, "name", nil}, "name", "marko", Vertex{}}
		propertyMap := vp.PropertyMap()
		assert.NotNil(t, propertyMap)
		assert.Equal(t, 0, len(propertyMap))
	})

	t.Run("Test VertexProperty.PropertyMap() with empty Properties slice returns empty map", func(t *testing.T) {
		vp := &VertexProperty{Element{1, "name", []interface{}{}}, "name", "marko", Vertex{}}
		propertyMap := vp.PropertyMap()
		assert.NotNil(t, propertyMap)
		assert.Equal(t, 0, len(propertyMap))
	})
}
