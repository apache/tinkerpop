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
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGraphStructureFunctions(t *testing.T) {
	t.Run("Test Vertex.String()", func(t *testing.T) {
		uid, _ := uuid.NewUUID()
		v := Vertex{Element{uid, "vertex-label"}}
		assert.Equal(t, fmt.Sprintf("v[%s]", uid.String()), v.String())
	})

	t.Run("Test Edge.String()", func(t *testing.T) {
		uidEdge, _ := uuid.NewUUID()
		uidIn, _ := uuid.NewUUID()
		uidOut, _ := uuid.NewUUID()
		v := Edge{Element{uidEdge, "edge_label"}, Vertex{Element{uidOut, "vertex_out"}}, Vertex{Element{uidIn, "vertex_in"}}}
		assert.Equal(t, fmt.Sprintf("e[%s][%s-edge_label->%s]", uidEdge.String(), uidOut.String(), uidIn.String()), v.String())
	})

	t.Run("Test VertexProperty.String()", func(t *testing.T) {
		uidVProp, _ := uuid.NewUUID()
		uidV, _ := uuid.NewUUID()
		v := VertexProperty{Element{uidVProp, "vertex-prop"}, "vertex", []uint32{0, 1}, Vertex{Element{uidV, "vertex"}}}
		assert.Equal(t, "vp[vertex-prop->[0 1]]", v.String())
	})

	t.Run("Test Property.String()", func(t *testing.T) {
		uidElement, _ := uuid.NewUUID()
		data := []uint32{0, 1}
		p := Property{"property-key", data, Element{uidElement, "prop"}}
		assert.Equal(t, "p[property-key->[0 1]]", p.String())
	})

	t.Run("Test Path.String()", func(t *testing.T) {
		keys := []string{"foo", "bar", "baz"}
		data := []interface{}{1, 2, "hello"}
		p := Path{keys, data}
		assert.Equal(t, "path[1, 2, hello]", p.String())
	})

	t.Run("Test Path.GetPathObject() with valid Path", func(t *testing.T) {
		keys := []string{"foo", "bar", "baz"}
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
		keys := []string{"foo", "bar", "baz"}
		data := []interface{}{1, 2}
		p := Path{keys, data}
		val, err := p.GetPathObject("foo")
		assert.Nil(t, val)
		assert.NotNil(t, err)
	})

	t.Run("Test Path.GetPathObject() with invalid key", func(t *testing.T) {
		keys := []string{"foo", "bar", "baz"}
		data := []interface{}{1, 2}
		p := Path{keys, data}
		val, err := p.GetPathObject("foobar")
		assert.Nil(t, val)
		assert.NotNil(t, err)
	})
}
