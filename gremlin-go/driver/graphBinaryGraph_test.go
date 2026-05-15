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
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

// TestGraphSerializerRoundTrip verifies GraphBinary 4.0 Graph (0x10)
// serialization/deserialization round-trip preserves vertices, edges,
// vertex-properties and properties.
func TestGraphSerializerRoundTrip(t *testing.T) {
	t.Run("preserves vertices, edges, vertex-properties and properties", func(t *testing.T) {
		graph := NewGraph()

		v1 := &Vertex{Element: Element{Id: int32(1), Label: "person"}}
		v2 := &Vertex{Element: Element{Id: int32(2), Label: "person"}}

		// VertexProperty on v1 with a meta-property
		vp1 := &VertexProperty{
			Element: Element{Id: int32(4), Label: "name"},
			Key:     "name",
			Value:   "marko",
			Vertex:  *v1,
		}
		vp1.Properties = []interface{}{&Property{Key: "acl", Value: "public"}}
		v1.Properties = []interface{}{vp1}
		v2.Properties = []interface{}{}

		graph.Vertices[v1.Id] = v1
		graph.Vertices[v2.Id] = v2

		// Edge v1 -knows-> v2 with weight property
		e1 := &Edge{
			Element: Element{Id: int32(3), Label: "knows"},
			InV:     *v2,
			OutV:    *v1,
		}
		e1.Properties = []interface{}{&Property{Key: "weight", Value: 0.5}}
		graph.Edges[e1.Id] = e1

		var buffer bytes.Buffer
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}

		// Round-trip via fully-qualified write/read.
		assert.Nil(t, serializer.write(graph, &buffer))

		d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
		out, err := d.ReadFullyQualified()
		assert.Nil(t, err)

		rg, ok := out.(*Graph)
		assert.True(t, ok, "expected *Graph result, got %T", out)
		assert.Equal(t, 2, len(rg.Vertices))
		assert.Equal(t, 1, len(rg.Edges))

		rv1 := rg.Vertices[int32(1)]
		assert.NotNil(t, rv1)
		assert.Equal(t, "person", rv1.Label)
		rv1Props, _ := rv1.Properties.([]interface{})
		assert.Equal(t, 1, len(rv1Props))
		rvp1, _ := rv1Props[0].(*VertexProperty)
		assert.NotNil(t, rvp1)
		assert.Equal(t, "name", rvp1.Label)
		assert.Equal(t, "marko", rvp1.Value)

		rvp1Meta, _ := rvp1.Properties.([]interface{})
		assert.Equal(t, 1, len(rvp1Meta))
		meta, _ := rvp1Meta[0].(*Property)
		assert.NotNil(t, meta)
		assert.Equal(t, "acl", meta.Key)
		assert.Equal(t, "public", meta.Value)

		rv2 := rg.Vertices[int32(2)]
		assert.NotNil(t, rv2)
		assert.Equal(t, "person", rv2.Label)

		re1 := rg.Edges[int32(3)]
		assert.NotNil(t, re1)
		assert.Equal(t, "knows", re1.Label)
		assert.Equal(t, int32(1), re1.OutV.Id)
		assert.Equal(t, int32(2), re1.InV.Id)

		re1Props, _ := re1.Properties.([]interface{})
		assert.Equal(t, 1, len(re1Props))
		w, _ := re1Props[0].(*Property)
		assert.NotNil(t, w)
		assert.Equal(t, "weight", w.Key)
		assert.Equal(t, 0.5, w.Value)
	})

	t.Run("handles empty graph", func(t *testing.T) {
		graph := NewGraph()

		var buffer bytes.Buffer
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
		assert.Nil(t, serializer.write(graph, &buffer))

		d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
		out, err := d.ReadFullyQualified()
		assert.Nil(t, err)

		rg, ok := out.(*Graph)
		assert.True(t, ok, "expected *Graph result, got %T", out)
		assert.Equal(t, 0, len(rg.Vertices))
		assert.Equal(t, 0, len(rg.Edges))
	})

	t.Run("handles vertices without properties and edges without properties", func(t *testing.T) {
		graph := NewGraph()

		v1 := &Vertex{Element: Element{Id: int32(10), Label: "person"}}
		v2 := &Vertex{Element: Element{Id: int32(20), Label: "software"}}
		v1.Properties = []interface{}{}
		v2.Properties = []interface{}{}
		graph.Vertices[v1.Id] = v1
		graph.Vertices[v2.Id] = v2

		e := &Edge{
			Element: Element{Id: int32(30), Label: "created"},
			InV:     *v2,
			OutV:    *v1,
		}
		e.Properties = []interface{}{}
		graph.Edges[e.Id] = e

		var buffer bytes.Buffer
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
		assert.Nil(t, serializer.write(graph, &buffer))

		d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
		out, err := d.ReadFullyQualified()
		assert.Nil(t, err)

		rg, _ := out.(*Graph)
		assert.Equal(t, 2, len(rg.Vertices))
		assert.Equal(t, 1, len(rg.Edges))

		re := rg.Edges[int32(30)]
		assert.Equal(t, "created", re.Label)
		assert.Equal(t, int32(10), re.OutV.Id)
		assert.Equal(t, int32(20), re.InV.Id)
	})

	t.Run("handles string ids", func(t *testing.T) {
		graph := NewGraph()

		v1 := &Vertex{Element: Element{Id: "a", Label: "person"}}
		v2 := &Vertex{Element: Element{Id: "b", Label: "person"}}
		v1.Properties = []interface{}{}
		v2.Properties = []interface{}{}
		graph.Vertices[v1.Id] = v1
		graph.Vertices[v2.Id] = v2

		e := &Edge{
			Element: Element{Id: "e1", Label: "knows"},
			InV:     *v2,
			OutV:    *v1,
		}
		e.Properties = []interface{}{}
		graph.Edges[e.Id] = e

		var buffer bytes.Buffer
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
		assert.Nil(t, serializer.write(graph, &buffer))

		d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
		out, err := d.ReadFullyQualified()
		assert.Nil(t, err)

		rg, _ := out.(*Graph)
		assert.Equal(t, 2, len(rg.Vertices))
		assert.Equal(t, 1, len(rg.Edges))

		re := rg.Edges["e1"]
		assert.NotNil(t, re)
		assert.Equal(t, "a", re.OutV.Id)
		assert.Equal(t, "b", re.InV.Id)
	})
}

// TestGraphString verifies the *Graph String() representation
// renders as graph[vertices:N edges:M].
func TestGraphString(t *testing.T) {
	t.Run("empty graph", func(t *testing.T) {
		g := NewGraph()
		assert.Equal(t, "graph[vertices:0 edges:0]", g.String())
	})

	t.Run("graph with vertices and edges", func(t *testing.T) {
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
}

// TestGraphSerializerRegistry verifies the graph dataType is wired up correctly.
func TestGraphSerializerRegistry(t *testing.T) {
	t.Run("getType returns graphType for *Graph", func(t *testing.T) {
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
		dt, err := serializer.getType(NewGraph())
		assert.Nil(t, err)
		assert.Equal(t, graphType, dt)
	})

	t.Run("getWriter returns graphWriter for graphType", func(t *testing.T) {
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
		_, err := serializer.getWriter(graphType)
		assert.Nil(t, err)
	})
}
