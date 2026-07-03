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
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestGValue(t *testing.T) {

	t.Run("test simple gValue", func(t *testing.T) {
		gVal := GValue{Name: "intVal", Value: 2}
		assert.Equal(t, "intVal", gVal.Name)
		assert.Equal(t, 2, gVal.Value)
		assert.False(t, gVal.IsNil())
	})

	t.Run("test gValue allow parameter reuse with arrays", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		val := [3]int{1, 2, 3}
		param := GValue{Name: "ids", Value: val}
		gl := g.Inject(param).V(param).GremlinLang
		assert.Equal(t, "g.inject(ids).V(ids)", gl.GetGremlin())
		assert.Equal(t, val, gl.parameters["ids"])
	})

	t.Run("test gValue allow parameter reuse with slices", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		val := []int{1, 2, 3}
		param := GValue{Name: "ids", Value: val}
		gl := g.Inject(param).V(param).GremlinLang
		assert.Equal(t, "g.inject(ids).V(ids)", gl.GetGremlin())
		assert.Equal(t, val, gl.parameters["ids"])
	})

	t.Run("test gValue allow parameter reuse with maps", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		val := map[string]int{"foo": 1, "bar": 2}
		param := GValue{Name: "ids", Value: val}
		gl := g.Inject(param).V(param).GremlinLang
		assert.Equal(t, "g.inject(ids).V(ids)", gl.GetGremlin())
		assert.Equal(t, val, gl.parameters["ids"])
	})

	t.Run("test gValue name not duplicated", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		param1 := GValue{Name: "ids", Value: [2]int{1, 2}}
		param2 := GValue{Name: "ids", Value: [2]int{2, 3}}
		assert.Panics(t, func() { g.Inject(param1).V(param2) }, "parameter with name ids already exists.")
	})

	t.Run("test IsNil returns true for nil value", func(t *testing.T) {
		gv := GValue{Name: "x", Value: nil}
		assert.True(t, gv.IsNil())
	})

	t.Run("test String representation", func(t *testing.T) {
		gv := GValue{Name: "x", Value: 1}
		assert.Equal(t, "x=1", gv.String())
	})

	t.Run("test distinct but equal slices allowed under same name", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		param1 := GValue{Name: "ids", Value: []int{1, 2, 3}}
		param2 := GValue{Name: "ids", Value: []int{1, 2, 3}}
		assert.NotPanics(t, func() { g.Inject(param1).V(param2) })
	})

	t.Run("test gValue nested in child traversal merges bindings", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		gl := g.V().Where(T__.Is(GValue{Name: "xx1", Value: 1})).GremlinLang
		assert.Equal(t, "g.V().where(__.is(xx1))", gl.GetGremlin())
		assert.Equal(t, 1, gl.parameters["xx1"])
	})

	t.Run("test gValue nested across multiple child traversals merges bindings", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		gl := g.V().Union(T__.V(GValue{Name: "vid1", Value: 1}), T__.V(GValue{Name: "vid4", Value: 4})).GremlinLang
		assert.Equal(t, "g.V().union(__.V(vid1),__.V(vid4))", gl.GetGremlin())
		assert.Equal(t, 1, gl.parameters["vid1"])
		assert.Equal(t, 4, gl.parameters["vid4"])
	})

	t.Run("test gValue underscore name works", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		gl := g.V(GValue{Name: "_1", Value: []int{1, 2, 3}}).GremlinLang
		assert.Equal(t, "g.V(_1)", gl.GetGremlin())
	})

	t.Run("test gValue dollar sign name works", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		gl := g.V(GValue{Name: "a$b", Value: 1}).GremlinLang
		assert.Equal(t, "g.V(a$b)", gl.GetGremlin())
	})

	t.Run("test gValue invalid identifier panics", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		assert.Panics(t, func() { g.V(GValue{Name: "1a", Value: 1}) })
	})
}
