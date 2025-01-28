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
		gVal := NewGValue("intVal", 2)
		assert.Equal(t, "intVal", gVal.Name())
		assert.Equal(t, 2, gVal.Value())
		assert.False(t, gVal.IsNil())
	})

	t.Run("test gValue allow parameter reuse with arrays", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		val := [3]int{1, 2, 3}
		param := NewGValue("ids", val)
		gl := g.Inject(param).V(param).GremlinLang
		assert.Equal(t, "g.inject(ids).V(ids)", gl.GetGremlin())
		assert.Equal(t, val, gl.parameters["ids"])
	})

	t.Run("test gValue allow parameter reuse with slices", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		val := []int{1, 2, 3}
		param := NewGValue("ids", val)
		gl := g.Inject(param).V(param).GremlinLang
		assert.Equal(t, "g.inject(ids).V(ids)", gl.GetGremlin())
		assert.Equal(t, val, gl.parameters["ids"])
	})

	t.Run("test gValue allow parameter reuse with maps", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		val := map[string]int{"foo": 1, "bar": 2}
		param := NewGValue("ids", val)
		gl := g.Inject(param).V(param).GremlinLang
		assert.Equal(t, "g.inject(ids).V(ids)", gl.GetGremlin())
		assert.Equal(t, val, gl.parameters["ids"])
	})

	t.Run("test gValue name not duplicated", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		param1 := NewGValue("ids", [2]int{1, 2})
		param2 := NewGValue("ids", [2]int{2, 3})
		assert.Panics(t, func() { g.Inject(param1).V(param2) }, "parameter with name ids already exists.")
	})

	t.Run("test invalid name that starts with _", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		assert.Panics(t, func() { g.Inject(NewGValue("_ids", [2]int{1, 2})) },
			"invalid GValue name _1. Should not start with _.")
	})

	t.Run("test name is valid identifier", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		assert.Panics(t, func() { g.Inject(NewGValue("1a", [2]int{1, 2})) },
			"invalid parameter name '1a'")
	})

	t.Run("test name is not a number", func(t *testing.T) {
		g := NewGraphTraversalSource(nil, nil)
		assert.Panics(t, func() { g.Inject(NewGValue("1", [2]int{1, 2})) },
			"invalid parameter name '1'")
	})
}
