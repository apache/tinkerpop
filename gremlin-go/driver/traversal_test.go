/*
Licensed to the Apache Software Foundation (ASF) Under one
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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTraversal(t *testing.T) {
	t.Run("Test clone traversal", func(t *testing.T) {
		g := NewGraphTraversalSource(&Graph{}, &TraversalStrategies{}, newBytecode(nil), nil)
		original := g.V().Out("created")
		clone := original.Clone().Out("knows")
		cloneClone := clone.Clone().Out("created")

		assert.Equal(t, 2, len(original.bytecode.stepInstructions))
		assert.Equal(t, 3, len(clone.bytecode.stepInstructions))
		assert.Equal(t, 4, len(cloneClone.bytecode.stepInstructions))

		original.Has("person", "name", "marko")
		clone.V().Out()

		assert.Equal(t, 3, len(original.bytecode.stepInstructions))
		assert.Equal(t, 5, len(clone.bytecode.stepInstructions))
		assert.Equal(t, 4, len(cloneClone.bytecode.stepInstructions))
	})

	t.Run("Test traversal with bindings", func(t *testing.T) {
		g := NewGraphTraversalSource(&Graph{}, &TraversalStrategies{}, newBytecode(nil), nil)
		bytecode := g.V((&Bindings{}).Of("a", []int32{1, 2, 3})).
			Out((&Bindings{}).Of("b", "created")).
			Where(T__.In((&Bindings{}).Of("c", "created"), (&Bindings{}).Of("d", "knows")).
				Count().Is((&Bindings{}).Of("e", P.Gt(2)))).bytecode
		assert.Equal(t, 5, len(bytecode.bindings))
		assert.Equal(t, []int32{1, 2, 3}, bytecode.bindings["a"])
		assert.Equal(t, "created", bytecode.bindings["b"])
		assert.Equal(t, "created", bytecode.bindings["c"])
		assert.Equal(t, "knows", bytecode.bindings["d"])
		assert.Equal(t, P.Gt(2), bytecode.bindings["e"])
		assert.Equal(t, &Binding{
			Key:   "b",
			Value: "created",
		}, bytecode.stepInstructions[1].arguments[0])
		assert.Equal(t, "binding[b=created]", bytecode.stepInstructions[1].arguments[0].(*Binding).String())
	})
}
