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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytecode(t *testing.T) {
	t.Run("Constructor", func(t *testing.T) {
		bc1 := NewBytecode(nil)
		assert.NotNil(t, bc1.bindings)
		assert.NotNil(t, bc1.sourceInstructions)
		assert.NotNil(t, bc1.stepInstructions)
		assert.Empty(t, bc1.bindings)
		assert.Empty(t, bc1.sourceInstructions)
		assert.Empty(t, bc1.stepInstructions)

		sourceInstructions := []instruction{{
			operator:  "mockSource",
			arguments: nil,
		}}
		stepInstructions := []instruction{{
			operator:  "mockStep",
			arguments: nil,
		}}
		bindingMap := make(map[string]interface{})
		bindingMap["mock"] = 123
		bc1.sourceInstructions = sourceInstructions
		bc1.stepInstructions = stepInstructions
		bc1.bindings = bindingMap

		bc2 := NewBytecode(bc1)
		assert.NotNil(t, bc2.bindings)
		assert.NotNil(t, bc2.sourceInstructions)
		assert.NotNil(t, bc2.stepInstructions)
		assert.Empty(t, bc2.bindings)
		assert.Equal(t, sourceInstructions, bc2.sourceInstructions)
		assert.Equal(t, stepInstructions, bc2.stepInstructions)
	})

	t.Run("AddSource", func(t *testing.T) {
		expectedSourceInstructions := []instruction{{
			operator:  "mockSource",
			arguments: []interface{}{123},
		}}
		bc := NewBytecode(nil)
		err := bc.AddSource("mockSource", 123)
		assert.Nil(t, err)
		assert.Equal(t, expectedSourceInstructions, bc.sourceInstructions)
	})

	t.Run("addStep", func(t *testing.T) {
		expectedStepInstructions := []instruction{{
			operator:  "mockStep",
			arguments: []interface{}{123},
		}}
		bc := NewBytecode(nil)
		err := bc.AddStep("mockStep", 123)
		assert.Nil(t, err)
		assert.Equal(t, expectedStepInstructions, bc.stepInstructions)
	})

	t.Run("convertArgument", func(t *testing.T) {
		bc := NewBytecode(nil)

		t.Run("map", func(t *testing.T) {
			testMap := make(map[string]int)
			testMap["test"] = 123
			converted, err := bc.convertArgument(testMap)
			assert.Nil(t, err)
			for k, v := range converted.(map[interface{}]interface{}) {
				key := k.(string)
				value := v.(int)
				assert.Equal(t, "test", key)
				assert.Equal(t, 123, value)
			}
		})

		t.Run("slice", func(t *testing.T) {
			testSlice := []int{1, 2, 3}
			converted, err := bc.convertArgument(testSlice)
			assert.Nil(t, err)
			for i, value := range converted.([]interface{}) {
				assert.Equal(t, testSlice[i], value)
			}
		})

		t.Run("binding", func(t *testing.T) {
			testKey := "testKey"
			testValue := "testValue"
			testBinding := &Binding{
				Key:   testKey,
				Value: testValue,
			}
			converted, err := bc.convertArgument(testBinding)
			assert.Nil(t, err)
			assert.Equal(t, testBinding, converted)
			assert.Equal(t, testValue, bc.bindings[testKey])
		})
	})

	t.Run("Test Bytecode traversal argument conversion without Graph", func(t *testing.T) {
		bc := Bytecode{}
		traversal := &GraphTraversal{
			&Traversal{},
		}
		traversal.graph = nil
		traversal.Bytecode = &Bytecode{}
		traversalBytecode, err := bc.convertArgument(traversal)
		assert.Nil(t, err)
		assert.Equal(t, traversal.Bytecode, traversalBytecode)
	})

	t.Run("Test Bytecode traversal argument conversion with Graph", func(t *testing.T) {
		// This should fail.
		bc := Bytecode{}
		traversal := &GraphTraversal{
			&Traversal{},
		}
		traversal.graph = &Graph{}
		traversal.Bytecode = &Bytecode{}
		traversalBytecode, err := bc.convertArgument(traversal)
		assert.Equal(t, newError(err1001ConvertArgumentChildTraversalNotFromAnonError), err)
		assert.Nil(t, traversalBytecode)
	})

	t.Run("Test Bytecode traversal argument multiple bindings", func(t *testing.T) {
		bc := Bytecode{}
		bc.bindings = map[string]interface{}{}
		testTraversal := &GraphTraversal{
			&Traversal{},
		}
		testTraversal.Bytecode = &Bytecode{}
		testTraversal.Bytecode.bindings = map[string]interface{}{"mock": "123"}
		traversalBytecode, err := bc.convertArgument(testTraversal)
		assert.Nil(t, err)
		assert.Equal(t, testTraversal.Bytecode, traversalBytecode)
	})
}
