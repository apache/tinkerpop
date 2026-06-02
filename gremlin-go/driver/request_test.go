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

func TestRequest(t *testing.T) {
	t.Run("Test makeStringRequest() with no bindings", func(t *testing.T) {
		r := MakeStringRequest("g.V()", "g", *new(RequestOptions))
		assert.Equal(t, "g.V()", r.Gremlin)
		assert.Equal(t, "g", r.Fields["g"])
		assert.Equal(t, "gremlin-lang", r.Fields["language"])
		assert.Nil(t, r.Fields["bindings"])
	})

	t.Run("Test makeStringRequest() with custom evaluationTimeout", func(t *testing.T) {
		r := MakeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetEvaluationTimeout(1234).Create())
		assert.Equal(t, 1234, r.Fields["evaluationTimeout"])
	})

	t.Run("Test makeStringRequest() with custom batchSize", func(t *testing.T) {
		r := MakeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetBatchSize(123).Create())
		assert.Equal(t, 123, r.Fields["batchSize"])
	})

	t.Run("Test makeStringRequest() with custom userAgent", func(t *testing.T) {
		r := MakeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetUserAgent("TestUserAgent").Create())
		assert.Equal(t, "TestUserAgent", r.Fields["userAgent"])
	})

	t.Run("Test makeStringRequest() with bulkResults", func(t *testing.T) {
		r := MakeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetBulkResults(true).Create())
		assert.Equal(t, true, r.Fields["bulkResults"])
	})

	t.Run("Test makeStringRequest() with string bindings", func(t *testing.T) {
		r := MakeStringRequest("g.V(x)", "g",
			new(RequestOptionsBuilder).SetBindingsString("[\"x\":1]").Create())
		assert.Equal(t, "[\"x\":1]", r.Fields["bindings"])
	})

	t.Run("Test makeStringRequest() with map bindings converted to string", func(t *testing.T) {
		r := MakeStringRequest("g.V(x)", "g",
			new(RequestOptionsBuilder).SetBindings(map[string]interface{}{"x": int32(1)}).Create())
		assert.Contains(t, r.Fields["bindings"], "\"x\":1")
	})

	t.Run("Test RequestOptionsBuilder.Create() converts map bindings to string", func(t *testing.T) {
		opts := new(RequestOptionsBuilder).SetBindings(map[string]interface{}{"x": int32(1)}).Create()
		assert.NotEmpty(t, opts.bindingsString)
		assert.Contains(t, opts.bindingsString, "\"x\":1")
	})

	t.Run("Test RequestOptionsBuilder rejects mixing SetBindings and SetBindingsString", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic when mixing SetBindings and SetBindingsString")
			}
		}()
		new(RequestOptionsBuilder).
			SetBindings(map[string]interface{}{"x": int32(1)}).
			SetBindingsString("[\"y\":2]")
	})
}
