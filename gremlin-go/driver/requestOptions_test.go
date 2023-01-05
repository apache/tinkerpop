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
	"github.com/google/uuid"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestOptions(t *testing.T) {
	t.Run("Test RequestOptionsBuilder with custom requestID", func(t *testing.T) {
		requestId := uuid.New()
		r := new(RequestOptionsBuilder).SetRequestId(requestId).Create()
		assert.Equal(t, requestId, r.requestID)
	})
	t.Run("Test RequestOptionsBuilder with custom evaluationTimeout", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetEvaluationTimeout(1234).Create()
		assert.Equal(t, 1234, r.evaluationTimeout)
	})
	t.Run("Test RequestOptionsBuilder with custom batchSize", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetBatchSize(123).Create()
		assert.Equal(t, 123, r.batchSize)
	})
	t.Run("Test RequestOptionsBuilder with custom userAgent", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetUserAgent("TestUserAgent").Create()
		assert.Equal(t, "TestUserAgent", r.userAgent)
	})
	t.Run("Test RequestOptionsBuilder with custom bindings", func(t *testing.T) {
		bindings := map[string]interface{}{"x": 2, "y": 5}
		r := new(RequestOptionsBuilder).SetBindings(bindings).Create()
		assert.Equal(t, bindings, r.bindings)
	})
	t.Run("Test RequestOptionsBuilder AddBinding() with no other bindings", func(t *testing.T) {
		r := new(RequestOptionsBuilder).AddBinding("x", 2).AddBinding("y", 5).Create()
		expectedBindings := map[string]interface{}{"x": 2, "y": 5}
		assert.Equal(t, expectedBindings, r.bindings)
	})
	t.Run("Test RequestOptionsBuilder AddBinding() overwriting existing key", func(t *testing.T) {
		r := new(RequestOptionsBuilder).AddBinding("x", 2).AddBinding("x", 5).Create()
		expectedBindings := map[string]interface{}{"x": 5}
		assert.Equal(t, expectedBindings, r.bindings)
	})
	t.Run("Test RequestOptionsBuilder AddBinding() with existing bindings", func(t *testing.T) {
		bindings := map[string]interface{}{"x": 2, "y": 5}
		r := new(RequestOptionsBuilder).SetBindings(bindings).AddBinding("z", 7).Create()
		expectedBindings := map[string]interface{}{"x": 2, "y": 5, "z": 7}
		assert.Equal(t, expectedBindings, r.bindings)
	})
	t.Run("Test RequestOptionsBuilder SetBinding(...), SetBinding(nil), AddBinding(...)", func(t *testing.T) {
		bindings := map[string]interface{}{"x": 2, "y": 5}
		r := new(RequestOptionsBuilder).SetBindings(bindings).
			SetBindings(nil).AddBinding("z", 7).Create()
		expectedBindings := map[string]interface{}{"z": 7}
		assert.Equal(t, expectedBindings, r.bindings)
	})
}
