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

func TestRequestOptions(t *testing.T) {
	t.Run("Test RequestOptionsBuilder with custom timeoutMs", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetTimeoutMs(1234).Create()
		assert.Equal(t, 1234, r.timeoutMs)
	})
	t.Run("Test RequestOptionsBuilder with custom batchSize", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetBatchSize(123).Create()
		assert.Equal(t, 123, r.batchSize)
	})
	t.Run("Test RequestOptionsBuilder with custom userAgent", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetUserAgent("TestUserAgent").Create()
		assert.Equal(t, "TestUserAgent", r.userAgent)
	})
	t.Run("Test RequestOptionsBuilder with custom materializeProperties", func(t *testing.T) {
		r := new(RequestOptionsBuilder).SetMaterializeProperties("TestMaterializeProperties").Create()
		assert.Equal(t, "TestMaterializeProperties", r.materializeProperties)
	})
	t.Run("Test RequestOptionsBuilder with custom parameters", func(t *testing.T) {
		parameters := map[string]interface{}{"x": int32(2), "y": int32(5)}
		r := new(RequestOptionsBuilder).SetParameters(parameters).Create()
		assert.Contains(t, r.parametersString, "\"x\":2")
		assert.Contains(t, r.parametersString, "\"y\":5")
	})
	t.Run("Test RequestOptionsBuilder AddParameter() with no other parameters", func(t *testing.T) {
		r := new(RequestOptionsBuilder).AddParameter("x", int32(2)).AddParameter("y", int32(5)).Create()
		assert.Contains(t, r.parametersString, "\"x\":2")
		assert.Contains(t, r.parametersString, "\"y\":5")
	})
	t.Run("Test RequestOptionsBuilder AddParameter() overwriting existing key", func(t *testing.T) {
		r := new(RequestOptionsBuilder).AddParameter("x", int32(2)).AddParameter("x", int32(5)).Create()
		assert.Contains(t, r.parametersString, "\"x\":5")
	})
	t.Run("Test RequestOptionsBuilder AddParameter() with existing parameters", func(t *testing.T) {
		parameters := map[string]interface{}{"x": int32(2), "y": int32(5)}
		r := new(RequestOptionsBuilder).SetParameters(parameters).AddParameter("z", int32(7)).Create()
		assert.Contains(t, r.parametersString, "\"x\":2")
		assert.Contains(t, r.parametersString, "\"y\":5")
		assert.Contains(t, r.parametersString, "\"z\":7")
	})
	t.Run("Test RequestOptionsBuilder SetParameters resets previous parameters", func(t *testing.T) {
		parameters1 := map[string]interface{}{"x": int32(2)}
		parameters2 := map[string]interface{}{"z": int32(7)}
		r := new(RequestOptionsBuilder).SetParameters(parameters1).SetParameters(parameters2).Create()
		assert.Contains(t, r.parametersString, "\"z\":7")
		assert.NotContains(t, r.parametersString, "\"x\"")
	})
}
