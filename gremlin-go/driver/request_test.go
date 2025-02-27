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
		r := makeStringRequest("g.V()", "g", *new(RequestOptions))
		assert.Equal(t, "g.V()", r.args["gremlin"])
		assert.Equal(t, "g", r.args["g"])
		assert.Equal(t, "gremlin-lang", r.args["language"])
		assert.Nil(t, r.args["bindings"])
	})

	t.Run("Test makeStringRequest() with custom evaluationTimeout", func(t *testing.T) {
		r := makeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetEvaluationTimeout(1234).Create())
		assert.Equal(t, 1234, r.args["evaluationTimeout"])
	})

	t.Run("Test makeStringRequest() with custom batchSize", func(t *testing.T) {
		r := makeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetBatchSize(123).Create())
		assert.Equal(t, 123, r.args["batchSize"])
	})

	t.Run("Test makeStringRequest() with custom userAgent", func(t *testing.T) {
		r := makeStringRequest("g.V()", "g",
			new(RequestOptionsBuilder).SetUserAgent("TestUserAgent").Create())
		assert.Equal(t, "TestUserAgent", r.args["userAgent"])
	})
}
