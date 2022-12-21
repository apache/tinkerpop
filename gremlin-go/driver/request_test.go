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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequest(t *testing.T) {
	t.Run("Test makeStringRequest() with custom requestID", func(t *testing.T) {
		requestId := fmt.Sprintf("%v", uuid.New())
		r := makeStringRequest("g.V()", "g", "", map[string]interface{}{"requestId": requestId})
		assert.Equal(t, requestId, fmt.Sprintf("%v", r.requestID))
	})

	t.Run("Test makeStringRequest() with no bindings", func(t *testing.T) {
		r := makeStringRequest("g.V()", "g", "")
		assert.NotNil(t, r.requestID)
		assert.NotEqual(t, uuid.Nil, r.requestID)
	})

	t.Run("Test makeStringRequest() with custom evaluationTimeout", func(t *testing.T) {
		r := makeStringRequest("g.V()", "g", "", map[string]interface{}{"evaluationTimeout": 1234})
		assert.NotNil(t, r.requestID)
		assert.NotEqual(t, uuid.Nil, r.requestID)
		bindings := r.args["bindings"].(map[string]interface{})
		assert.Equal(t, 1234, bindings["evaluationTimeout"])
	})
}
