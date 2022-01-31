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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

func TestSerializer(t *testing.T) {
	t.Run("test serialized request message", func(t *testing.T) {
		var u, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
		testRequest := request{
			requestID: u,
			op:        "eval",
			processor: "",
			args:      map[string]interface{}{"gremlin": "g.V().count()", "aliases": map[string]interface{}{"g": "g"}},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		serialized, _ := serializer.serializeMessage(&testRequest)
		assert.Equal(t, "[32 97 112 112 108 105 99 97 116 105 111 110 47 118 110 100 46 103 114 97 112 104 98 105 110 97 114 121 45 118 49 46 48 129 65 210 226 138 32 164 74 176 179 121 216 16 222 222 55 134 0 0 0 4 101 118 97 108 0 0 0 0 0 0 0 2 3 0 0 0 0 7 103 114 101 109 108 105 110 3 0 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41 3 0 0 0 0 7 97 108 105 97 115 101 115 10 0 0 0 0 1 3 0 0 0 0 1 103 3 0 0 0 0 1 103]", fmt.Sprintf("%v", serialized))
	})

	t.Run("test serialized request message", func(t *testing.T) {
		responseByteArray := []byte{129, 0, 251, 37, 42, 74, 117, 221, 71, 191, 183, 78, 86, 53, 0, 12, 132, 100, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 16, 47, 49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 54, 50, 48, 51, 53, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, _ := serializer.deserializeMessage(responseByteArray)
		assert.Equal(t, "fb252a4a-75dd-47bf-b74e-5635000c8464", response.responseID.String())
		assert.Equal(t, uint16(200), response.responseStatus.code)
		assert.Equal(t, "", response.responseStatus.message)
		assert.Equal(t, map[string]interface{}{"host": "/127.0.0.1:62035"}, response.responseStatus.attributes)
		assert.Equal(t, map[string]interface{}{}, response.responseResult.meta)
		assert.Equal(t, []interface{}{int64(0)}, response.responseResult.data)
	})
}
