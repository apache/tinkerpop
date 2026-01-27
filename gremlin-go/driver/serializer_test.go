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

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

const mapDataOrder1 = "[129 0 0 0 2 3 0 0 0 0 8 108 97 110 103 117 97 103 101 3 0 0 0 0 12 103 114 101 109 108 105 110 45 108 97 110 103 3 0 0 0 0 1 103 3 0 0 0 0 1 103 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41]"
const mapDataOrder2 = "[129 0 0 0 2 3 0 0 0 0 1 103 3 0 0 0 0 1 103 3 0 0 0 0 8 108 97 110 103 117 97 103 101 3 0 0 0 0 12 103 114 101 109 108 105 110 45 108 97 110 103 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41]"

func TestSerializer(t *testing.T) {
	t.Run("test serialized request message", func(t *testing.T) {
		testRequest := request{
			gremlin: "g.V().count()",
			fields:  map[string]interface{}{"g": "g", "language": "gremlin-lang"},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		serialized, _ := serializer.SerializeMessage(&testRequest)
		stringified := fmt.Sprintf("%v", serialized)
		if stringified != mapDataOrder1 && stringified != mapDataOrder2 {
			assert.Fail(t, "Error, expected serialized map data to match one of the provided binary arrays. Can vary based on ordering of keyset, but must map to one of two.")
		}
	})

	t.Run("test serialized response message", func(t *testing.T) {
		// response of status code 200 with message OK and data of list of single item of integer zero
		responseByteArray := []byte{129, 0, 1, 0, 0, 0, 0, 0, 253, 0, 0, 0, 0, 0, 200, 1, 1}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.DeserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, uint32(200), response.ResponseStatus.code)
		assert.Equal(t, "OK", response.ResponseStatus.message)
		assert.Equal(t, []interface{}{int32(0)}, response.ResponseResult.Data)
	})
}

func TestSerializerFailures(t *testing.T) {
	t.Run("test serialize request fields failure", func(t *testing.T) {
		invalid := "invalid"
		testRequest := request{
			// Invalid pointer type in fields, so should fail
			fields: map[string]interface{}{"invalidInput": &invalid, "g": "g"},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		resp, err := serializer.SerializeMessage(&testRequest)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
		assert.True(t, isSameErrorCode(newError(err0407GetSerializerToWriteUnknownTypeError), err))
	})
}
