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

const mapDataOrder1 = "[132 0 0 0 2 3 0 0 0 0 8 108 97 110 103 117 97 103 101 3 0 0 0 0 12 103 114 101 109 108 105 110 45 108 97 110 103 3 0 0 0 0 1 103 3 0 0 0 0 1 103 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41]"
const mapDataOrder2 = "[132 0 0 0 2 3 0 0 0 0 1 103 3 0 0 0 0 1 103 3 0 0 0 0 8 108 97 110 103 117 97 103 101 3 0 0 0 0 12 103 114 101 109 108 105 110 45 108 97 110 103 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41]"

func TestSerializer(t *testing.T) {
	t.Run("test serialized request message", func(t *testing.T) {
		testRequest := RequestMessage{
			Gremlin: "g.V().count()",
			Fields:  map[string]interface{}{"g": "g", "language": "gremlin-lang"},
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

	t.Run("test deserialized bulked response message", func(t *testing.T) {
		// Build a bulked response: version=0x84, bulked=0x01,
		// value: int32(7) [type=0x01, flag=0x00, value=0x00000007],
		// bulk: int64(3) [type=0x02, flag=0x00, value=0x0000000000000003],
		// EndOfStream marker [0xFD, 0x00, 0x00],
		// status code 200 [0x00, 0x00, 0x00, 0xC8],
		// null message [0x01], null exception [0x01]
		responseByteArray := []byte{
			0x84, 0x01, // version, bulked=true
			0x01, 0x00, 0x00, 0x00, 0x00, 0x07, // int32(7)
			0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, // int64(3) bulk count
			0xFD, 0x00, 0x00, // EndOfStream marker
			0x00, 0x00, 0x00, 0xC8, // status code 200
			0x01, // null message
			0x01, // null exception
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.DeserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, uint32(200), response.ResponseStatus.code)

		// Bulked response should produce a single Traverser with Bulk=3, Value=int32(7)
		data, ok := response.ResponseResult.Data.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 1, len(data))
		tr, ok := data[0].(*Traverser)
		assert.True(t, ok)
		assert.Equal(t, int64(3), tr.Bulk)
		assert.Equal(t, int32(7), tr.Value)
	})

	t.Run("test deserialized bulked response with multiple values", func(t *testing.T) {
		// Bulked response with two values:
		// int32(1) with bulk 2, int32(3) with bulk 1
		responseByteArray := []byte{
			0x84, 0x01, // version, bulked=true
			0x01, 0x00, 0x00, 0x00, 0x00, 0x01, // int32(1)
			0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, // int64(2) bulk count
			0x01, 0x00, 0x00, 0x00, 0x00, 0x03, // int32(3)
			0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, // int64(1) bulk count
			0xFD, 0x00, 0x00, // EndOfStream marker
			0x00, 0x00, 0x00, 0xC8, // status code 200
			0x01, // null message
			0x01, // null exception
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.DeserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, uint32(200), response.ResponseStatus.code)

		data, ok := response.ResponseResult.Data.([]interface{})
		assert.True(t, ok)
		assert.Equal(t, 2, len(data))

		tr0, ok := data[0].(*Traverser)
		assert.True(t, ok)
		assert.Equal(t, int64(2), tr0.Bulk)
		assert.Equal(t, int32(1), tr0.Value)

		tr1, ok := data[1].(*Traverser)
		assert.True(t, ok)
		assert.Equal(t, int64(1), tr1.Bulk)
		assert.Equal(t, int32(3), tr1.Value)
	})
}

func TestSerializerFailures(t *testing.T) {
	t.Run("test serialize request fields failure", func(t *testing.T) {
		invalid := "invalid"
		testRequest := RequestMessage{
			// Invalid pointer type in fields, so should fail
			Fields: map[string]interface{}{"invalidInput": &invalid, "g": "g"},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		resp, err := serializer.SerializeMessage(&testRequest)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
		assert.True(t, isSameErrorCode(newError(err0407GetSerializerToWriteUnknownTypeError), err))
	})
}
