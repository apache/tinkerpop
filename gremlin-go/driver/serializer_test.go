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
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

const mapDataOrder1 = "[32 97 112 112 108 105 99 97 116 105 111 110 47 118 110 100 46 103 114 97 112 104 98 105 110 97 114 121 45 118 49 46 48 129 65 210 226 138 32 164 74 176 179 121 216 16 222 222 55 134 0 0 0 4 101 118 97 108 0 0 0 0 0 0 0 2 3 0 0 0 0 7 103 114 101 109 108 105 110 3 0 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41 3 0 0 0 0 7 97 108 105 97 115 101 115 10 0 0 0 0 1 3 0 0 0 0 1 103 3 0 0 0 0 1 103]"
const mapDataOrder2 = "[32 97 112 112 108 105 99 97 116 105 111 110 47 118 110 100 46 103 114 97 112 104 98 105 110 97 114 121 45 118 49 46 48 129 65 210 226 138 32 164 74 176 179 121 216 16 222 222 55 134 0 0 0 4 101 118 97 108 0 0 0 0 0 0 0 2 3 0 0 0 0 7 97 108 105 97 115 101 115 10 0 0 0 0 1 3 0 0 0 0 1 103 3 0 0 0 0 1 103 3 0 0 0 0 7 103 114 101 109 108 105 110 3 0 0 0 0 13 103 46 86 40 41 46 99 111 117 110 116 40 41]"

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
		serialized, _ := serializer.SerializeMessage(&testRequest)
		stringified := fmt.Sprintf("%v", serialized)
		if stringified != mapDataOrder1 && stringified != mapDataOrder2 {
			assert.Fail(t, "Error, expected serialized map data to match one of the provided binary arrays. Can vary based on ordering of keyset, but must map to one of two.")
		}
	})

	t.Run("test serialized response message", func(t *testing.T) {
		responseByteArray := []byte{129, 0, 251, 37, 42, 74, 117, 221, 71, 191, 183, 78, 86, 53, 0, 12, 132, 100, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 16, 47, 49, 50, 55, 46, 48, 46, 48, 46, 49, 58, 54, 50, 48, 51, 53, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.DeserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, "fb252a4a-75dd-47bf-b74e-5635000c8464", response.ResponseID.String())
		assert.Equal(t, uint16(200), response.ResponseStatus.code)
		assert.Equal(t, "", response.ResponseStatus.message)
		assert.Equal(t, map[string]interface{}{"host": "/127.0.0.1:62035"}, response.ResponseStatus.attributes)
		assert.Equal(t, map[string]interface{}{}, response.ResponseResult.Meta)
		assert.Equal(t, []interface{}{int64(0)}, response.ResponseResult.Data)
	})

	t.Run("test serialized response message w/ custom type", func(t *testing.T) {
		RegisterCustomTypeReader("janusgraph.RelationIdentifier", exampleJanusgraphRelationIdentifierReader)
		defer func() {
			UnregisterCustomTypeReader("janusgraph.RelationIdentifier")
		}()
		responseByteArray := []byte{129, 0, 69, 222, 40, 55, 95, 62, 75, 249, 134, 133, 155, 133, 43, 151, 221, 68, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 18, 47, 49, 48, 46, 50, 52, 52, 46, 48, 46, 51, 51, 58, 53, 49, 52, 55, 48, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 33, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 29, 106, 97, 110, 117, 115, 103, 114, 97, 112, 104, 46, 82, 101, 108, 97, 116, 105, 111, 110, 73, 100, 101, 110, 116, 105, 102, 105, 101, 114, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 16, 240, 0, 0, 0, 0, 0, 0, 100, 21, 0, 0, 0, 0, 0, 0, 24, 30, 0, 0, 0, 0, 0, 0, 0, 32, 56}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		response, err := serializer.DeserializeMessage(responseByteArray)
		assert.Nil(t, err)
		assert.Equal(t, "45de2837-5f3e-4bf9-8685-9b852b97dd44", response.ResponseID.String())
		assert.Equal(t, uint16(200), response.ResponseStatus.code)
		assert.Equal(t, "", response.ResponseStatus.message)
		assert.Equal(t, map[string]interface{}{"host": "/10.244.0.33:51470"}, response.ResponseStatus.attributes)
		assert.Equal(t, map[string]interface{}{}, response.ResponseResult.Meta)
		assert.NotNil(t, response.ResponseResult.Data)
	})
}

func TestSerializerFailures(t *testing.T) {
	t.Run("test convertArgs failure", func(t *testing.T) {
		var u, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
		testRequest := request{
			requestID: u,
			op:        "traversal",
			processor: "",
			// Invalid Input in args, so should fail
			args: map[string]interface{}{"invalidInput": "invalidInput", "aliases": map[string]interface{}{"g": "g"}},
		}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		resp, err := serializer.SerializeMessage(&testRequest)
		assert.Nil(t, resp)
		assert.NotNil(t, err)
		assert.True(t, isSameErrorCode(newError(err0704ConvertArgsNoSerializerError), err))
	})

	t.Run("test unkownCustomType failure", func(t *testing.T) {
		responseByteArray := []byte{129, 0, 69, 222, 40, 55, 95, 62, 75, 249, 134, 133, 155, 133, 43, 151, 221, 68, 0, 0, 0, 200, 0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 0, 0, 0, 0, 4, 104, 111, 115, 116, 3, 0, 0, 0, 0, 18, 47, 49, 48, 46, 50, 52, 52, 46, 48, 46, 51, 51, 58, 53, 49, 52, 55, 48, 0, 0, 0, 0, 9, 0, 0, 0, 0, 1, 33, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 29, 106, 97, 110, 117, 115, 103, 114, 97, 112, 104, 46, 82, 101, 108, 97, 116, 105, 111, 110, 73, 100, 101, 110, 116, 105, 102, 105, 101, 114, 0, 0, 16, 1, 0, 0, 0, 0, 0, 0, 0, 0, 16, 240, 0, 0, 0, 0, 0, 0, 100, 21, 0, 0, 0, 0, 0, 0, 24, 30, 0, 0, 0, 0, 0, 0, 0, 32, 56}
		serializer := newGraphBinarySerializer(newLogHandler(&defaultLogger{}, Error, language.English))
		resp, err := serializer.DeserializeMessage(responseByteArray)
		// a partial message will still be returned
		assert.NotNil(t, resp)
		assert.NotNil(t, err)
		assert.True(t, isSameErrorCode(newError(err0409GetSerializerToReadUnknownCustomTypeError), err))
	})
}

// exampleJanusgraphRelationIdentifierReader this implementation is not complete and is used only for the purposes of testing custom readers
func exampleJanusgraphRelationIdentifierReader(data *[]byte, i *int) (interface{}, error) {
	const relationIdentifierType = 0x1001
	const longMarker = 0

	// expect type code
	customDataTyp := readUint32Safe(data, i)
	if customDataTyp != relationIdentifierType {
		return nil, fmt.Errorf("unknown type code. got 0x%x, expected 0x%x", customDataTyp, relationIdentifierType)
	}

	// value flag, expect this to be non-nullable
	if readByteSafe(data, i) != valueFlagNone {
		return nil, errors.New("expected non-null value")
	}

	// outVertexId
	if readByteSafe(data, i) == longMarker {
		return readLongSafe(data, i), nil
	} else {
		return readString(data, i)
	}
}
