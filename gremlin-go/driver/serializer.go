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
	"bytes"
	"encoding/binary"
	"io"
	"sync"
)

const graphBinaryMimeType = "application/vnd.graphbinary-v4.0"

// Serializer interface for serializers.
type Serializer interface {
	SerializeMessage(request *request) ([]byte, error)
	DeserializeMessage(message []byte) (Response, error)
}

// GraphBinarySerializer serializes/deserializes message to/from GraphBinary.
type GraphBinarySerializer struct {
	ser *graphBinaryTypeSerializer
}

// CustomTypeReader user provided function to deserialize custom types
// Deprecated: Custom type deserialization is handled by GraphBinaryDeserializer
type CustomTypeReader func(data *[]byte, i *int) (interface{}, error)

type writer func(interface{}, io.Writer, *graphBinaryTypeSerializer) error

var serializers map[dataType]writer

// customTypeReaderLock used to synchronize access to the customDeserializers map
// Deprecated: Custom type deserialization is handled by GraphBinaryDeserializer
var customTypeReaderLock = sync.RWMutex{}
var customDeserializers map[string]CustomTypeReader

func init() {
	initSerializers()
	customDeserializers = map[string]CustomTypeReader{}
}

func newGraphBinarySerializer(handler *logHandler) *GraphBinarySerializer {
	serializer := graphBinaryTypeSerializer{handler}
	return &GraphBinarySerializer{
		ser: &serializer,
	}
}

// TODO change for graph binary 4.0 version is finalized
const versionByte byte = 0x81

// SerializeMessage serializes a request message into GraphBinary format.
//
// This method is part of the serializer interface and is used internally by the HTTP driver.
// It is also exposed publicly to enable alternative transport protocols (gRPC, HTTP/2, etc.) to
// serialize requests created with MakeBytecodeRequest() or MakeStringRequest().
//
// The serialized bytes can be transmitted over any transport protocol that supports binary data.
//
// Parameters:
//   - request: The request to serialize (created via MakeBytecodeRequest or MakeStringRequest)
//
// Returns:
//   - []byte: The GraphBinary-encoded request ready for transmission
//   - error: Any serialization error encountered
//
// Example for alternative transports:
//
//	req := MakeBytecodeRequest(bytecode, "g", "")
//	serializer := newGraphBinarySerializer(nil)
//	bytes, err := serializer.(graphBinarySerializer).SerializeMessage(&req)
//	// Send bytes over custom transport
//
// SerializeMessage serializes a request message into GraphBinary.
func (gs *GraphBinarySerializer) SerializeMessage(request *request) ([]byte, error) {
	finalMessage, err := gs.buildMessage(request.gremlin, request.fields)
	if err != nil {
		return nil, err
	}
	return finalMessage, nil
}

func (gs *GraphBinarySerializer) buildMessage(gremlin string, args map[string]interface{}) ([]byte, error) {
	buffer := bytes.Buffer{}

	// Version
	buffer.WriteByte(versionByte)

	err := gs.ser.writeValue(args, &buffer, false)
	if err != nil {
		return nil, err
	}
	err = gs.ser.writeValue(gremlin, &buffer, false)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// DeserializeMessage deserializes a GraphBinary-encoded response message.
//
// This method is part of the serializer interface and is used internally by the HTTP driver.
// It is also exposed publicly to enable alternative transport protocols (gRPC, HTTP/2, etc.) to
// deserialize responses received from a Gremlin server.
//
// Parameters:
//   - message: The GraphBinary-encoded response bytes
//
// Returns:
//   - response: The deserialized Response containing results and metadata
//   - error: Any deserialization error encountered
//
// Example for alternative transports:
//
//	// Receive bytes from custom transport
//	serializer := newGraphBinarySerializer(nil)
//	resp, err := serializer.(graphBinarySerializer).DeserializeMessage(responseBytes)
//	results := resp.responseResult.data
func (gs *GraphBinarySerializer) DeserializeMessage(message []byte) (Response, error) {
	var msg Response

	if message == nil || len(message) == 0 {
		gs.ser.logHandler.log(Error, nullInput)
		return msg, newError(err0405ReadValueInvalidNullInputError)
	}

	d := NewGraphBinaryDeserializer(bytes.NewReader(message))

	// Read header
	if err := d.ReadHeader(); err != nil {
		return msg, err
	}

	results := make([]interface{}, 0)
	for {
		n, err := d.ReadFullyQualified()
		if err != nil {
			return msg, err
		}
		if n == EndOfStream() {
			break
		}
		results = append(results, n)
	}

	msg.ResponseResult.Data = results

	code, statusMsg, exception, err := d.ReadStatus()
	if err != nil {
		return msg, err
	}
	msg.ResponseStatus.code = code
	msg.ResponseStatus.message = "OK"
	if statusMsg != "" {
		msg.ResponseStatus.message = statusMsg
	}
	if exception != "" {
		msg.ResponseStatus.exception = exception
	}

	return msg, nil
}

func initSerializers() {
	serializers = map[dataType]writer{
		stringType:     stringWriter,
		bigDecimalType: bigDecimalWriter,
		bigIntegerType: bigIntWriter,
		longType:       longWriter,
		intType:        intWriter,
		shortType:      shortWriter,
		byteType: func(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
			return binary.Write(w, binary.BigEndian, value.(uint8))
		},
		booleanType: func(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
			return binary.Write(w, binary.BigEndian, value.(bool))
		},
		uuidType: func(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
			return binary.Write(w, binary.BigEndian, value)
		},
		floatType: func(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
			return binary.Write(w, binary.BigEndian, value)
		},
		doubleType: func(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
			return binary.Write(w, binary.BigEndian, value)
		},
		vertexType:         vertexWriter,
		edgeType:           edgeWriter,
		propertyType:       propertyWriter,
		vertexPropertyType: vertexPropertyWriter,
		pathType:           pathWriter,
		datetimeType:       dateTimeWriter,
		durationType:       durationWriter,
		directionType:      enumWriter,
		gTypeType:          enumWriter,
		tType:              enumWriter,
		mergeType:          enumWriter,
		mapType:            mapWriter,
		listType:           listWriter,
		setType:            setWriter,
		byteBuffer:         byteBufferWriter,
		markerType:         markerWriter,
	}
}

// RegisterCustomTypeReader register a reader (deserializer) for a custom type
// Deprecated: Custom type deserialization should be handled by extending GraphBinaryDeserializer
func RegisterCustomTypeReader(customTypeName string, reader CustomTypeReader) {
	customTypeReaderLock.Lock()
	defer customTypeReaderLock.Unlock()
	customDeserializers[customTypeName] = reader
}

// UnregisterCustomTypeReader unregister a reader (deserializer) for a custom type
// Deprecated: Custom type deserialization should be handled by extending GraphBinaryDeserializer
func UnregisterCustomTypeReader(customTypeName string) {
	customTypeReaderLock.Lock()
	defer customTypeReaderLock.Unlock()
	delete(customDeserializers, customTypeName)
}
