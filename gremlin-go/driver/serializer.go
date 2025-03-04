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
	"fmt"
	"os"
	"sync"
)

const graphBinaryMimeType = "application/vnd.graphbinary-v4.0"

// serializer interface for serializers.
type serializer interface {
	serializeMessage(request *request) ([]byte, error)
	deserializeMessage(message []byte) (response, error)
}

// graphBinarySerializer serializes/deserializes message to/from GraphBinary.
type graphBinarySerializer struct {
	ser *graphBinaryTypeSerializer
}

// CustomTypeReader user provided function to deserialize custom types
type CustomTypeReader func(data *[]byte, i *int) (interface{}, error)

type writer func(interface{}, *bytes.Buffer, *graphBinaryTypeSerializer) ([]byte, error)
type reader func(data *[]byte, i *int) (interface{}, error)

var deserializers map[dataType]reader
var serializers map[dataType]writer

// customTypeReaderLock used to synchronize access to the customDeserializers map
var customTypeReaderLock = sync.RWMutex{}
var customDeserializers map[string]CustomTypeReader

func init() {
	initSerializers()
	initDeserializers()
}

func newGraphBinarySerializer(handler *logHandler) serializer {
	serializer := graphBinaryTypeSerializer{handler}
	return graphBinarySerializer{&serializer}
}

// TODO change for graph binary 4.0 version is finalized
const versionByte byte = 0x81

// serializeMessage serializes a request message into GraphBinary.
func (gs graphBinarySerializer) serializeMessage(request *request) ([]byte, error) {
	finalMessage, err := gs.buildMessage(request.gremlin, request.fields)
	if err != nil {
		return nil, err
	}
	return finalMessage, nil
}

func (gs *graphBinarySerializer) buildMessage(gremlin string, args map[string]interface{}) ([]byte, error) {
	buffer := bytes.Buffer{}

	// Version
	buffer.WriteByte(versionByte)

	_, err := gs.ser.writeValue(args, &buffer, false)
	if err != nil {
		return nil, err
	}
	_, err = gs.ser.writeValue(gremlin, &buffer, false)
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// deserializeMessage deserializes a response message.
func (gs graphBinarySerializer) deserializeMessage(message []byte) (response, error) {
	var msg response

	if message == nil || len(message) == 0 {
		gs.ser.logHandler.log(Error, nullInput)
		return msg, newError(err0405ReadValueInvalidNullInputError)
	}
	results := make([]interface{}, 0)

	//Skip version and nullable byte.
	i := 2
	// TODO temp serialization before fully streaming set-up
	for len(message) > 0 {
		n, err := readFullyQualifiedNullable(&message, &i, true)
		if err != nil {
			return msg, err
		}
		_, _ = fmt.Fprintf(os.Stdout, "Deserializing data : %v\n", n)
		if n == EndOfStream() {
			break
		}
		results = append(results, n)
	}
	_, _ = fmt.Fprintf(os.Stdout, "Deserialized results : %s\n", results)
	msg.responseResult.data = results
	code := readUint32Safe(&message, &i)
	msg.responseStatus.code = code
	// TODO read status message
	msg.responseStatus.message = "OK"
	statusMsg, err := readUnqualified(&message, &i, stringType, true)
	if err != nil {
		return msg, err
	}
	if statusMsg != nil {
		msg.responseStatus.message = statusMsg.(string)
	}
	exception, err := readUnqualified(&message, &i, stringType, true)
	if err != nil {
		return msg, err
	}
	if exception != nil {
		msg.responseStatus.exception = exception.(string)
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
		byteType: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value.(uint8))
			return buffer.Bytes(), err
		},
		booleanType: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value.(bool))
			return buffer.Bytes(), err
		},
		uuidType: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		},
		floatType: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		},
		doubleType: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		},
		vertexType:         vertexWriter,
		edgeType:           edgeWriter,
		propertyType:       propertyWriter,
		vertexPropertyType: vertexPropertyWriter,
		pathType:           pathWriter,
		datetimeType:       timeWriter,
		durationType:       durationWriter,
		directionType:      enumWriter,
		tType:              enumWriter,
		mergeType:          enumWriter,
		mapType:            mapWriter,
		listType:           listWriter,
		setType:            setWriter,
		byteBuffer:         byteBufferWriter,
		markerType:         markerWriter,
	}
}

func initDeserializers() {
	deserializers = map[dataType]reader{
		// Primitive
		booleanType:    readBoolean,
		byteType:       readByte,
		shortType:      readShort,
		intType:        readInt,
		longType:       readLong,
		bigDecimalType: readBigDecimal,
		bigIntegerType: readBigInt,
		floatType:      readFloat,
		doubleType:     readDouble,
		stringType:     readString,

		// Composite
		//listType:   readList,
		//setType:    readSet,
		mapType:    readMap,
		uuidType:   readUuid,
		byteBuffer: readByteBuffer,

		// Date Time
		datetimeType: timeReader,
		durationType: durationReader,

		// Graph
		vertexType:         vertexReader,
		edgeType:           edgeReader,
		propertyType:       propertyReader,
		vertexPropertyType: vertexPropertyReader,
		pathType:           pathReader,
		tType:              enumReader,
		directionType:      enumReader,

		// Customer
		customType: customTypeReader,

		markerType: markerReader,
	}
	customDeserializers = map[string]CustomTypeReader{}
}

// RegisterCustomTypeReader register a reader (deserializer) for a custom type
func RegisterCustomTypeReader(customTypeName string, reader CustomTypeReader) {
	customTypeReaderLock.Lock()
	defer customTypeReaderLock.Unlock()
	customDeserializers[customTypeName] = reader
}

// UnregisterCustomTypeReader unregister a reader (deserializer) for a custom type
func UnregisterCustomTypeReader(customTypeName string) {
	customTypeReaderLock.Lock()
	defer customTypeReaderLock.Unlock()
	delete(customDeserializers, customTypeName)
}
