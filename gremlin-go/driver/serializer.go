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
	"math/big"
	"reflect"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const graphBinaryMimeType = "application/vnd.graphbinary-v1.0"

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

func newGraphBinarySerializer(handler *logHandler) Serializer {
	serializer := graphBinaryTypeSerializer{handler}
	return GraphBinarySerializer{&serializer}
}

const versionByte byte = 0x81

func convertArgs(request *request, gs GraphBinarySerializer) (map[string]interface{}, error) {
	if request.op != bytecodeProcessor {
		return request.args, nil
	}

	// Convert to format:
	// args["gremlin"]: <serialized args["gremlin"]>
	gremlin := request.args["gremlin"]
	switch gremlin.(type) {
	case Bytecode:
		buffer := bytes.Buffer{}
		gremlinBuffer, err := gs.ser.write(gremlin, &buffer)
		if err != nil {
			return nil, err
		}
		request.args["gremlin"] = gremlinBuffer
		return request.args, nil
	default:
		var typeName string
		if gremlin != nil {
			typeName = reflect.TypeOf(gremlin).Name()
		}

		return nil, newError(err0704ConvertArgsNoSerializerError, typeName)
	}
}

// SerializeMessage serializes a request message into GraphBinary format.
//
// This method is part of the serializer interface and is used internally by the WebSocket driver.
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
func (gs GraphBinarySerializer) SerializeMessage(request *request) ([]byte, error) {
	args, err := convertArgs(request, gs)
	if err != nil {
		return nil, err
	}
	finalMessage, err := gs.buildMessage(request.requestID, byte(len(graphBinaryMimeType)), request.op, request.processor, args)
	if err != nil {
		return nil, err
	}
	return finalMessage, nil
}

func (gs *GraphBinarySerializer) buildMessage(id uuid.UUID, mimeLen byte, op string, processor string, args map[string]interface{}) ([]byte, error) {
	buffer := bytes.Buffer{}

	// mime header
	buffer.WriteByte(mimeLen)
	buffer.WriteString(graphBinaryMimeType)

	// Version
	buffer.WriteByte(versionByte)

	// Request uuid
	bigIntUUID := uuidToBigInt(id)
	lower := bigIntUUID.Uint64()
	upperBigInt := bigIntUUID.Rsh(&bigIntUUID, 64)
	upper := upperBigInt.Uint64()
	err := binary.Write(&buffer, binary.BigEndian, upper)
	if err != nil {
		return nil, err
	}
	err = binary.Write(&buffer, binary.BigEndian, lower)
	if err != nil {
		return nil, err
	}

	// op
	err = binary.Write(&buffer, binary.BigEndian, uint32(len(op)))
	if err != nil {
		return nil, err
	}

	_, err = buffer.WriteString(op)
	if err != nil {
		return nil, err
	}

	// processor
	err = binary.Write(&buffer, binary.BigEndian, uint32(len(processor)))
	if err != nil {
		return nil, err
	}

	_, err = buffer.WriteString(processor)
	if err != nil {
		return nil, err
	}

	// args
	err = binary.Write(&buffer, binary.BigEndian, uint32(len(args)))
	if err != nil {
		return nil, err
	}
	for k, v := range args {
		_, err = gs.ser.write(k, &buffer)
		if err != nil {
			return nil, err
		}

		switch t := v.(type) {
		case []byte:
			_, err = buffer.Write(t)
		default:
			_, err = gs.ser.write(t, &buffer)
		}
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func uuidToBigInt(requestID uuid.UUID) big.Int {
	var bigInt big.Int
	bigInt.SetString(strings.Replace(requestID.String(), "-", "", 4), 16)
	return bigInt
}

// DeserializeMessage deserializes a GraphBinary-encoded response message.
//
// This method is part of the serializer interface and is used internally by the WebSocket driver.
// It is also exposed publicly to enable alternative transport protocols (gRPC, HTTP/2, etc.) to
// deserialize responses received from a Gremlin server.
//
// Parameters:
//   - message: The GraphBinary-encoded response bytes
//
// Returns:
//   - response: The deserialized response containing results and metadata
//   - error: Any deserialization error encountered
//
// Example for alternative transports:
//
//	// Receive bytes from custom transport
//	serializer := newGraphBinarySerializer(nil)
//	resp, err := serializer.(graphBinarySerializer).DeserializeMessage(responseBytes)
//	results := resp.responseResult.data
func (gs GraphBinarySerializer) DeserializeMessage(message []byte) (Response, error) {
	var msg Response

	if message == nil || len(message) == 0 {
		gs.ser.logHandler.log(Error, nullInput)
		return msg, newError(err0405ReadValueInvalidNullInputError)
	}

	// Skip version and nullable byte.
	i := 2
	id, err := readUuid(&message, &i)
	if err != nil {
		return msg, err
	}
	msg.ResponseID = id.(uuid.UUID)
	msg.ResponseStatus.code = uint16(readUint32Safe(&message, &i) & 0xFF)
	isMessageValid := readByteSafe(&message, &i)
	if isMessageValid == 0 {
		message, err := readString(&message, &i)
		if err != nil {
			return msg, err
		}
		msg.ResponseStatus.message = message.(string)
	}
	attr, err := readMapUnqualified(&message, &i)
	if err != nil {
		return msg, err
	}
	msg.ResponseStatus.attributes = attr.(map[string]interface{})
	meta, err := readMapUnqualified(&message, &i)
	if err != nil {
		return msg, err
	}
	msg.ResponseResult.Meta = meta.(map[string]interface{})
	msg.ResponseResult.Data, err = readFullyQualifiedNullable(&message, &i, true)
	if err != nil {
		return msg, err
	}
	return msg, nil
}

func initSerializers() {
	serializers = map[dataType]writer{
		bytecodeType:   bytecodeWriter,
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
		vertexType:            vertexWriter,
		edgeType:              edgeWriter,
		propertyType:          propertyWriter,
		vertexPropertyType:    vertexPropertyWriter,
		lambdaType:            lambdaWriter,
		traversalStrategyType: traversalStrategyWriter,
		pathType:              pathWriter,
		setType:               setWriter,
		dateType:              timeWriter,
		durationType:          durationWriter,
		offsetDateTimeType:    offsetDateTimeWriter,
		cardinalityType:       enumWriter,
		columnType:            enumWriter,
		directionType:         enumWriter,
		dtType:                enumWriter,
		gTypeType:             enumWriter,
		operatorType:          enumWriter,
		orderType:             enumWriter,
		pickType:              enumWriter,
		popType:               enumWriter,
		tType:                 enumWriter,
		barrierType:           enumWriter,
		scopeType:             enumWriter,
		mergeType:             enumWriter,
		pType:                 pWriter,
		textPType:             textPWriter,
		bindingType:           bindingWriter,
		mapType:               mapWriter,
		listType:              listWriter,
		byteBuffer:            byteBufferWriter,
		classType:             classWriter,
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
		listType:   readList,
		mapType:    readMap,
		setType:    readSet,
		uuidType:   readUuid,
		byteBuffer: readByteBuffer,
		classType:  readClass,

		// Date Time
		dateType:           timeReader,
		timestampType:      timeReader,
		offsetDateTimeType: offsetDateTimeReader,
		durationType:       durationReader,

		// Graph
		traverserType:      traverserReader,
		vertexType:         vertexReader,
		edgeType:           edgeReader,
		propertyType:       propertyReader,
		vertexPropertyType: vertexPropertyReader,
		pathType:           pathReader,
		bulkSetType:        bulkSetReader,
		tType:              enumReader,
		directionType:      enumReader,
		dtType:             enumReader,
		gTypeType:          enumReader,
		bindingType:        bindingReader,

		// Metrics
		metricsType:          metricsReader,
		traversalMetricsType: traversalMetricsReader,

		// Customer
		customType: customTypeReader,
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
