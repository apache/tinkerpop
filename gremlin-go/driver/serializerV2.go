package gremlingo

import (
	"github.com/google/uuid"
	"math/big"
	"reflect"
	"time"
)

type graphBinarySerializerV2 struct {
	logHandler *logHandler
}

var deserializers map[dataType]func(data *[]byte, i *int) interface{}
var serializers map[dataType]func(data interface{}, buffer *[]byte) []byte

func newGraphBinarySerializerV2(handler *logHandler) serializer {
	return graphBinarySerializerV2{handler}
}

func convertArgs2(request *request) (map[string]interface{}, error) {
	if request.op != bytecodeProcessor {
		return request.args, nil
	}

	// Convert to format:
	// args["gremlin"]: <serialized args["gremlin"]>
	gremlin := request.args["gremlin"]
	switch gremlin.(type) {
	case bytecode:
		var buffer []byte
		buffer, err := write(gremlin, &buffer)
		if err != nil {
			return nil, err
		}
		request.args["gremlin"] = buffer
		return request.args, nil
	default:
		var typeName string
		if gremlin != nil {
			typeName = reflect.TypeOf(gremlin).Name()
		}

		return nil, newError(err0704ConvertArgsNoSerializerError, typeName)
	}
}

func (gs graphBinarySerializerV2) serializeMessage(request *request) ([]byte, error) {
	// todo: implement
	/*tmp := newGraphBinarySerializer(gs.logHandler)
	return tmp.serializeMessage(request)*/
	initSerializers()

	args, err := convertArgs2(request)
	if err != nil {
		return nil, err
	}

	return buildMessage(request.requestID, byte(len(graphBinaryMimeType)), request.op, request.processor, args)
}

func buildMessage(id uuid.UUID, mimeLen byte, op string, processor string, args map[string]interface{}) ([]byte, error) {
	var buffer []byte

	// mime header
	buffer = writeByte(mimeLen, &buffer)
	buffer = writeString(graphBinaryMimeType, &buffer)

	// Version
	buffer = writeByte(versionByte, &buffer)

	// Request uuid
	bigIntUUID := uuidToBigInt(id)
	lower := bigIntUUID.Uint64()
	upperBigInt := bigIntUUID.Rsh(&bigIntUUID, 64)
	upper := upperBigInt.Uint64()
	buffer = writeLong(upper, &buffer)
	buffer = writeLong(lower, &buffer)

	// op
	buffer = writeInt(uint32(len(op)), &buffer)
	buffer = writeString(op, &buffer)

	// processor
	buffer = writeInt(uint32(len(processor)), &buffer)
	buffer = writeString(processor, &buffer)

	// args
	buffer = writeInt(uint32(len(args)), &buffer)
	for k, v := range args {
		buffer, err := write(k, &buffer)
		if err != nil {
			return nil, err
		}

		switch t := v.(type) {
		case []byte:
			buffer = writeBytes(t, &buffer)
		default:
			buffer, err = write(t, &buffer)
		}
		if err != nil {
			return nil, err
		}
	}
	return buffer, nil
}

func initSerializers() {
	if serializers == nil || len(serializers) == 0 {
		serializers = map[dataType]func(data interface{}, buffer *[]byte) []byte{
			// Primitive
			booleanType:    writeBoolean,
			byteType:       writeByte,
			shortType:      writeShort,
			intType:        writeInt,
			longType:       writeLong,
			bigIntegerType: writeBigInt,
			floatType:      writeFloat,
			doubleType:     writeDouble,
			stringType:     writeString,

			// Composite
			listType: writeList,
			mapType:  writeMap,
			setType:  writeSet,
			uuidType: writeUuid,
			/*
				// Date Time
				dateType:      timeReader2,
				timestampType: timeReader2,
				durationType:  durationReader2,

				// Graph
				traverserType:      traverserReader,
				vertexType:         vertexReader2,
				edgeType:           edgeReader2,
				propertyType:       propertyReader2,
				vertexPropertyType: vertexPropertyReader2,
				pathType:           pathReader2,
				bulkSetType:        bulkSetReader2,
				tType:              enumReader2,
				directionType:      enumReader2,
				bindingType:        bindingReader2,*/
		}
	}
}

func initDeserializers() {
	// todo: lock or don't care?
	if deserializers == nil || len(deserializers) == 0 {
		deserializers = map[dataType]func(data *[]byte, i *int) interface{}{
			// Primitive
			booleanType:    readBoolean,
			byteType:       readByte,
			shortType:      readShort,
			intType:        readInt,
			longType:       readLong,
			bigIntegerType: readBigInt,
			floatType:      readFloat,
			doubleType:     readDouble,
			stringType:     readString2,

			// Composite
			listType: readList,
			mapType:  readMap2,
			setType:  readSet,
			uuidType: readUuid,

			// Date Time
			dateType:      timeReader2,
			timestampType: timeReader2,
			durationType:  durationReader2,

			// Graph
			traverserType:      traverserReader,
			vertexType:         vertexReader2,
			edgeType:           edgeReader2,
			propertyType:       propertyReader2,
			vertexPropertyType: vertexPropertyReader2,
			pathType:           pathReader2,
			bulkSetType:        bulkSetReader2,
			tType:              enumReader2,
			directionType:      enumReader2,
			bindingType:        bindingReader2,
		}
	}
}

func (gs graphBinarySerializerV2) deserializeMessage(message []byte) (response, error) {
	initDeserializers()

	// Skip version and nullable byte.
	i := 2
	var msg response
	msg.responseID = readUuid(&message, &i).(uuid.UUID)
	msg.responseStatus.code = uint16(readUint32(&message, &i).(uint32) & 0xFF)
	if isMessageValid := readBoolean(&message, &i).(bool); isMessageValid {
		msg.responseStatus.message = readString2(&message, &i).(string)
	}
	msg.responseStatus.attributes = readMapUnqualified2(&message, &i).(map[string]interface{})
	msg.responseResult.meta = readMapUnqualified2(&message, &i).(map[string]interface{})
	msg.responseResult.data = readFullyQualifiedNullable(&message, &i, true)
	return msg, nil
}

// todo: map?
func getSerializerToWrite(val interface{}) (func(data interface{}, buffer *[]byte) ([]byte, error), error) {
	switch val.(type) {
	case *bytecode, bytecode, *GraphTraversal:
		return nil, nil
	case string:
		return writeString, nil
	case *big.Int:
		return writeBigInt, nil
	case int64, int, uint32:
		return func(data interface{}, buffer *[]byte) ([]byte, error) {
			switch v := data.(type) {
			case int:
				data = int64(v)
			case uint32:
				data = int64(v)
			}
			return writeLong(data, buffer)
		}, nil
	case int32, uint16:
		return func(data interface{}, buffer *[]byte) ([]byte, error) {
			switch v := data.(type) {
			case uint16:
				data = int32(v)
			}
			return writeInt(data, buffer)
		}, nil
	case int16:
		return writeShort, nil
	case uint8:
		return writeByte, nil
	case bool:
		return writeBoolean, nil
	case uuid.UUID:
		return writeUuid, nil
	case float32:
		return writeFloat, nil
	case float64:
		return writeDouble, nil
	case *Vertex:
		return writeVertex, nil
	case *Edge:
		return &graphBinaryTypeSerializer{dataType: edgeType, writer: edgeWriter, logHandler: serializer.logHandler}, nil
	case *Property:
		return &graphBinaryTypeSerializer{dataType: propertyType, writer: propertyWriter, logHandler: serializer.logHandler}, nil
	case *VertexProperty:
		return &graphBinaryTypeSerializer{dataType: vertexPropertyType, writer: vertexPropertyWriter, logHandler: serializer.logHandler}, nil
	case *Lambda:
		return &graphBinaryTypeSerializer{dataType: lambdaType, writer: lambdaWriter, logHandler: serializer.logHandler}, nil
	case *traversalStrategy:
		return &graphBinaryTypeSerializer{dataType: traversalStrategyType, writer: traversalStrategyWriter, logHandler: serializer.logHandler}, nil
	case *Path:
		return &graphBinaryTypeSerializer{dataType: pathType, writer: pathWriter, logHandler: serializer.logHandler}, nil
	case Set:
		return &graphBinaryTypeSerializer{dataType: setType, writer: setWriter, logHandler: serializer.logHandler}, nil
	case time.Time:
		return writeTime, nil
	case time.Duration:
		return writeDuration, nil
	case Cardinality:
		return &graphBinaryTypeSerializer{dataType: cardinalityType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Column:
		return &graphBinaryTypeSerializer{dataType: columnType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Direction:
		return &graphBinaryTypeSerializer{dataType: directionType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Operator:
		return &graphBinaryTypeSerializer{dataType: operatorType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Order:
		return &graphBinaryTypeSerializer{dataType: orderType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Pick:
		return &graphBinaryTypeSerializer{dataType: pickType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Pop:
		return &graphBinaryTypeSerializer{dataType: popType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case T:
		return &graphBinaryTypeSerializer{dataType: tType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Barrier:
		return &graphBinaryTypeSerializer{dataType: barrierType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Scope:
		return &graphBinaryTypeSerializer{dataType: scopeType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case p, Predicate:
		return &graphBinaryTypeSerializer{dataType: pType, writer: pWriter, logHandler: serializer.logHandler}, nil
	case textP, TextPredicate:
		return &graphBinaryTypeSerializer{dataType: textPType, writer: textPWriter, logHandler: serializer.logHandler}, nil
	case *Binding, Binding:
		return &graphBinaryTypeSerializer{dataType: bindingType, writer: bindingWriter, logHandler: serializer.logHandler}, nil
	default:
		switch reflect.TypeOf(val).Kind() {
		case reflect.Map:
			return writeMap, nil
		case reflect.Array, reflect.Slice:
			// We can write an array or slice into the list dataType.
			return writeList, nil
		default:
			//serializer.logHandler.logf(Error, serializeDataTypeError, reflect.TypeOf(val).Name())
			return nil, newError(err0407GetSerializerToWriteUnknownTypeError, reflect.TypeOf(val).Name())
		}
	}

}
