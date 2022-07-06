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
	"github.com/google/uuid"
	"math"
	"math/big"
	"reflect"
	"time"
)

// Version 1.0

// dataType graphBinary types.
type dataType uint8

// dataType defined as constants.
const (
	intType               dataType = 0x01
	longType              dataType = 0x02
	stringType            dataType = 0x03
	dateType              dataType = 0x04
	timestampType         dataType = 0x05
	classType             dataType = 0x06
	doubleType            dataType = 0x07
	floatType             dataType = 0x08
	listType              dataType = 0x09
	mapType               dataType = 0x0a
	setType               dataType = 0x0b
	uuidType              dataType = 0x0c
	edgeType              dataType = 0x0d
	pathType              dataType = 0x0e
	propertyType          dataType = 0x0f
	vertexType            dataType = 0x11
	vertexPropertyType    dataType = 0x12
	barrierType           dataType = 0x13
	bindingType           dataType = 0x14
	cardinalityType       dataType = 0x16
	bytecodeType          dataType = 0x15
	columnType            dataType = 0x17
	directionType         dataType = 0x18
	operatorType          dataType = 0x19
	orderType             dataType = 0x1a
	pickType              dataType = 0x1b
	popType               dataType = 0x1c
	lambdaType            dataType = 0x1d
	pType                 dataType = 0x1e
	scopeType             dataType = 0x1f
	tType                 dataType = 0x20
	traverserType         dataType = 0x21
	bigDecimalType        dataType = 0x22
	bigIntegerType        dataType = 0x23
	byteType              dataType = 0x24
	byteBuffer            dataType = 0x25
	shortType             dataType = 0x26
	booleanType           dataType = 0x27
	textPType             dataType = 0x28
	traversalStrategyType dataType = 0x29
	bulkSetType           dataType = 0x2a
	metricsType           dataType = 0x2c
	traversalMetricsType  dataType = 0x2d
	durationType          dataType = 0x81
	nullType              dataType = 0xFE
)

var nullBytes = []byte{nullType.getCodeByte(), 0x01}

func (dataType dataType) getCodeByte() byte {
	return byte(dataType)
}

func (dataType dataType) getCodeBytes() []byte {
	return []byte{dataType.getCodeByte()}
}

// graphBinaryTypeSerializer struct for the different types of serializers.
type graphBinaryTypeSerializer struct {
	logHandler *logHandler
}

func (serializer *graphBinaryTypeSerializer) writeType(value interface{}, buffer *bytes.Buffer, writer writer) ([]byte, error) {
	return serializer.writeTypeValue(value, buffer, writer, true)
}

func (serializer *graphBinaryTypeSerializer) writeTypeValue(value interface{}, buffer *bytes.Buffer, writer writer, nullable bool) ([]byte, error) {
	if value == nil {
		if !nullable {
			serializer.logHandler.log(Error, unexpectedNull)
			return nil, newError(err0401WriteTypeValueUnexpectedNullError)
		}
		serializer.writeValueFlagNull(buffer)
		return buffer.Bytes(), nil
	}
	if nullable {
		serializer.writeValueFlagNone(buffer)
	}
	return writer(value, buffer, serializer)
}

// Format: {length}{item_0}...{item_n}
func listWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	v := reflect.ValueOf(value)
	valLen := v.Len()
	err := binary.Write(buffer, binary.BigEndian, int32(valLen))
	if err != nil {
		return nil, err
	}
	for i := 0; i < valLen; i++ {
		_, err := typeSerializer.write(v.Index(i).Interface(), buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

// Format: {length}{value}
func byteBufferWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var v ByteBuffer
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*ByteBuffer))
	} else {
		v = value.(ByteBuffer)
	}

	err := binary.Write(buffer, binary.BigEndian, int32(len(v.Data)))
	if err != nil {
		return nil, err
	}
	buffer.Write(v.Data)

	return buffer.Bytes(), nil
}

// Format: {length}{item_0}...{item_n}
// Item format: {type_code}{type_info}{value_flag}{value}
func mapWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	if value == nil {
		_, err := typeSerializer.writeValue(int32(0), buffer, false)
		if err != nil {
			return nil, err
		}
		return buffer.Bytes(), nil
	}

	v := reflect.ValueOf(value)
	keys := v.MapKeys()
	err := binary.Write(buffer, binary.BigEndian, int32(len(keys)))
	if err != nil {
		return nil, err
	}
	for _, k := range keys {
		convKey := k.Convert(v.Type().Key())
		// serialize k
		_, err := typeSerializer.write(k.Interface(), buffer)
		if err != nil {
			return nil, err
		}
		// serialize v.MapIndex(c_key)
		val := v.MapIndex(convKey)
		_, err = typeSerializer.write(val.Interface(), buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

func instructionWriter(instructions []instruction, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) error {
	// Write {steps_length}, i.e number of steps.
	err := binary.Write(buffer, binary.BigEndian, int32(len(instructions)))
	if err != nil {
		return err
	}

	// Write {step_0} to {step_n}.
	for _, instruction := range instructions {
		// Write {name} of {step_i}.
		// Note: {name} follows string writing, therefore write string length followed by actual string.
		_, err = typeSerializer.writeValue(instruction.operator, buffer, false)
		if err != nil {
			return err
		}

		// Write {values_length} of {step_i}.
		err = binary.Write(buffer, binary.BigEndian, int32(len(instruction.arguments)))
		if err != nil {
			return err
		}

		// Write {values_0} to {values_n}.
		for _, argument := range instruction.arguments {
			_, err = typeSerializer.write(argument, buffer)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Format: {steps_length}{step_0}…{step_n}{sources_length}{source_0}…{source_n}
// Where:
//		{steps_length} is an Int value describing the amount of steps.
//		{step_i} is composed of {name}{values_length}{value_0}…{value_n}, where:
//      {name} is a String. This is also known as the operator.
//		{values_length} is an Int describing the amount values.
//		{value_i} is a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value} describing the step argument.
func bytecodeWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var bc Bytecode
	switch typedVal := value.(type) {
	case *GraphTraversal:
		bc = *typedVal.Bytecode
	case Bytecode:
		bc = typedVal
	case *Bytecode:
		bc = *typedVal
	default:
		return nil, newError(err0402BytecodeWriterError)
	}

	// Write {steps_length} and {step_0} through {step_n}, then {sources_length} and {source_0} through {source_n}
	err := instructionWriter(bc.stepInstructions, buffer, typeSerializer)
	if err != nil {
		return nil, err
	}
	err = instructionWriter(bc.sourceInstructions, buffer, typeSerializer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func stringWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	err := binary.Write(buffer, binary.BigEndian, int32(len(value.(string))))
	if err != nil {
		return nil, err
	}
	_, err = buffer.WriteString(value.(string))
	return buffer.Bytes(), err
}

func longWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	switch v := value.(type) {
	case int:
		value = int64(v)
	case uint32:
		value = int64(v)
	}
	err := binary.Write(buffer, binary.BigEndian, value)
	return buffer.Bytes(), err
}

func intWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	switch v := value.(type) {
	case uint16:
		value = int32(v)
	}
	err := binary.Write(buffer, binary.BigEndian, value.(int32))
	return buffer.Bytes(), err
}

func shortWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	switch v := value.(type) {
	case int8:
		value = int16(v)
	}
	err := binary.Write(buffer, binary.BigEndian, value.(int16))
	return buffer.Bytes(), err
}

// Golang stores BigIntegers with big.Int types
// it contains an unsigned representation of the number and uses a boolean to track +ve and -ve
// getSignedBytesFromBigInt gives us the signed(two's complement) byte array that represents the unsigned byte array in
// big.Int
func getSignedBytesFromBigInt(n *big.Int) []byte {
	var one = big.NewInt(1)
	if n.Sign() == 1 {
		// add a buffer 0x00 byte to the start of byte array if number is positive and has a 1 in its MSB
		b := n.Bytes()
		if b[0]&0x80 > 0 {
			b = append([]byte{0}, b...)
		}
		return b
	} else if n.Sign() == -1 {
		// Convert Unsigned byte array to signed byte array
		length := uint(n.BitLen()/8+1) * 8
		b := new(big.Int).Add(n, new(big.Int).Lsh(one, length)).Bytes()
		// Strip any redundant 0xff bytes from the front of the byte array if the following byte starts with a 1
		if len(b) >= 2 && b[0] == 0xff && b[1]&0x80 != 0 {
			b = b[1:]
		}
		return b
	}
	return []byte{}
}

// Format: {length}{value_0}...{value_n}
func bigIntWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	var v big.Int
	switch val := value.(type) {
	case uint:
		v = *(new(big.Int).SetUint64(uint64(val)))
	case uint64:
		v = *(new(big.Int).SetUint64(val))
	default:
		if reflect.TypeOf(value).Kind() == reflect.Ptr {
			v = *(value.(*big.Int))
		} else {
			v = value.(big.Int)
		}
	}
	signedBytes := getSignedBytesFromBigInt(&v)
	err := binary.Write(buffer, binary.BigEndian, int32(len(signedBytes)))
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(signedBytes); i++ {
		err := binary.Write(buffer, binary.BigEndian, signedBytes[i])
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), nil
}

// Format: {scale}{unscaled_value}
func bigDecimalWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var v BigDecimal
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*BigDecimal))
	} else {
		v = value.(BigDecimal)
	}
	err := binary.Write(buffer, binary.BigEndian, v.Scale)
	if err != nil {
		return nil, err
	}

	return bigIntWriter(v.UnscaledValue, buffer, typeSerializer)
}

func classWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var v GremlinType
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*GremlinType))
	} else {
		v = value.(GremlinType)
	}
	return stringWriter(v.Fqcn, buffer, typeSerializer)
}

// Format: {Id}{Label}{properties}
func vertexWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	v := value.(*Vertex)
	_, err := typeSerializer.write(v.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue(v.Label, buffer, false)
	if err != nil {
		return nil, err
	}
	// Note that as TinkerPop currently send "references" only, properties will always be null
	buffer.Write(nullBytes)
	return buffer.Bytes(), nil
}

// Format: {Id}{Label}{inVId}{inVLabel}{outVId}{outVLabel}{parent}{properties}
func edgeWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	e := value.(*Edge)
	_, err := typeSerializer.write(e.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified
	_, err = typeSerializer.writeValue(e.Label, buffer, false)
	if err != nil {
		return nil, err
	}

	// Write in-vertex
	_, err = typeSerializer.write(e.InV.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue(e.InV.Label, buffer, false)
	if err != nil {
		return nil, err
	}
	// Write out-vertex
	_, err = typeSerializer.write(e.OutV.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue(e.OutV.Label, buffer, false)
	if err != nil {
		return nil, err
	}

	// Note that as TinkerPop currently send "references" only, parent and properties will always be null
	buffer.Write(nullBytes)
	buffer.Write(nullBytes)
	return buffer.Bytes(), nil
}

//Format: {Key}{Value}{parent}
func propertyWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	v := value.(*Property)

	// Not fully qualified.
	_, err := typeSerializer.writeValue(v.Key, buffer, false)
	if err != nil {
		return nil, err
	}

	_, err = typeSerializer.write(v.Value, buffer)
	if err != nil {
		return nil, err
	}
	// Note that as TinkerPop currently send "references" only, parent and properties  will always be null
	buffer.Write(nullBytes)
	return buffer.Bytes(), nil
}

//Format: {Id}{Label}{Value}{parent}{properties}
func vertexPropertyWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	vp := value.(*VertexProperty)
	_, err := typeSerializer.write(vp.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue(vp.Label, buffer, false)
	if err != nil {
		return nil, err
	}
	_, err = typeSerializer.write(vp.Value, buffer)
	if err != nil {
		return nil, err
	}
	// Note that as TinkerPop currently send "references" only, parent and properties will always be null
	buffer.Write(nullBytes)
	buffer.Write(nullBytes)
	return buffer.Bytes(), nil
}

//Format: {Labels}{Objects}
func pathWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	p := value.(*Path)
	_, err := typeSerializer.write(p.Labels, buffer)
	if err != nil {
		return nil, err
	}
	_, err = typeSerializer.write(p.Objects, buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

// Format: Same as List.
// Mostly similar to listWriter with small changes
func setWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	slice := value.(Set).ToSlice()
	return listWriter(slice, buffer, typeSerializer)
}

func timeWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	t := value.(time.Time)
	err := binary.Write(buffer, binary.BigEndian, t.UnixMilli())
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func durationWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	t := value.(time.Duration)
	sec := int64(t / time.Second)
	nanos := int32(t % time.Second)
	err := binary.Write(buffer, binary.BigEndian, sec)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buffer, binary.BigEndian, nanos)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

const (
	valueFlagNull byte = 1
	valueFlagNone byte = 0
)

func enumWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	_, err := typeSerializer.write(reflect.ValueOf(value).String(), buffer)
	return buffer.Bytes(), err
}

// Format: {language}{script}{arguments_length}
func lambdaWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	lambda := value.(*Lambda)
	if lambda.Language == "" {
		lambda.Language = "gremlin-groovy"
	}
	_, err := typeSerializer.writeValue(lambda.Language, buffer, false)
	if err != nil {
		return nil, err
	}

	_, err = typeSerializer.writeValue(lambda.Script, buffer, false)
	if err != nil {
		return nil, err
	}

	// It's hard to know how many parameters there are without extensive string parsing.
	// Instead, we can set -1 which means unknown.
	err = binary.Write(buffer, binary.BigEndian, int32(-1))
	if err != nil {
		return nil, err
	}

	return buffer.Bytes(), nil
}

// Format: {strategy_class}{configuration}
func traversalStrategyWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	ts := value.(*traversalStrategy)

	_, err := typeSerializer.writeValue(ts.name, buffer, false)
	if err != nil {
		return nil, err
	}

	return mapWriter(ts.configuration, buffer, typeSerializer)
}

func pWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var v p
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*p))
	} else {
		v = value.(p)
	}
	_, err := typeSerializer.writeValue(v.operator, buffer, false)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buffer, binary.BigEndian, int32(len(v.values)))
	if err != nil {
		return nil, err
	}

	for _, pValue := range v.values {
		_, err := typeSerializer.write(pValue, buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), err
}

func textPWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var v textP
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*textP))
	} else {
		v = value.(textP)
	}
	_, err := typeSerializer.writeValue(v.operator, buffer, false)
	if err != nil {
		return nil, err
	}

	err = binary.Write(buffer, binary.BigEndian, int32(len(v.values)))
	if err != nil {
		return nil, err
	}

	for _, pValue := range v.values {
		_, err := typeSerializer.write(pValue, buffer)
		if err != nil {
			return nil, err
		}
	}
	return buffer.Bytes(), err
}

// Format: {key}{value}
func bindingWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	var v Binding
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*Binding))
	} else {
		v = value.(Binding)
	}

	// Not fully qualified.
	_, err := typeSerializer.writeValue(v.Key, buffer, false)
	if err != nil {
		return nil, err
	}

	_, err = typeSerializer.write(v.Value, buffer)
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func (serializer *graphBinaryTypeSerializer) getType(val interface{}) (dataType, error) {
	switch val.(type) {
	case *Bytecode, Bytecode, *GraphTraversal:
		return bytecodeType, nil
	case string:
		return stringType, nil
	case uint, uint64, *big.Int:
		return bigIntegerType, nil
	case int64, int, uint32:
		return longType, nil
	case int32, uint16:
		return intType, nil
	case int8, int16: // GraphBinary doesn't have a type for signed 8-bit integer, serializing int8 as Short instead.
		return shortType, nil
	case uint8:
		return byteType, nil
	case bool:
		return booleanType, nil
	case uuid.UUID:
		return uuidType, nil
	case float32:
		return floatType, nil
	case float64:
		return doubleType, nil
	case *Vertex:
		return vertexType, nil
	case *Edge:
		return edgeType, nil
	case *Property:
		return propertyType, nil
	case *VertexProperty:
		return vertexPropertyType, nil
	case *Lambda:
		return lambdaType, nil
	case *traversalStrategy:
		return traversalStrategyType, nil
	case *Path:
		return pathType, nil
	case Set:
		return setType, nil
	case time.Time:
		return dateType, nil
	case time.Duration:
		return durationType, nil
	case cardinality:
		return cardinalityType, nil
	case column:
		return columnType, nil
	case direction:
		return directionType, nil
	case operator:
		return operatorType, nil
	case order:
		return orderType, nil
	case pick:
		return pickType, nil
	case pop:
		return popType, nil
	case t:
		return tType, nil
	case barrier:
		return barrierType, nil
	case scope:
		return scopeType, nil
	case p, Predicate:
		return pType, nil
	case textP, TextPredicate:
		return textPType, nil
	case *Binding, Binding:
		return bindingType, nil
	case *BigDecimal, BigDecimal:
		return bigDecimalType, nil
	case *GremlinType, GremlinType:
		return classType, nil
	case *Metrics, Metrics:
		return metricsType, nil
	case *TraversalMetrics, TraversalMetrics:
		return traversalMetricsType, nil
	case *ByteBuffer, ByteBuffer:
		return byteBuffer, nil
	default:
		switch reflect.TypeOf(val).Kind() {
		case reflect.Map:
			return mapType, nil
		case reflect.Array, reflect.Slice:
			// We can write an array or slice into the list dataType.
			return listType, nil
		default:
			serializer.logHandler.logf(Error, serializeDataTypeError, reflect.TypeOf(val).Name())
			return intType, newError(err0407GetSerializerToWriteUnknownTypeError, reflect.TypeOf(val).Name())
		}
	}
}

func (serializer *graphBinaryTypeSerializer) getWriter(dataType dataType) (writer, error) {
	if writer, ok := serializers[dataType]; ok {
		return writer, nil
	}
	serializer.logHandler.logf(Error, deserializeDataTypeError, int32(dataType))
	return nil, newError(err0407GetSerializerToWriteUnknownTypeError, dataType)
}

// gets the type of the serializer based on the value
func (serializer *graphBinaryTypeSerializer) getSerializerToWrite(val interface{}) (writer, dataType, error) {
	dataType, err := serializer.getType(val)
	if err != nil {
		return nil, intType, err
	}
	writer, err := serializer.getWriter(dataType)
	if err != nil {
		return nil, intType, err
	}

	return writer, dataType, nil
}

// Writes an object in fully-qualified format, containing {type_code}{type_info}{value_flag}{value}.
func (serializer *graphBinaryTypeSerializer) write(valueObject interface{}, buffer *bytes.Buffer) (interface{}, error) {
	if valueObject == nil {
		// return Object of type "unspecified object null" with the value flag set to null.
		buffer.Write(nullBytes)
		return buffer.Bytes(), nil
	}

	writer, dataType, err := serializer.getSerializerToWrite(valueObject)
	if err != nil {
		return nil, err
	}
	buffer.Write(dataType.getCodeBytes())
	return serializer.writeType(valueObject, buffer, writer)
}

// Writes a value without including type information.
func (serializer *graphBinaryTypeSerializer) writeValue(value interface{}, buffer *bytes.Buffer, nullable bool) (interface{}, error) {
	if value == nil {
		if !nullable {
			serializer.logHandler.log(Error, unexpectedNull)
			return nil, newError(err0403WriteValueUnexpectedNullError)
		}
		serializer.writeValueFlagNull(buffer)
		return buffer.Bytes(), nil
	}

	writer, _, err := serializer.getSerializerToWrite(value)
	if err != nil {
		return nil, err
	}
	message, err := serializer.writeTypeValue(value, buffer, writer, nullable)
	if err != nil {
		return nil, err
	}
	return message, nil
}

func (serializer *graphBinaryTypeSerializer) writeValueFlagNull(buffer *bytes.Buffer) {
	buffer.WriteByte(valueFlagNull)
}

func (serializer *graphBinaryTypeSerializer) writeValueFlagNone(buffer *bytes.Buffer) {
	buffer.WriteByte(valueFlagNone)
}

// readers

func readTemp(data *[]byte, i *int, len int) *[]byte {
	tmp := make([]byte, len)
	for j := 0; j < len; j++ {
		tmp[j] = (*data)[j+*i]
	}
	*i += len
	return &tmp
}

// Primitive
func readBoolean(data *[]byte, i *int) (interface{}, error) {
	b, _ := readByte(data, i)
	return b != uint8(0), nil
}

func readByteSafe(data *[]byte, i *int) byte {
	*i++
	return (*data)[*i-1]
}
func readByte(data *[]byte, i *int) (interface{}, error) {
	return readByteSafe(data, i), nil
}

func readShort(data *[]byte, i *int) (interface{}, error) {
	return int16(binary.BigEndian.Uint16(*readTemp(data, i, 2))), nil
}

func readIntSafe(data *[]byte, i *int) int32 {
	return int32(binary.BigEndian.Uint32(*readTemp(data, i, 4)))
}
func readInt(data *[]byte, i *int) (interface{}, error) {
	return readIntSafe(data, i), nil
}

func readLongSafe(data *[]byte, i *int) int64 {
	return int64(binary.BigEndian.Uint64(*readTemp(data, i, 8)))
}
func readLong(data *[]byte, i *int) (interface{}, error) {
	return readLongSafe(data, i), nil
}

func readBigInt(data *[]byte, i *int) (interface{}, error) {
	sz := readIntSafe(data, i)
	b := readTemp(data, i, int(sz))

	var newBigInt = big.NewInt(0).SetBytes(*b)
	var one = big.NewInt(1)
	if len(*b) == 0 {
		return newBigInt, nil
	}
	// If the first bit in the first element of the byte array is a 1, we need to interpret the byte array as a two's complement representation
	if (*b)[0]&0x80 == 0x00 {
		newBigInt.SetBytes(*b)
		return newBigInt, nil
	}
	// Undo two's complement to byte array and set negative boolean to true
	length := uint((len(*b)*8)/8+1) * 8
	b2 := new(big.Int).Sub(newBigInt, new(big.Int).Lsh(one, length)).Bytes()

	// Strip the resulting 0xff byte at the start of array
	b2 = b2[1:]

	// Strip any redundant 0x00 byte at the start of array
	if b2[0] == 0x00 {
		b2 = b2[1:]
	}
	newBigInt = big.NewInt(0)
	newBigInt.SetBytes(b2)
	newBigInt.Neg(newBigInt)
	return newBigInt, nil
}

func readBigDecimal(data *[]byte, i *int) (interface{}, error) {
	bigDecimal := &BigDecimal{}
	bigDecimal.Scale = readIntSafe(data, i)
	unscaled, err := readBigInt(data, i)
	if err != nil {
		return nil, err
	}
	bigDecimal.UnscaledValue = *unscaled.(*big.Int)
	return bigDecimal, nil
}

func readUint32Safe(data *[]byte, i *int) uint32 {
	return binary.BigEndian.Uint32(*readTemp(data, i, 4))
}

func readFloat(data *[]byte, i *int) (interface{}, error) {
	return math.Float32frombits(binary.BigEndian.Uint32(*readTemp(data, i, 4))), nil
}

func readDouble(data *[]byte, i *int) (interface{}, error) {
	return math.Float64frombits(binary.BigEndian.Uint64(*readTemp(data, i, 8))), nil
}

func readString(data *[]byte, i *int) (interface{}, error) {
	sz := int(readUint32Safe(data, i))
	if sz == 0 {
		return "", nil
	}
	*i += sz
	return string((*data)[*i-sz : *i]), nil
}

func readDataType(data *[]byte, i *int) dataType {
	return dataType(readByteSafe(data, i))
}

func getDefaultValue(dataType dataType) interface{} {
	switch dataType {
	case intType, bigIntegerType, longType, shortType, byteType, booleanType, floatType, doubleType:
		return 0
	case traverserType, stringType:
		return ""
	case uuidType:
		return uuid.Nil
	case vertexType:
		return Vertex{}
	case edgeType:
		return Edge{}
	case propertyType:
		return Property{}
	case vertexPropertyType:
		return VertexProperty{}
	case pathType:
		return Path{}
	case setType:
		return SimpleSet{}
	case dateType, timestampType:
		return time.Time{}
	case durationType:
		return time.Duration(0)
	default:
		return nil
	}
}

// Composite
func readList(data *[]byte, i *int) (interface{}, error) {
	sz := readIntSafe(data, i)
	var valList []interface{}
	for j := int32(0); j < sz; j++ {
		val, err := readFullyQualifiedNullable(data, i, true)
		if err != nil {
			return nil, err
		}
		valList = append(valList, val)
	}
	return valList, nil
}

func readByteBuffer(data *[]byte, i *int) (interface{}, error) {
	r := &ByteBuffer{}
	sz := readIntSafe(data, i)
	r.Data = make([]byte, sz)
	for j := int32(0); j < sz; j++ {
		r.Data[j] = readByteSafe(data, i)
	}
	return r, nil
}

func readMap(data *[]byte, i *int) (interface{}, error) {
	sz := readUint32Safe(data, i)
	var mapData = make(map[interface{}]interface{})
	for j := uint32(0); j < sz; j++ {
		k, err := readFullyQualifiedNullable(data, i, true)
		if err != nil {
			return nil, err
		}
		v, err := readFullyQualifiedNullable(data, i, true)
		if err != nil {
			return nil, err
		}
		if k == nil {
			mapData[nil] = v
		} else {
			switch reflect.TypeOf(k).Kind() {
			case reflect.Map:
				mapData[&k] = v
				break
			case reflect.Slice:
				mapData[fmt.Sprint(k)] = v
				break
			default:
				mapData[k] = v
				break
			}
		}
	}
	return mapData, nil
}

func readMapUnqualified(data *[]byte, i *int) (interface{}, error) {
	sz := readUint32Safe(data, i)
	var mapData = make(map[string]interface{})
	for j := uint32(0); j < sz; j++ {
		keyDataType := readDataType(data, i)
		if keyDataType != stringType {
			return nil, newError(err0703ReadMapNonStringKeyError)
		}

		// Skip nullable, key must be present
		*i++

		k, err := readString(data, i)
		if err != nil {
			return nil, err
		}
		mapData[k.(string)], err = readFullyQualifiedNullable(data, i, true)
		if err != nil {
			return nil, err
		}
	}
	return mapData, nil
}

func readSet(data *[]byte, i *int) (interface{}, error) {
	list, err := readList(data, i)
	if err != nil {
		return nil, err
	}
	return NewSimpleSet(list.([]interface{})...), nil
}

func readUuid(data *[]byte, i *int) (interface{}, error) {
	id, _ := uuid.FromBytes(*readTemp(data, i, 16))
	return id, nil
}

func timeReader(data *[]byte, i *int) (interface{}, error) {
	return time.UnixMilli(readLongSafe(data, i)), nil
}

func durationReader(data *[]byte, i *int) (interface{}, error) {
	return time.Duration(readLongSafe(data, i)*int64(time.Second) + int64(readIntSafe(data, i))), nil
}

// Graph

// {fully qualified id}{unqualified label}
func vertexReader(data *[]byte, i *int) (interface{}, error) {
	return vertexReaderNullByte(data, i, true)
}

// {fully qualified id}{unqualified label}{[unused null byte]}
func vertexReaderNullByte(data *[]byte, i *int, unusedByte bool) (interface{}, error) {
	var err error
	v := new(Vertex)
	v.Id, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	label, err := readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	v.Label = label.(string)
	if unusedByte {
		*i += 2
	}
	return v, nil
}

// {fully qualified id}{unqualified label}{in vertex w/o null byte}{out vertex}{unused null byte}{unused null byte}
func edgeReader(data *[]byte, i *int) (interface{}, error) {
	var err error
	e := new(Edge)
	e.Id, err = readFullyQualifiedNullable(data, i, true)
	label, err := readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	e.Label = label.(string)
	v, err := vertexReaderNullByte(data, i, false)
	if err != nil {
		return nil, err
	}
	e.InV = *v.(*Vertex)
	v, err = vertexReaderNullByte(data, i, false)
	if err != nil {
		return nil, err
	}
	e.OutV = *v.(*Vertex)
	*i += 4
	return e, nil
}

// {unqualified key}{fully qualified value}{null byte}
func propertyReader(data *[]byte, i *int) (interface{}, error) {
	p := new(Property)
	key, err := readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	p.Key = key.(string)
	p.Value, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	*i += 2
	return p, nil
}

// {fully qualified id}{unqualified label}{fully qualified value}{null byte}{null byte}
func vertexPropertyReader(data *[]byte, i *int) (interface{}, error) {
	var err error
	vp := new(VertexProperty)
	vp.Id, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	label, err := readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	vp.Label = label.(string)
	vp.Value, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}

	*i += 4
	return vp, nil
}

// {list of set of strings}{list of fully qualified objects}
func pathReader(data *[]byte, i *int) (interface{}, error) {
	path := new(Path)
	newLabels, err := readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	for _, param := range newLabels.([]interface{}) {
		path.Labels = append(path.Labels, param.(*SimpleSet))
	}
	objects, err := readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	path.Objects = objects.([]interface{})
	return path, err
}

// {bulk int}{fully qualified value}
func traverserReader(data *[]byte, i *int) (interface{}, error) {
	var err error
	traverser := new(Traverser)
	traverser.bulk = readLongSafe(data, i)
	traverser.value, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	return traverser, nil
}

// {int32 length}{fully qualified item_0}{int64 repetition_0}...{fully qualified item_n}{int64 repetition_n}
func bulkSetReader(data *[]byte, i *int) (interface{}, error) {
	sz := int(readIntSafe(data, i))
	var valList []interface{}
	for j := 0; j < sz; j++ {
		val, err := readFullyQualifiedNullable(data, i, true)
		if err != nil {
			return nil, err
		}
		rep := readLongSafe(data, i)
		for k := 0; k < int(rep); k++ {
			valList = append(valList, val)
		}
	}
	return valList, nil
}

// {type code (always string so ignore)}{nil code (always false so ignore)}{int32 size}{string enum}
func enumReader(data *[]byte, i *int) (interface{}, error) {
	typeCode := readDataType(data, i)
	if typeCode != stringType {
		return nil, newError(err0406EnumReaderInvalidTypeError)
	}
	*i++
	return readString(data, i)
}

// {unqualified key}{fully qualified value}
func bindingReader(data *[]byte, i *int) (interface{}, error) {
	b := new(Binding)
	val, err := readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	b.Key = val.(string)

	b.Value, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// {id}{name}{duration}{counts}{annotations}{nested_metrics}
func metricsReader(data *[]byte, i *int) (interface{}, error) {
	metrics := new(Metrics)
	val, err := readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	metrics.Id = val.(string)

	val, err = readUnqualified(data, i, stringType, false)
	if err != nil {
		return nil, err
	}
	metrics.Name = val.(string)

	dur, err := readLong(data, i)
	if err != nil {
		return nil, err
	}
	metrics.Duration = dur.(int64)

	counts, err := readMap(data, i)
	cmap := counts.(map[interface{}]interface{})
	if err != nil {
		return nil, err
	}
	metrics.Counts = make(map[string]int64, len(cmap))
	for k := range cmap {
		metrics.Counts[k.(string)] = cmap[k].(int64)
	}

	annotations, err := readMap(data, i)
	if err != nil {
		return nil, err
	}
	amap := annotations.(map[interface{}]interface{})
	if err != nil {
		return nil, err
	}
	metrics.Annotations = make(map[string]interface{}, len(amap))
	for k := range amap {
		metrics.Annotations[k.(string)] = amap[k]
	}

	nested, err := readList(data, i)
	if err != nil {
		return nil, err
	}
	list := nested.([]interface{})
	metrics.NestedMetrics = make([]Metrics, len(list))
	for i, metric := range list {
		metrics.NestedMetrics[i] = metric.(Metrics)
	}

	return metrics, nil
}

// {id}{name}{duration}{counts}{annotations}{nested_metrics}
func traversalMetricsReader(data *[]byte, i *int) (interface{}, error) {
	m := new(TraversalMetrics)
	dur, err := readLong(data, i)
	if err != nil {
		return nil, err
	}
	m.Duration = dur.(int64)

	nested, err := readList(data, i)
	if err != nil {
		return nil, err
	}
	list := nested.([]interface{})
	m.Metrics = make([]Metrics, len(list))
	for i, metric := range list {
		m.Metrics[i] = *metric.(*Metrics)
	}

	return m, nil
}

// Format: A String containing the fqcn.
func readClass(data *[]byte, i *int) (interface{}, error) {
	gremlinType := new(GremlinType)
	str, err := readString(data, i)
	if err != nil {
		return nil, err
	}
	gremlinType.Fqcn = str.(string)

	return gremlinType, nil
}

func readUnqualified(data *[]byte, i *int, dataTyp dataType, nullable bool) (interface{}, error) {
	if nullable && readByteSafe(data, i) == valueFlagNull {
		return getDefaultValue(dataTyp), nil
	}
	deserializer, ok := deserializers[dataTyp]
	if !ok {
		return nil, newError(err0408GetSerializerToReadUnknownTypeError, dataTyp)
	}
	return deserializer(data, i)
}

func readFullyQualifiedNullable(data *[]byte, i *int, nullable bool) (interface{}, error) {
	dataTyp := readDataType(data, i)
	if dataTyp == nullType {
		if readByteSafe(data, i) != valueFlagNull {
			return nil, newError(err0404ReadNullTypeError)
		}
		return nil, nil
	} else if nullable {
		if readByteSafe(data, i) == valueFlagNull {
			return getDefaultValue(dataTyp), nil
		}
	}
	deserializer, ok := deserializers[dataTyp]
	if !ok {
		return nil, newError(err0408GetSerializerToReadUnknownTypeError, dataTyp)
	}
	return deserializer(data, i)
}
