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
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// Version 1.0

// DataType graphBinary types.
type DataType uint8

// DataType defined as constants.
const (
	IntType            DataType = 0x01
	LongType           DataType = 0x02
	StringType         DataType = 0x03
	DateType           DataType = 0x04
	TimestampType      DataType = 0x05
	DoubleType         DataType = 0x07
	FloatType          DataType = 0x08
	ListType           DataType = 0x09
	MapType            DataType = 0x0a
	SetType            DataType = 0x0b
	UUIDType           DataType = 0x0c
	EdgeType           DataType = 0x0d
	PathType           DataType = 0x0e
	PropertyType       DataType = 0x0f
	VertexType         DataType = 0x11
	VertexPropertyType DataType = 0x12
	LambdaType         DataType = 0x1d
	BarrierType        DataType = 0x13
	CardinalityType    DataType = 0x16
	BytecodeType       DataType = 0x15
	ColumnType         DataType = 0x17
	DirectionType      DataType = 0x18
	OperatorType       DataType = 0x19
	OrderType          DataType = 0x1a
	PickType           DataType = 0x1b
	PopType            DataType = 0x1c
	PType              DataType = 0x1e
	ScopeType          DataType = 0x1f
	TType              DataType = 0x20
	TraverserType      DataType = 0x21
	BigIntegerType     DataType = 0x23
	ByteType           DataType = 0x24
	ShortType          DataType = 0x26
	BooleanType        DataType = 0x27
	TextPType          DataType = 0x28
	BulkSetType        DataType = 0x2a
	DurationType       DataType = 0x81
	NullType           DataType = 0xFE
)

var nullBytes = []byte{NullType.getCodeByte(), 0x01}

func (dataType DataType) getCodeByte() byte {
	return byte(dataType)
}

func (dataType DataType) getCodeBytes() []byte {
	return []byte{dataType.getCodeByte()}
}

// graphBinaryTypeSerializer struct for the different types of serializers.
type graphBinaryTypeSerializer struct {
	dataType       DataType
	writer         func(interface{}, *bytes.Buffer, *graphBinaryTypeSerializer) ([]byte, error)
	reader         func(*bytes.Buffer, *graphBinaryTypeSerializer) (interface{}, error)
	nullFlagReturn interface{}
	logHandler     *logHandler
}

func (serializer *graphBinaryTypeSerializer) writeType(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	return serializer.writeTypeValue(value, buffer, typeSerializer, true)
}

func (serializer *graphBinaryTypeSerializer) writeTypeValue(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer, nullable bool) ([]byte, error) {
	if value == nil {
		if !nullable {
			serializer.logHandler.log(Error, unexpectedNull)
			return nil, errors.New("unexpected null value to writeType when nullable is false")
		}
		serializer.writeValueFlagNull(buffer)
		return buffer.Bytes(), nil
	}
	if nullable {
		serializer.writeValueFlagNone(buffer)
	}
	return typeSerializer.writer(value, buffer, typeSerializer)
}

func (serializer graphBinaryTypeSerializer) readType(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	return serializer.readTypeValue(buffer, typeSerializer, true)
}

func (serializer graphBinaryTypeSerializer) readTypeValue(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer, nullable bool) (interface{}, error) {
	if nullable {
		nullFlag, err := buffer.ReadByte()
		if err != nil {
			return nil, err
		}
		if nullFlag == valueFlagNull {
			return serializer.nullFlagReturn, nil
		}
	}
	return typeSerializer.reader(buffer, typeSerializer)
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

func listReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	var size int32
	err := binary.Read(buffer, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}
	// Currently, all list data types will be converted to a slice of interface{}.
	var valList []interface{}
	for i := 0; i < int(size); i++ {
		val, err := typeSerializer.read(buffer)
		if err != nil {
			return nil, err
		}
		valList = append(valList, val)
	}
	return valList, nil
}

// Format: {length}{item_0}...{item_n}
func mapWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
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

func mapReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	var size int32
	err := binary.Read(buffer, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}
	// Currently, all map data types will be converted to a map of [interface{}]interface{}.
	valMap := make(map[interface{}]interface{})
	for i := 0; i < int(size); i++ {
		key, err := typeSerializer.read(buffer)
		if err != nil {
			return nil, err
		}
		val, err := typeSerializer.read(buffer)
		if err != nil {
			return nil, err
		}
		if key == nil {
			valMap[nil] = val
		} else {
			switch reflect.TypeOf(key).Kind() {
			case reflect.Map:
				// Passing the pointer to the map as key, as maps are not hashable
				valMap[&key] = val
			case reflect.Slice:
				// Turning map keys of slice type into string type for comparison purposes
				// string slices should also be converted into slices more easily
				valMap[fmt.Sprint(key)] = val
			default:
				valMap[key] = val
			}
		}
	}
	return valMap, nil
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
	var bc bytecode
	switch typedVal := value.(type) {
	case *GraphTraversal:
		bc = *typedVal.bytecode
	case bytecode:
		bc = typedVal
	default:
		return nil, errors.New("need GraphTraversal or bytecode to write bytecode")
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

func getBigIntFromSignedBytes(b []byte) *big.Int {
	var newBigInt = big.NewInt(0).SetBytes(b)
	var one = big.NewInt(1)
	if len(b) == 0 {
		return newBigInt
	}
	// If the first bit in the first element of the byte array is a 1, we need to interpret the byte array as a two's complement representation
	if b[0]&0x80 == 0x00 {
		newBigInt.SetBytes(b)
		return newBigInt
	}
	// Undo two's complement to byte array and set negative boolean to true
	length := uint((len(b)*8)/8+1) * 8
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
	return newBigInt
}

// Format: {length}{value_0}...{value_n}
func bigIntWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	v := value.(*big.Int)
	signedBytes := getSignedBytesFromBigInt(v)
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

func bigIntReader(buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) (interface{}, error) {
	var size int32
	err := binary.Read(buffer, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}
	var valList = make([]byte, size)
	for i := int32(0); i < size; i++ {
		err := binary.Read(buffer, binary.BigEndian, &valList[i])
		if err != nil {
			return nil, err
		}
	}
	return getBigIntFromSignedBytes(valList), nil
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

func vertexReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	var err error
	v := new(Vertex)
	v.Id, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	newLabel, err := typeSerializer.readValue(buffer, byte(StringType), false)
	if err != nil {
		return nil, err
	}
	v.Label = newLabel.(string)

	// read null byte
	_, _ = typeSerializer.read(buffer)
	return v, nil
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

func edgeReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	e := new(Edge)
	var err error

	// Edge ID.
	e.Id, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}

	// Edge label - not fully qualified.
	newLabel, err := typeSerializer.readValue(buffer, byte(StringType), false)
	if err != nil {
		return nil, err
	}
	e.Label = newLabel.(string)

	// Create new in-vertex.
	inV := new(Vertex)

	// In-vertex ID - fully qualified.
	inV.Id, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}

	// In-vertex label -nNot fully qualified.
	inVLabel, err := typeSerializer.readValue(buffer, byte(StringType), false)
	if err != nil {
		return nil, err
	}
	inV.Label = inVLabel.(string)

	e.InV = *inV

	// Create new out-vertex.
	outV := new(Vertex)

	// In-vertex ID - fully qualified.
	outV.Id, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}

	// In-vertex label - not fully qualified.
	outVLabel, err := typeSerializer.readValue(buffer, byte(StringType), false)
	if err != nil {
		return nil, err
	}
	outV.Label = outVLabel.(string)

	e.OutV = *outV

	// Read null bytes.
	_, _ = typeSerializer.read(buffer)
	_, _ = typeSerializer.read(buffer)
	return e, nil
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

func propertyReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	p := new(Property)

	// Not fully qualified.
	newKey, err := typeSerializer.readValue(buffer, byte(StringType), false)
	if err != nil {
		return nil, err
	}
	p.Key = newKey.(string)

	newValue, err := typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}
	p.Value = newValue

	// read null byte
	_, _ = typeSerializer.read(buffer)
	return p, nil
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

func vertexPropertyReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	var err error
	vp := new(VertexProperty)
	vp.Id, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}

	// Label - NOT fully qualified.
	vp.Label, err = readString(buffer)
	if err != nil {
		return nil, err
	}
	vp.Value, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}
	_, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}
	_, err = typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}
	return vp, nil
}

//Format: {Labels}{Objects}
// TODO: Path serialization is currently incomplete as Labels are represented as list of lists due to lack of native set types in go. Fully functional Path serialization will be implemented when set is implemented in AN-1032
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

func pathReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	p := new(Path)
	newLabels, err := typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}
	for _, param := range newLabels.([]interface{}) {
		p.Labels = append(p.Labels, param.(*SimpleSet))
	}
	newObjects, err := typeSerializer.read(buffer)
	if err != nil {
		return nil, err
	}
	p.Objects = newObjects.([]interface{})
	return p, nil
}

// Format: Same as List.
// Mostly similar to listWriter and listReader with small changes
func setWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	slice := value.(Set).ToSlice()
	return listWriter(slice, buffer, typeSerializer)
}

func setReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	slice, err := listReader(buffer, typeSerializer)
	if err != nil {
		return nil, err
	}
	return NewSimpleSet(slice.([]interface{})...), nil
}

func timeWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	t := value.(time.Time)
	err := binary.Write(buffer, binary.BigEndian, t.UnixMilli())
	if err != nil {
		return nil, err
	}
	return buffer.Bytes(), nil
}

func timeReader(buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) (interface{}, error) {
	var newMillis int64
	err := binary.Read(buffer, binary.BigEndian, &newMillis)
	if err != nil {
		return nil, err
	}
	return time.UnixMilli(newMillis), nil
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

func durationReader(buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) (interface{}, error) {
	var sec int64
	err := binary.Read(buffer, binary.BigEndian, &sec)
	if err != nil {
		return nil, err
	}
	var nanos int32
	err = binary.Read(buffer, binary.BigEndian, &nanos)
	if err != nil {
		return nil, err
	}
	total := sec*int64(time.Second) + int64(nanos)
	newDuration := time.Duration(total)
	return newDuration, nil
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

// Format: {length}{item_0}...{item_n}
// Where:
// {length} is an Int describing the length of the BulkSet.
// {item_0}...{item_n} are the items of the BulkSet. {item_i} is a sequence of a fully qualified typed value composed of {type_code}{type_info}{value_flag}{value} followed by the "bulk" which is a Long value.
// If the implementing language does not have a BulkSet object to deserialize into, this format can be coerced to a List and still be considered compliant with Gremlin. Simply "expand the bulk" by adding the item to the List the number of times specified by the bulk.
func bulkSetReader(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
	var size int32
	err := binary.Read(buffer, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}
	var valList []interface{}
	for i := 0; i < int(size); i++ {
		val, err := typeSerializer.read(buffer)
		if err != nil {
			return nil, err
		}
		var rep int64
		err = binary.Read(buffer, binary.BigEndian, &rep)
		if err != nil {
			return nil, err
		}
		for j := 0; j < int(rep); j++ {
			valList = append(valList, val)
		}
	}
	return valList, nil
}

// Format: a single string representing the enum value
func enumReader(buffer *bytes.Buffer, serializer *graphBinaryTypeSerializer) (interface{}, error) {
	var typeCode uint8
	err := binary.Read(buffer, binary.BigEndian, &typeCode)
	if err != nil {
		return nil, err
	} else if typeCode != StringType.getCodeByte() {
		return nil, fmt.Errorf("error, expected string type for enum, but got %x", typeCode)
	}

	var nilByte uint8
	err = binary.Read(buffer, binary.BigEndian, &nilByte)
	if err != nil {
		return nil, err
	} else if nilByte != 0 {
		return nil, nil
	}

	var size int32
	err = binary.Read(buffer, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}

	valBytes := make([]byte, size)
	_, err = buffer.Read(valBytes)
	return string(valBytes), err
}

// gets the type of the serializer based on the value
func (serializer *graphBinaryTypeSerializer) getSerializerToWrite(val interface{}) (*graphBinaryTypeSerializer, error) {
	switch val.(type) {
	case bytecode, *GraphTraversal:
		return &graphBinaryTypeSerializer{dataType: BytecodeType, writer: bytecodeWriter, logHandler: serializer.logHandler}, nil
	case string:
		return &graphBinaryTypeSerializer{dataType: StringType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, int32(len(value.(string))))
			if err != nil {
				return nil, err
			}
			_, err = buffer.WriteString(value.(string))
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case *big.Int:
		return &graphBinaryTypeSerializer{dataType: BigIntegerType, writer: bigIntWriter, logHandler: serializer.logHandler}, nil
	case int64, int, uint32:
		return &graphBinaryTypeSerializer{dataType: LongType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			switch v := value.(type) {
			case int:
				value = int64(v)
			case uint32:
				value = int64(v)
			}
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case int32, uint16:
		return &graphBinaryTypeSerializer{dataType: IntType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			switch v := value.(type) {
			case uint16:
				value = int32(v)
			}
			err := binary.Write(buffer, binary.BigEndian, value.(int32))
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case int16:
		return &graphBinaryTypeSerializer{dataType: ShortType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value.(int16))
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case uint8:
		return &graphBinaryTypeSerializer{dataType: ByteType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value.(uint8))
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case bool:
		return &graphBinaryTypeSerializer{dataType: BooleanType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value.(bool))
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case uuid.UUID:
		return &graphBinaryTypeSerializer{dataType: UUIDType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case float32:
		return &graphBinaryTypeSerializer{dataType: FloatType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case float64:
		return &graphBinaryTypeSerializer{dataType: DoubleType, writer: func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
			err := binary.Write(buffer, binary.BigEndian, value)
			return buffer.Bytes(), err
		}, logHandler: serializer.logHandler}, nil
	case *Vertex:
		return &graphBinaryTypeSerializer{dataType: VertexType, writer: vertexWriter, logHandler: serializer.logHandler}, nil
	case *Edge:
		return &graphBinaryTypeSerializer{dataType: EdgeType, writer: edgeWriter, logHandler: serializer.logHandler}, nil
	case *Property:
		return &graphBinaryTypeSerializer{dataType: PropertyType, writer: propertyWriter, logHandler: serializer.logHandler}, nil
	case *VertexProperty:
		return &graphBinaryTypeSerializer{dataType: VertexPropertyType, writer: vertexPropertyWriter, logHandler: serializer.logHandler}, nil
	case *Lambda:
		return &graphBinaryTypeSerializer{dataType: LambdaType, writer: lambdaWriter, logHandler: serializer.logHandler}, nil
	case *Path:
		return &graphBinaryTypeSerializer{dataType: PathType, writer: pathWriter, logHandler: serializer.logHandler}, nil
	case Set:
		return &graphBinaryTypeSerializer{dataType: SetType, writer: setWriter, logHandler: serializer.logHandler}, nil
	case time.Time:
		return &graphBinaryTypeSerializer{dataType: DateType, writer: timeWriter, logHandler: serializer.logHandler}, nil
	case time.Duration:
		return &graphBinaryTypeSerializer{dataType: DurationType, writer: durationWriter, logHandler: serializer.logHandler}, nil
	case Cardinality:
		return &graphBinaryTypeSerializer{dataType: CardinalityType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Column:
		return &graphBinaryTypeSerializer{dataType: ColumnType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Direction:
		return &graphBinaryTypeSerializer{dataType: DirectionType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Operator:
		return &graphBinaryTypeSerializer{dataType: OperatorType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Order:
		return &graphBinaryTypeSerializer{dataType: OrderType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Pick:
		return &graphBinaryTypeSerializer{dataType: PickType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Pop:
		return &graphBinaryTypeSerializer{dataType: PopType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case T:
		return &graphBinaryTypeSerializer{dataType: TType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Barrier:
		return &graphBinaryTypeSerializer{dataType: BarrierType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case Scope:
		return &graphBinaryTypeSerializer{dataType: ScopeType, writer: enumWriter, logHandler: serializer.logHandler}, nil
	case p, Predicate:
		return &graphBinaryTypeSerializer{dataType: PType, writer: pWriter, logHandler: serializer.logHandler}, nil
	case textP, TextPredicate:
		return &graphBinaryTypeSerializer{dataType: TextPType, writer: textPWriter, logHandler: serializer.logHandler}, nil
	default:
		switch reflect.TypeOf(val).Kind() {
		case reflect.Map:
			return &graphBinaryTypeSerializer{dataType: MapType, writer: mapWriter, reader: mapReader, logHandler: serializer.logHandler}, nil
		case reflect.Array, reflect.Slice:
			// We can write an array or slice into the list datatype.
			return &graphBinaryTypeSerializer{dataType: ListType, writer: listWriter, reader: listReader, logHandler: serializer.logHandler}, nil
		default:
			serializer.logHandler.logf(Error, serializeDataTypeError, reflect.TypeOf(val).Name())
			return nil, fmt.Errorf("unknown data type to serialize %s", reflect.TypeOf(val).Name())
		}
	}
}

// gets the type of the serializer based on the DataType byte value.
func (serializer *graphBinaryTypeSerializer) getSerializerToRead(typ byte) (*graphBinaryTypeSerializer, error) {
	switch typ {
	case TraverserType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: TraverserType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			traverser := new(Traverser)
			err := binary.Read(buffer, binary.BigEndian, &traverser.bulk)
			if err != nil {
				return nil, err
			}
			traverser.value, err = serializer.read(buffer)
			if err != nil {
				return nil, err
			}
			return traverser, nil
		}, nullFlagReturn: "", logHandler: serializer.logHandler}, nil
	case StringType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: StringType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var size int32
			err := binary.Read(buffer, binary.BigEndian, &size)
			if err != nil {
				return nil, err
			}
			valBytes := make([]byte, size)
			_, err = buffer.Read(valBytes)
			return string(valBytes), err
		}, nullFlagReturn: "", logHandler: serializer.logHandler}, nil
	case BigIntegerType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: BigIntegerType, reader: bigIntReader, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case LongType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: LongType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val int64
			return val, binary.Read(buffer, binary.BigEndian, &val)
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case IntType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: IntType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val int32
			return val, binary.Read(buffer, binary.BigEndian, &val)
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case ShortType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: ShortType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val int16
			return val, binary.Read(buffer, binary.BigEndian, &val)
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case ByteType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: ByteType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val uint8
			err := binary.Read(buffer, binary.BigEndian, &val)
			return val, err
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case BooleanType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: BooleanType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val bool
			err := binary.Read(buffer, binary.BigEndian, &val)
			return val, err
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case UUIDType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: UUIDType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			valBytes := make([]byte, 16)
			_, err := buffer.Read(valBytes)
			if err != nil {
				return uuid.Nil, err
			}
			val, _ := uuid.FromBytes(valBytes)
			return val, nil
		}, nullFlagReturn: uuid.Nil, logHandler: serializer.logHandler}, nil
	case FloatType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: FloatType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val float32
			err := binary.Read(buffer, binary.BigEndian, &val)
			return val, err
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case DoubleType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: DoubleType, reader: func(buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) (interface{}, error) {
			var val float64
			err := binary.Read(buffer, binary.BigEndian, &val)
			return val, err
		}, nullFlagReturn: 0, logHandler: serializer.logHandler}, nil
	case VertexType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: VertexType, reader: vertexReader, nullFlagReturn: Vertex{}, logHandler: serializer.logHandler}, nil
	case EdgeType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: EdgeType, reader: edgeReader, nullFlagReturn: Edge{}, logHandler: serializer.logHandler}, nil
	case PropertyType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: PropertyType, reader: propertyReader, nullFlagReturn: Property{}, logHandler: serializer.logHandler}, nil
	case VertexPropertyType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: VertexPropertyType, reader: vertexPropertyReader, nullFlagReturn: VertexProperty{}, logHandler: serializer.logHandler}, nil
	case PathType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: PathType, reader: pathReader, nullFlagReturn: Path{}, logHandler: serializer.logHandler}, nil
	case SetType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: SetType, reader: setReader, nullFlagReturn: SimpleSet{}, logHandler: serializer.logHandler}, nil
	case ListType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: ListType, reader: listReader, nullFlagReturn: nil, logHandler: serializer.logHandler}, nil
	case DateType.getCodeByte(), TimestampType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: DateType, reader: timeReader, nullFlagReturn: time.Time{}, logHandler: serializer.logHandler}, nil
	case DurationType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: DurationType, reader: durationReader, nullFlagReturn: time.Duration(0), logHandler: serializer.logHandler}, nil
	case MapType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: MapType, reader: mapReader, nullFlagReturn: nil, logHandler: serializer.logHandler}, nil
	case BulkSetType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: BulkSetType, reader: bulkSetReader, nullFlagReturn: nil, logHandler: serializer.logHandler}, nil
	case DirectionType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: DirectionType, reader: enumReader, logHandler: serializer.logHandler}, nil
	case TType.getCodeByte():
		return &graphBinaryTypeSerializer{dataType: BulkSetType, reader: enumReader, nullFlagReturn: nil, logHandler: serializer.logHandler}, nil
	default:
		serializer.logHandler.logf(Error, deserializeDataTypeError, int32(typ))
		return nil, fmt.Errorf("unknown data type to deserialize %x", typ)
	}
}

// Writes an object in fully-qualified format, containing {type_code}{type_info}{value_flag}{value}.
func (serializer *graphBinaryTypeSerializer) write(valueObject interface{}, buffer *bytes.Buffer) (interface{}, error) {
	if valueObject == nil {
		// return Object of type "unspecified object null" with the value flag set to null.
		buffer.Write(nullBytes)
		return buffer.Bytes(), nil
	}

	typeSerializer, err := serializer.getSerializerToWrite(valueObject)
	if err != nil {
		return nil, err
	}
	buffer.Write(typeSerializer.dataType.getCodeBytes())
	message, err := typeSerializer.writeType(valueObject, buffer, typeSerializer)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// Writes a value without including type information.
func (serializer *graphBinaryTypeSerializer) writeValue(value interface{}, buffer *bytes.Buffer, nullable bool) (interface{}, error) {
	if value == nil {
		if !nullable {
			serializer.logHandler.log(Error, unexpectedNull)
			return nil, errors.New("unexpected null value to writeType when nullable is false")
		}
		serializer.writeValueFlagNull(buffer)
		return buffer.Bytes(), nil
	}

	typeSerializer, err := serializer.getSerializerToWrite(value)
	if err != nil {
		return nil, err
	}
	message, err := typeSerializer.writeTypeValue(value, buffer, typeSerializer, nullable)
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

// Reads the type code, information and value of a given buffer with fully-qualified format.
func (serializer *graphBinaryTypeSerializer) read(buffer *bytes.Buffer) (interface{}, error) {
	var typeCode DataType
	err := binary.Read(buffer, binary.BigEndian, &typeCode)
	if err != nil {
		return nil, err
	}
	if typeCode == NullType {
		var isNull byte
		_ = binary.Read(buffer, binary.BigEndian, &isNull)
		if isNull != 1 {
			return nil, errors.New("expected isNull check to be true for NullType")
		}
		return nil, nil
	}

	typeSerializer, err := serializer.getSerializerToRead(byte(typeCode))
	if err != nil {
		return nil, err
	}
	return typeSerializer.readType(buffer, typeSerializer)
}

func (serializer *graphBinaryTypeSerializer) readValue(buffer *bytes.Buffer, typ byte, nullable bool) (interface{}, error) {
	if buffer == nil {
		serializer.logHandler.log(Error, nullInput)
		return nil, errors.New("input cannot be null")
	}
	typeSerializer, _ := serializer.getSerializerToRead(typ)
	val, _ := typeSerializer.readTypeValue(buffer, typeSerializer, nullable)
	return val, nil
}
