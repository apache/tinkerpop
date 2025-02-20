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
	"math"
	"math/big"
	"reflect"
	"time"

	"github.com/google/uuid"
)

// Version 1.0

// dataType graphBinary types.
type dataType uint8

// dataType defined as constants.
const (
	customType         dataType = 0x00
	intType            dataType = 0x01
	longType           dataType = 0x02
	stringType         dataType = 0x03
	datetimeType       dataType = 0x04
	doubleType         dataType = 0x07
	floatType          dataType = 0x08
	listType           dataType = 0x09
	mapType            dataType = 0x0a
	setType            dataType = 0x0b
	uuidType           dataType = 0x0c
	edgeType           dataType = 0x0d
	pathType           dataType = 0x0e
	propertyType       dataType = 0x0f
	vertexType         dataType = 0x11
	vertexPropertyType dataType = 0x12
	directionType      dataType = 0x18
	tType              dataType = 0x20
	bigDecimalType     dataType = 0x22
	bigIntegerType     dataType = 0x23
	byteType           dataType = 0x24
	byteBuffer         dataType = 0x25
	shortType          dataType = 0x26
	booleanType        dataType = 0x27
	mergeType          dataType = 0x2e
	durationType       dataType = 0x81
	markerType         dataType = 0xfd
	nullType           dataType = 0xFE
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

// Format: {Id}{Label}{properties}
func vertexWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	v := value.(*Vertex)
	_, err := typeSerializer.write(v.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue([1]string{v.Label}, buffer, false)
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
	_, err = typeSerializer.writeValue([1]string{e.Label}, buffer, false)
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

// Format: {Key}{Value}{parent}
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

// Format: {Id}{Label}{Value}{parent}{properties}
func vertexPropertyWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	vp := value.(*VertexProperty)
	_, err := typeSerializer.write(vp.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue([1]string{vp.Label}, buffer, false)
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

// Format: {Labels}{Objects}
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
	err := binary.Write(buffer, binary.BigEndian, int32(t.Year()))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buffer, binary.BigEndian, byte(t.Month()))
	if err != nil {
		return nil, err
	}
	err = binary.Write(buffer, binary.BigEndian, byte(t.Day()))
	if err != nil {
		return nil, err
	}
	// construct time of day in nanoseconds
	h := t.Hour()
	m := t.Minute()
	s := t.Second()
	ns := (h * 60 * 60 * 1e9) + (m * 60 * 1e9) + (s * 1e9) + t.Nanosecond()
	err = binary.Write(buffer, binary.BigEndian, int64(ns))
	if err != nil {
		return nil, err
	}
	_, os := t.Zone()
	err = binary.Write(buffer, binary.BigEndian, int32(os))
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

func enumWriter(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
	_, err := typeSerializer.write(reflect.ValueOf(value).String(), buffer)
	return buffer.Bytes(), err
}

func markerWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
	m := value.(Marker)
	err := binary.Write(buffer, binary.BigEndian, m.GetValue())
	return buffer.Bytes(), err
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

const (
	valueFlagNull byte = 1
	valueFlagNone byte = 0
)

func (serializer *graphBinaryTypeSerializer) getType(val interface{}) (dataType, error) {
	switch val.(type) {
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
	case *Path:
		return pathType, nil
	case Set:
		return setType, nil
	case time.Time:
		return datetimeType, nil
	case time.Duration:
		return durationType, nil
	case direction:
		return directionType, nil
	case t:
		return tType, nil
	case merge:
		return mergeType, nil
	case *BigDecimal, BigDecimal:
		return bigDecimalType, nil
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
	case datetimeType:
		return time.Time{}
	case durationType:
		return time.Duration(0)
	default:
		return nil
	}
}

// Composite
func readList(data *[]byte, i *int, flag byte) (interface{}, error) {
	sz := readIntSafe(data, i)
	var valList []interface{}
	if flag == 0x02 {
		for j := int32(0); j < sz; j++ {
			val, err := readFullyQualifiedNullable(data, i, true)
			if err != nil {
				return nil, err
			}
			bulk := readIntSafe(data, i)
			for k := int32(0); k < bulk; k++ {
				valList = append(valList, val)
			}
		}
	} else {
		for j := int32(0); j < sz; j++ {
			val, err := readFullyQualifiedNullable(data, i, true)
			if err != nil {
				return nil, err
			}
			valList = append(valList, val)
		}
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
		} else if reflect.TypeOf(k).Comparable() {
			mapData[k] = v
		} else {
			switch reflect.TypeOf(k).Kind() {
			case reflect.Map:
				mapData[&k] = v
			default:
				mapData[fmt.Sprint(k)] = v
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
			return nil, newError(err0703ReadMapNonStringKeyError, keyDataType)
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

func readSet(data *[]byte, i *int, flag byte) (interface{}, error) {
	list, err := readList(data, i, flag)
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
	year := readIntSafe(data, i)
	month := readByteSafe(data, i)
	day := readByteSafe(data, i)
	ns := readLongSafe(data, i)
	offset := readIntSafe(data, i)
	// only way to pass offset info, timezone display is fixed to UTC as consequence (offset is calculated properly)
	loc := time.FixedZone("UTC", int(offset))
	datetime := time.Date(int(year), time.Month(month), int(day), 0, 0, 0, int(ns), loc)
	return datetime, nil
}

func durationReader(data *[]byte, i *int) (interface{}, error) {
	return time.Duration(readLongSafe(data, i)*int64(time.Second) + int64(readIntSafe(data, i))), nil
}

// Graph

// {fully qualified id}{unqualified label}
func vertexReader(data *[]byte, i *int) (interface{}, error) {
	return vertexReaderReadingProperties(data, i, true)
}

// {fully qualified id}{unqualified label}{fully qualified properties}
func vertexReaderReadingProperties(data *[]byte, i *int, readProperties bool) (interface{}, error) {
	var err error
	v := new(Vertex)
	v.Id, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	label, err := readUnqualified(data, i, listType, false)
	if err != nil {
		return nil, err
	}
	v.Label = label.([]interface{})[0].(string)
	if readProperties {
		props, err := readFullyQualifiedNullable(data, i, true)
		if err != nil {
			return nil, err
		}
		// null properties are returned as empty slices
		v.Properties = make([]interface{}, 0)
		if props != nil {
			v.Properties = props
		}
	}
	return v, nil
}

// {fully qualified id}{unqualified label}{in vertex w/o null byte}{out vertex}{unused null byte}{fully qualified properties}
func edgeReader(data *[]byte, i *int) (interface{}, error) {
	var err error
	e := new(Edge)
	e.Id, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	label, err := readUnqualified(data, i, listType, false)
	if err != nil {
		return nil, err
	}
	e.Label = label.([]interface{})[0].(string)
	v, err := vertexReaderReadingProperties(data, i, false)
	if err != nil {
		return nil, err
	}
	e.InV = *v.(*Vertex)
	v, err = vertexReaderReadingProperties(data, i, false)
	if err != nil {
		return nil, err
	}
	e.OutV = *v.(*Vertex)
	*i += 2
	props, err := readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}
	// null properties are returned as empty slices
	e.Properties = make([]interface{}, 0)
	if props != nil {
		e.Properties = props
	}
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
	label, err := readUnqualified(data, i, listType, false)
	if err != nil {
		return nil, err
	}
	vp.Label = label.([]interface{})[0].(string)
	vp.Value, err = readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}

	*i += 2

	props, err := readFullyQualifiedNullable(data, i, true)
	if err != nil {
		return nil, err
	}

	// null properties are returned as empty slices
	vp.Properties = make([]interface{}, 0)
	if props != nil {
		vp.Properties = props
	}

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

// {type code (always string so ignore)}{nil code (always false so ignore)}{int32 size}{string enum}
func enumReader(data *[]byte, i *int) (interface{}, error) {
	typeCode := readDataType(data, i)
	if typeCode != stringType {
		return nil, newError(err0406EnumReaderInvalidTypeError)
	}
	*i++
	return readString(data, i)
}

func markerReader(data *[]byte, i *int) (interface{}, error) {
	m, err := Of(readByteSafe(data, i))
	if err != nil {
		return nil, err
	}
	return m, nil
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
		flag := readByteSafe(data, i)
		if flag == valueFlagNull {
			return getDefaultValue(dataTyp), nil
		}
		if dataTyp == listType {
			return readList(data, i, flag)
		}
		if dataTyp == setType {
			return readSet(data, i, flag)
		}
	}
	deserializer, ok := deserializers[dataTyp]
	if !ok {
		return nil, newError(err0408GetSerializerToReadUnknownTypeError, dataTyp)
	}

	return deserializer(data, i)
}

// {name}{type specific payload}
func customTypeReader(data *[]byte, i *int) (interface{}, error) {
	// we need to decrement the index by 1 to be read the 32-bit int with the size of the string
	*i = *i - 1
	customTypeName, err := readString(data, i)
	if err != nil {
		return nil, err
	}

	// grab a read lock for the map of deserializers, since these can be updated out-of-band
	customTypeReaderLock.RLock()
	defer customTypeReaderLock.RUnlock()
	deserializer, ok := customDeserializers[customTypeName.(string)]
	if !ok {
		return nil, newError(err0409GetSerializerToReadUnknownCustomTypeError, customTypeName)
	}
	return deserializer(data, i)
}
