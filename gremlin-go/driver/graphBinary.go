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
	gTypeType          dataType = 0x30
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
	_, err = typeSerializer.writeValue([1]string{e.InV.Label}, buffer, false)
	if err != nil {
		return nil, err
	}
	// Write out-vertex
	_, err = typeSerializer.write(e.OutV.Id, buffer)
	if err != nil {
		return nil, err
	}

	// Not fully qualified.
	_, err = typeSerializer.writeValue([1]string{e.OutV.Label}, buffer, false)
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

func dateTimeWriter(value interface{}, buffer *bytes.Buffer, _ *graphBinaryTypeSerializer) ([]byte, error) {
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
	case gType:
		return gTypeType, nil
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

// GetTimezoneFromOffset is a helper function to convert an offset in seconds to a time.Location
func GetTimezoneFromOffset(offsetSeconds int) *time.Location {
	// calculate hours and minutes from seconds
	hours := offsetSeconds / 3600
	minutes := (offsetSeconds % 3600) / 60

	// format the timezone name in the format that go expects
	// for example: "UTC+01:00" or "UTC-05:30"
	sign := "+"
	if hours < 0 {
		sign = "-"
		hours = -hours
		minutes = -minutes
	}
	tzName := fmt.Sprintf("UTC%s%02d:%02d", sign, hours, minutes)

	return time.FixedZone(tzName, offsetSeconds)
}
