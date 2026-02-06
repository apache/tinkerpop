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
	"encoding/binary"
	"fmt"
	"io"
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

func (serializer *graphBinaryTypeSerializer) writeType(value interface{}, w io.Writer, writer writer) error {
	return serializer.writeTypeValue(value, w, writer, true)
}

func (serializer *graphBinaryTypeSerializer) writeTypeValue(value interface{}, w io.Writer, writer writer, nullable bool) error {
	if value == nil {
		if !nullable {
			serializer.logHandler.log(Error, unexpectedNull)
			return newError(err0401WriteTypeValueUnexpectedNullError)
		}
		return serializer.writeValueFlagNull(w)
	}
	if nullable {
		if err := serializer.writeValueFlagNone(w); err != nil {
			return err
		}
	}
	return writer(value, w, serializer)
}

// Format: {length}{item_0}...{item_n}
func listWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	v := reflect.ValueOf(value)
	valLen := v.Len()
	if err := binary.Write(w, binary.BigEndian, int32(valLen)); err != nil {
		return err
	}
	for i := 0; i < valLen; i++ {
		if err := typeSerializer.write(v.Index(i).Interface(), w); err != nil {
			return err
		}
	}
	return nil
}

// Format: {length}{value}
func byteBufferWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	var v ByteBuffer
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*ByteBuffer))
	} else {
		v = value.(ByteBuffer)
	}

	if err := binary.Write(w, binary.BigEndian, int32(len(v.Data))); err != nil {
		return err
	}
	_, err := w.Write(v.Data)
	return err
}

// Format: {length}{item_0}...{item_n}
// Item format: {type_code}{type_info}{value_flag}{value}
func mapWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	if value == nil {
		return typeSerializer.writeValue(int32(0), w, false)
	}

	v := reflect.ValueOf(value)
	keys := v.MapKeys()
	if err := binary.Write(w, binary.BigEndian, int32(len(keys))); err != nil {
		return err
	}
	for _, k := range keys {
		convKey := k.Convert(v.Type().Key())
		// serialize k
		if err := typeSerializer.write(k.Interface(), w); err != nil {
			return err
		}
		// serialize v.MapIndex(c_key)
		val := v.MapIndex(convKey)
		if err := typeSerializer.write(val.Interface(), w); err != nil {
			return err
		}
	}
	return nil
}

func stringWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	str := value.(string)
	if err := binary.Write(w, binary.BigEndian, int32(len(str))); err != nil {
		return err
	}
	_, err := io.WriteString(w, str)
	return err
}

func longWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	switch v := value.(type) {
	case int:
		value = int64(v)
	case uint32:
		value = int64(v)
	}
	return binary.Write(w, binary.BigEndian, value)
}

func intWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	switch v := value.(type) {
	case uint16:
		value = int32(v)
	}
	return binary.Write(w, binary.BigEndian, value.(int32))
}

func shortWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	switch v := value.(type) {
	case int8:
		value = int16(v)
	}
	return binary.Write(w, binary.BigEndian, value.(int16))
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
func bigIntWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
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
	if err := binary.Write(w, binary.BigEndian, int32(len(signedBytes))); err != nil {
		return err
	}
	_, err := w.Write(signedBytes)
	return err
}

// Format: {scale}{unscaled_value}
func bigDecimalWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	var v BigDecimal
	if reflect.TypeOf(value).Kind() == reflect.Ptr {
		v = *(value.(*BigDecimal))
	} else {
		v = value.(BigDecimal)
	}
	if err := binary.Write(w, binary.BigEndian, v.Scale); err != nil {
		return err
	}

	return bigIntWriter(v.UnscaledValue, w, typeSerializer)
}

// Format: {Id}{Label}{properties}
func vertexWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	v := value.(*Vertex)
	if err := typeSerializer.write(v.Id, w); err != nil {
		return err
	}

	// Not fully qualified.
	if err := typeSerializer.writeValue([1]string{v.Label}, w, false); err != nil {
		return err
	}
	// Note that as TinkerPop currently send "references" only, properties will always be null
	_, err := w.Write(nullBytes)
	return err
}

// Format: {Id}{Label}{inVId}{inVLabel}{outVId}{outVLabel}{parent}{properties}
func edgeWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	e := value.(*Edge)
	if err := typeSerializer.write(e.Id, w); err != nil {
		return err
	}

	// Not fully qualified
	if err := typeSerializer.writeValue([1]string{e.Label}, w, false); err != nil {
		return err
	}

	// Write in-vertex
	if err := typeSerializer.write(e.InV.Id, w); err != nil {
		return err
	}

	// Not fully qualified.
	if err := typeSerializer.writeValue([1]string{e.InV.Label}, w, false); err != nil {
		return err
	}
	// Write out-vertex
	if err := typeSerializer.write(e.OutV.Id, w); err != nil {
		return err
	}

	// Not fully qualified.
	if err := typeSerializer.writeValue([1]string{e.OutV.Label}, w, false); err != nil {
		return err
	}

	// Note that as TinkerPop currently send "references" only, parent and properties will always be null
	if _, err := w.Write(nullBytes); err != nil {
		return err
	}
	_, err := w.Write(nullBytes)
	return err
}

// Format: {Key}{Value}{parent}
func propertyWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	v := value.(*Property)

	// Not fully qualified.
	if err := typeSerializer.writeValue(v.Key, w, false); err != nil {
		return err
	}

	if err := typeSerializer.write(v.Value, w); err != nil {
		return err
	}
	// Note that as TinkerPop currently send "references" only, parent and properties  will always be null
	_, err := w.Write(nullBytes)
	return err
}

// Format: {Id}{Label}{Value}{parent}{properties}
func vertexPropertyWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	vp := value.(*VertexProperty)
	if err := typeSerializer.write(vp.Id, w); err != nil {
		return err
	}

	// Not fully qualified.
	if err := typeSerializer.writeValue([1]string{vp.Label}, w, false); err != nil {
		return err
	}
	if err := typeSerializer.write(vp.Value, w); err != nil {
		return err
	}
	// Note that as TinkerPop currently send "references" only, parent and properties will always be null
	if _, err := w.Write(nullBytes); err != nil {
		return err
	}
	_, err := w.Write(nullBytes)
	return err
}

// Format: {Labels}{Objects}
func pathWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	p := value.(*Path)
	if err := typeSerializer.write(p.Labels, w); err != nil {
		return err
	}
	return typeSerializer.write(p.Objects, w)
}

// Format: Same as List.
// Mostly similar to listWriter with small changes
func setWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	slice := value.(Set).ToSlice()
	return listWriter(slice, w, typeSerializer)
}

func dateTimeWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	t := value.(time.Time)
	if err := binary.Write(w, binary.BigEndian, int32(t.Year())); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, byte(t.Month())); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, byte(t.Day())); err != nil {
		return err
	}
	// construct time of day in nanoseconds
	h := t.Hour()
	m := t.Minute()
	s := t.Second()
	ns := (h * 60 * 60 * 1e9) + (m * 60 * 1e9) + (s * 1e9) + t.Nanosecond()
	if err := binary.Write(w, binary.BigEndian, int64(ns)); err != nil {
		return err
	}
	_, os := t.Zone()
	return binary.Write(w, binary.BigEndian, int32(os))
}

func durationWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	t := value.(time.Duration)
	sec := int64(t / time.Second)
	nanos := int32(t % time.Second)
	if err := binary.Write(w, binary.BigEndian, sec); err != nil {
		return err
	}
	return binary.Write(w, binary.BigEndian, nanos)
}

func enumWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	return typeSerializer.write(reflect.ValueOf(value).String(), w)
}

func markerWriter(value interface{}, w io.Writer, _ *graphBinaryTypeSerializer) error {
	m := value.(Marker)
	return binary.Write(w, binary.BigEndian, m.GetValue())
}

// Format: {strategy_class}{configuration}
func traversalStrategyWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	ts := value.(*traversalStrategy)

	if err := typeSerializer.writeValue(ts.name, w, false); err != nil {
		return err
	}

	return mapWriter(ts.configuration, w, typeSerializer)
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
func (serializer *graphBinaryTypeSerializer) write(valueObject interface{}, w io.Writer) error {
	if valueObject == nil {
		// return Object of type "unspecified object null" with the value flag set to null.
		_, err := w.Write(nullBytes)
		return err
	}

	writer, dataType, err := serializer.getSerializerToWrite(valueObject)
	if err != nil {
		return err
	}
	if _, err := w.Write(dataType.getCodeBytes()); err != nil {
		return err
	}
	return serializer.writeType(valueObject, w, writer)
}

// Writes a value without including type information.
func (serializer *graphBinaryTypeSerializer) writeValue(value interface{}, w io.Writer, nullable bool) error {
	if value == nil {
		if !nullable {
			serializer.logHandler.log(Error, unexpectedNull)
			return newError(err0403WriteValueUnexpectedNullError)
		}
		return serializer.writeValueFlagNull(w)
	}

	writer, _, err := serializer.getSerializerToWrite(value)
	if err != nil {
		return err
	}
	return serializer.writeTypeValue(value, w, writer, nullable)
}

func (serializer *graphBinaryTypeSerializer) writeValueFlagNull(w io.Writer) error {
	_, err := w.Write([]byte{valueFlagNull})
	return err
}

func (serializer *graphBinaryTypeSerializer) writeValueFlagNone(w io.Writer) error {
	_, err := w.Write([]byte{valueFlagNone})
	return err
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
