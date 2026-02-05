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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

func TestGraphBinaryV4(t *testing.T) {
	t.Run("graphBinaryTypeSerializer tests", func(t *testing.T) {
		serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}

		t.Run("getType should return correct type for simple type", func(t *testing.T) {
			res, err := serializer.getType(int32(1))
			assert.Nil(t, err)
			assert.Equal(t, intType, res)
		})
		t.Run("getType should return correct type for complex type", func(t *testing.T) {
			res, err := serializer.getType([]byte{1, 2, 3})
			assert.Nil(t, err)
			assert.Equal(t, listType, res)
		})
		t.Run("getType should return error for missing type", func(t *testing.T) {
			_, err := serializer.getType(Error)
			assert.NotNil(t, err)
		})

		t.Run("getWriter should return correct func for dataType", func(t *testing.T) {
			writer, err := serializer.getWriter(intType)
			assert.Nil(t, err)
			assert.Equal(t, reflect.ValueOf(intWriter).Pointer(), reflect.ValueOf(writer).Pointer())
		})
		t.Run("getWriter should return error for missing dataType", func(t *testing.T) {
			_, err := serializer.getWriter(nullType)
			assert.NotNil(t, err)
		})

		t.Run("getSerializerToWrite should return correct func for int", func(t *testing.T) {
			writer, dataType, err := serializer.getSerializerToWrite(int32(1))
			assert.Nil(t, err)
			assert.Equal(t, intType, dataType)
			assert.Equal(t, reflect.ValueOf(intWriter).Pointer(), reflect.ValueOf(writer).Pointer())
		})
		t.Run("getSerializerToWrite should return error for missing dataType", func(t *testing.T) {
			_, _, err := serializer.getSerializerToWrite(nullType)
			assert.NotNil(t, err)
		})
	})

	t.Run("read-write tests", func(t *testing.T) {
		t.Run("read-write string", func(t *testing.T) {
			var buffer bytes.Buffer
			str := "test string"
			_, err := stringWriter(str, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readString()
			assert.Nil(t, err)
			assert.Equal(t, str, res)
		})
		t.Run("read-write bool", func(t *testing.T) {
			var buffer bytes.Buffer
			f := func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) ([]byte, error) {
				err := binary.Write(buffer, binary.BigEndian, value.(bool))
				return buffer.Bytes(), err
			}
			_, err := f(false, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			b, err := d.readByte()
			assert.Nil(t, err)
			assert.False(t, b != 0)

			buffer.Reset()
			_, err = f(true, &buffer, nil)
			assert.Nil(t, err)
			d = NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			b, err = d.readByte()
			assert.Nil(t, err)
			assert.True(t, b != 0)
		})
		t.Run("read-write BigDecimal", func(t *testing.T) {
			var buffer bytes.Buffer
			source := &BigDecimal{11, big.NewInt(int64(22))}
			_, err := bigDecimalWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigDecimal()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write int", func(t *testing.T) {
			var buffer bytes.Buffer
			source := int32(123)
			_, err := intWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readInt32()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write short", func(t *testing.T) {
			var buffer bytes.Buffer
			source := int16(123)
			_, err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			buf, err := d.readBytes(2)
			assert.Nil(t, err)
			res := int16(binary.BigEndian.Uint16(buf))
			assert.Equal(t, source, res)
		})
		t.Run("read-write short int8", func(t *testing.T) {
			var buffer bytes.Buffer
			source := int8(123)
			_, err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			buf, err := d.readBytes(2)
			assert.Nil(t, err)
			res := int16(binary.BigEndian.Uint16(buf))
			assert.Equal(t, int16(source), res)
		})
		t.Run("read-write long", func(t *testing.T) {
			var buffer bytes.Buffer
			source := 123
			_, err := longWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readInt64()
			assert.Nil(t, err)
			assert.Equal(t, int64(source), res)
		})
		t.Run("read-write bigInt", func(t *testing.T) {
			var buffer bytes.Buffer
			source := big.NewInt(123)
			_, err := bigIntWriter(*source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigInt()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write bigInt uint64", func(t *testing.T) {
			var buffer bytes.Buffer
			source := uint64(123)
			_, err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigInt()
			assert.Nil(t, err)
			assert.Equal(t, new(big.Int).SetUint64(source), res)
		})
		t.Run("read-write bigInt uint", func(t *testing.T) {
			var buffer bytes.Buffer
			source := uint(123)
			_, err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigInt()
			assert.Nil(t, err)
			assert.Equal(t, new(big.Int).SetUint64(uint64(source)), res)
		})
		t.Run("read-write list", func(t *testing.T) {
			var buffer bytes.Buffer
			source := []interface{}{int32(111), "str"}
			_, err := listWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readList(false)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write byteBuffer", func(t *testing.T) {
			var buffer bytes.Buffer
			source := &ByteBuffer{[]byte{byte(127), byte(255)}}
			_, err := byteBufferWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readByteBuffer()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write set", func(t *testing.T) {
			var buffer bytes.Buffer
			source := NewSimpleSet(int32(111), "str")
			_, err := setWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			list, err := d.readList(false)
			assert.Nil(t, err)
			res := NewSimpleSet(list.([]interface{})...)
			assert.Equal(t, source, res)
		})
		t.Run("read-write map", func(t *testing.T) {
			var buffer bytes.Buffer
			source := map[interface{}]interface{}{int32(1): "s1", "s2": int32(2), nil: nil}
			_, err := mapWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readMap()
			assert.Nil(t, err)
			assert.Equal(t, fmt.Sprintf("%v", source), fmt.Sprintf("%v", res))
		})
		t.Run("read incomparable map: a map value as the key", func(t *testing.T) {
			// prepare test data
			var buf = &bytes.Buffer{}
			typeSerializer := &graphBinaryTypeSerializer{}
			// write the size of map
			err := binary.Write(buf, binary.BigEndian, uint32(1))
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			// write a map value as the key
			k1 := map[string]string{"key": "value"}
			_, err = typeSerializer.write(reflect.ValueOf(k1).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}
			v1 := "value1"
			_, err = typeSerializer.write(reflect.ValueOf(v1).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}

			d := NewStreamingDeserializer(bytes.NewReader(buf.Bytes()))
			result, err := d.readMap()
			if err != nil {
				t.Fatalf("readMap failed: %v", err)
			}
			mResult, ok := result.(map[interface{}]interface{})
			if !ok {
				t.Fatalf("readMap result not map[interface{}]interface{}")
			}
			for k, v := range mResult {
				assert.Equal(t, reflect.Ptr, reflect.TypeOf(k).Kind())
				assert.Equal(t, "value1", v)
			}
		})
		t.Run("read incomparable map: a slice value as the key", func(t *testing.T) {
			// prepare test data
			var buf = &bytes.Buffer{}
			typeSerializer := &graphBinaryTypeSerializer{}
			// write the size of map
			err := binary.Write(buf, binary.BigEndian, uint32(1))
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			// write a slice value as the key
			k2 := []int{1, 2, 3}
			_, err = typeSerializer.write(reflect.ValueOf(k2).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}
			v2 := "value2"
			_, err = typeSerializer.write(reflect.ValueOf(v2).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}

			d := NewStreamingDeserializer(bytes.NewReader(buf.Bytes()))
			result, err := d.readMap()
			if err != nil {
				t.Fatalf("readMap failed: %v", err)
			}
			expected := map[interface{}]interface{}{
				"[1 2 3]": "value2",
			}
			if !reflect.DeepEqual(result, expected) {
				t.Errorf("Expected %v, but got %v", expected, result)
			}
		})
		t.Run("read-write time", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 0, 0, time.Local)
			_, err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
		t.Run("read-write local datetime", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 0, 0, time.Local)
			_, err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
		t.Run("read-write UTC datetime", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 0, 0, time.UTC)
			_, err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
		t.Run("read-write HST datetime", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 34, 123456789, GetTimezoneFromOffset(-36000))
			_, err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
	})

	t.Run("read-write marker", func(t *testing.T) {
		var buffer bytes.Buffer
		source := EndOfStream()
		_, err := markerWriter(source, &buffer, nil)
		assert.Nil(t, err)
		d := NewStreamingDeserializer(bytes.NewReader(buffer.Bytes()))
		b, err := d.readByte()
		assert.Nil(t, err)
		res, err := Of(b)
		assert.Nil(t, err)
		assert.Equal(t, source, res)
	})

}
