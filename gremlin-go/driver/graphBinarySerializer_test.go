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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

// failingWriter is an io.Writer that fails after writing a specified number of bytes.
// Used to test error propagation through the serialization chain.
// Feature: serializer-writer-refactor, Property 4: Writer Error Propagation
type failingWriter struct {
	failAfter int // Number of bytes to write before failing
	written   int // Number of bytes written so far
}

func (w *failingWriter) Write(p []byte) (n int, err error) {
	if w.written >= w.failAfter {
		return 0, errors.New("simulated write failure")
	}
	// Calculate how many bytes we can write before failing
	remaining := w.failAfter - w.written
	if len(p) <= remaining {
		w.written += len(p)
		return len(p), nil
	}
	// Partial write then fail
	w.written += remaining
	return remaining, errors.New("simulated write failure")
}

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
			err := stringWriter(str, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readString()
			assert.Nil(t, err)
			assert.Equal(t, str, res)
		})
		t.Run("read-write bool", func(t *testing.T) {
			var buffer bytes.Buffer
			f := func(value interface{}, buffer *bytes.Buffer, typeSerializer *graphBinaryTypeSerializer) error {
				return binary.Write(buffer, binary.BigEndian, value.(bool))
			}
			err := f(false, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			b, err := d.readByte()
			assert.Nil(t, err)
			assert.False(t, b != 0)

			buffer.Reset()
			err = f(true, &buffer, nil)
			assert.Nil(t, err)
			d = NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			b, err = d.readByte()
			assert.Nil(t, err)
			assert.True(t, b != 0)
		})
		t.Run("read-write BigDecimal", func(t *testing.T) {
			var buffer bytes.Buffer
			source := &BigDecimal{11, big.NewInt(int64(22))}
			err := bigDecimalWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigDecimal()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write int", func(t *testing.T) {
			var buffer bytes.Buffer
			source := int32(123)
			err := intWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readInt32()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write short", func(t *testing.T) {
			var buffer bytes.Buffer
			source := int16(123)
			err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			buf, err := d.readBytes(2)
			assert.Nil(t, err)
			res := int16(binary.BigEndian.Uint16(buf))
			assert.Equal(t, source, res)
		})
		t.Run("read-write short int8", func(t *testing.T) {
			var buffer bytes.Buffer
			source := int8(123)
			err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			buf, err := d.readBytes(2)
			assert.Nil(t, err)
			res := int16(binary.BigEndian.Uint16(buf))
			assert.Equal(t, int16(source), res)
		})
		t.Run("read-write long", func(t *testing.T) {
			var buffer bytes.Buffer
			source := 123
			err := longWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readInt64()
			assert.Nil(t, err)
			assert.Equal(t, int64(source), res)
		})
		t.Run("read-write bigInt", func(t *testing.T) {
			var buffer bytes.Buffer
			source := big.NewInt(123)
			err := bigIntWriter(*source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigInt()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write bigInt uint64", func(t *testing.T) {
			var buffer bytes.Buffer
			source := uint64(123)
			err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigInt()
			assert.Nil(t, err)
			assert.Equal(t, new(big.Int).SetUint64(source), res)
		})
		t.Run("read-write bigInt uint", func(t *testing.T) {
			var buffer bytes.Buffer
			source := uint(123)
			err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readBigInt()
			assert.Nil(t, err)
			assert.Equal(t, new(big.Int).SetUint64(uint64(source)), res)
		})
		t.Run("read-write list", func(t *testing.T) {
			var buffer bytes.Buffer
			source := []interface{}{int32(111), "str"}
			serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
			err := listWriter(source, &buffer, &serializer)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readList(false)
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write byteBuffer", func(t *testing.T) {
			var buffer bytes.Buffer
			source := &ByteBuffer{[]byte{byte(127), byte(255)}}
			err := byteBufferWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readByteBuffer()
			assert.Nil(t, err)
			assert.Equal(t, source, res)
		})
		t.Run("read-write set", func(t *testing.T) {
			var buffer bytes.Buffer
			source := NewSimpleSet(int32(111), "str")
			serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
			err := setWriter(source, &buffer, &serializer)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			list, err := d.readList(false)
			assert.Nil(t, err)
			res := NewSimpleSet(list.([]interface{})...)
			assert.Equal(t, source, res)
		})
		t.Run("read-write map", func(t *testing.T) {
			var buffer bytes.Buffer
			source := map[interface{}]interface{}{int32(1): "s1", "s2": int32(2), nil: nil}
			serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
			err := mapWriter(source, &buffer, &serializer)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readMap()
			assert.Nil(t, err)
			assert.Equal(t, fmt.Sprintf("%v", source), fmt.Sprintf("%v", res))
		})
		t.Run("read incomparable map: a map value as the key", func(t *testing.T) {
			// prepare test data
			var buf = &bytes.Buffer{}
			typeSerializer := &graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
			// write the size of map
			err := binary.Write(buf, binary.BigEndian, uint32(1))
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			// write a map value as the key
			k1 := map[string]string{"key": "value"}
			err = typeSerializer.write(reflect.ValueOf(k1).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}
			v1 := "value1"
			err = typeSerializer.write(reflect.ValueOf(v1).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}

			d := NewGraphBinaryDeserializer(bytes.NewReader(buf.Bytes()))
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
			typeSerializer := &graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}
			// write the size of map
			err := binary.Write(buf, binary.BigEndian, uint32(1))
			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}
			// write a slice value as the key
			k2 := []int{1, 2, 3}
			err = typeSerializer.write(reflect.ValueOf(k2).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}
			v2 := "value2"
			err = typeSerializer.write(reflect.ValueOf(v2).Interface(), buf)
			if err != nil {
				t.Fatalf("Failed to encode data: %v", err)
			}

			d := NewGraphBinaryDeserializer(bytes.NewReader(buf.Bytes()))
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
			err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
		t.Run("read-write local datetime", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 0, 0, time.Local)
			err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
		t.Run("read-write UTC datetime", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 0, 0, time.UTC)
			err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
		t.Run("read-write HST datetime", func(t *testing.T) {
			var buffer bytes.Buffer
			source := time.Date(2022, 5, 10, 9, 51, 34, 123456789, GetTimezoneFromOffset(-36000))
			err := dateTimeWriter(source, &buffer, nil)
			assert.Nil(t, err)
			d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
			res, err := d.readDateTime()
			assert.Nil(t, err)
			// ISO format
			assert.Equal(t, source.Format(time.RFC3339Nano), res.Format(time.RFC3339Nano))
		})
	})

	t.Run("read-write marker", func(t *testing.T) {
		var buffer bytes.Buffer
		source := EndOfStream()
		err := markerWriter(source, &buffer, nil)
		assert.Nil(t, err)
		d := NewGraphBinaryDeserializer(bytes.NewReader(buffer.Bytes()))
		b, err := d.readByte()
		assert.Nil(t, err)
		res, err := Of(b)
		assert.Nil(t, err)
		assert.Equal(t, source, res)
	})

}

// TestWriterErrorPropagation tests that errors from io.Writer are properly propagated
// through the serialization chain.
// Feature: serializer-writer-refactor, Property 4: Writer Error Propagation
// **Validates: Requirements 7.1, 7.4**
func TestWriterErrorPropagation(t *testing.T) {
	serializer := graphBinaryTypeSerializer{newLogHandler(&defaultLogger{}, Error, language.English)}

	t.Run("error propagates from stringWriter", func(t *testing.T) {
		// String "hello" requires: 4 bytes for length + 5 bytes for content = 9 bytes
		// Fail after 2 bytes (during length write)
		w := &failingWriter{failAfter: 2}
		err := stringWriter("hello", w, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from stringWriter during content write", func(t *testing.T) {
		// String "hello" requires: 4 bytes for length + 5 bytes for content = 9 bytes
		// Fail after 6 bytes (during content write)
		w := &failingWriter{failAfter: 6}
		err := stringWriter("hello", w, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from intWriter", func(t *testing.T) {
		// int32 requires 4 bytes
		// Fail after 2 bytes
		w := &failingWriter{failAfter: 2}
		err := intWriter(int32(42), w, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from longWriter", func(t *testing.T) {
		// int64 requires 8 bytes
		// Fail after 4 bytes
		w := &failingWriter{failAfter: 4}
		err := longWriter(int64(42), w, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from listWriter", func(t *testing.T) {
		// List with one int32 element requires:
		// 4 bytes for length + (1 byte type code + 1 byte value flag + 4 bytes int value) = 10 bytes
		// Fail after 2 bytes (during length write)
		w := &failingWriter{failAfter: 2}
		err := listWriter([]interface{}{int32(1)}, w, &serializer)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from listWriter during element write", func(t *testing.T) {
		// List with one int32 element requires:
		// 4 bytes for length + (1 byte type code + 1 byte value flag + 4 bytes int value) = 10 bytes
		// Fail after 6 bytes (during element write)
		w := &failingWriter{failAfter: 6}
		err := listWriter([]interface{}{int32(1)}, w, &serializer)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from mapWriter", func(t *testing.T) {
		// Map with one entry requires:
		// 4 bytes for length + key bytes + value bytes
		// Fail after 2 bytes (during length write)
		w := &failingWriter{failAfter: 2}
		err := mapWriter(map[interface{}]interface{}{int32(1): "v"}, w, &serializer)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from graphBinaryTypeSerializer.write", func(t *testing.T) {
		// Writing a fully-qualified int32 requires:
		// 1 byte type code + 1 byte value flag + 4 bytes value = 6 bytes
		// Fail immediately
		w := &failingWriter{failAfter: 0}
		err := serializer.write(int32(42), w)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from graphBinaryTypeSerializer.write during value", func(t *testing.T) {
		// Writing a fully-qualified int32 requires:
		// 1 byte type code + 1 byte value flag + 4 bytes value = 6 bytes
		// Fail after 3 bytes (during value write)
		w := &failingWriter{failAfter: 3}
		err := serializer.write(int32(42), w)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from graphBinaryTypeSerializer.writeValue", func(t *testing.T) {
		// Writing a non-nullable int32 value requires:
		// 4 bytes value
		// Fail after 2 bytes
		w := &failingWriter{failAfter: 2}
		err := serializer.writeValue(int32(42), w, false)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from graphBinaryTypeSerializer.writeValue nullable", func(t *testing.T) {
		// Writing a nullable int32 value requires:
		// 1 byte value flag + 4 bytes value = 5 bytes
		// Fail immediately (during value flag write)
		w := &failingWriter{failAfter: 0}
		err := serializer.writeValue(int32(42), w, true)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from bigIntWriter", func(t *testing.T) {
		// BigInt requires: 4 bytes for length + variable bytes for value
		// Fail after 2 bytes (during length write)
		w := &failingWriter{failAfter: 2}
		err := bigIntWriter(big.NewInt(12345), w, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("error propagates from dateTimeWriter", func(t *testing.T) {
		// DateTime requires: 4 bytes year + 1 byte month + 1 byte day + 8 bytes nanoseconds + 4 bytes offset = 18 bytes
		// Fail after 2 bytes (during year write)
		w := &failingWriter{failAfter: 2}
		err := dateTimeWriter(time.Now(), w, nil)
		assert.NotNil(t, err)
		assert.Contains(t, err.Error(), "simulated write failure")
	})

	t.Run("successful write with sufficient buffer", func(t *testing.T) {
		// Verify that writes succeed when the writer has enough capacity
		w := &failingWriter{failAfter: 100}
		err := stringWriter("hello", w, nil)
		assert.Nil(t, err)
		// String "hello" = 4 bytes length + 5 bytes content = 9 bytes
		assert.Equal(t, 9, w.written)
	})

	t.Run("successful write of complex type with sufficient buffer", func(t *testing.T) {
		// Verify that complex type writes succeed when the writer has enough capacity
		w := &failingWriter{failAfter: 100}
		err := serializer.write([]interface{}{int32(1), "test"}, w)
		assert.Nil(t, err)
		// List type code (1) + value flag (1) + length (4) +
		// int32: type code (1) + value flag (1) + value (4) +
		// string: type code (1) + value flag (1) + length (4) + "test" (4)
		// Total = 22 bytes
		assert.Equal(t, 22, w.written)
	})
}
