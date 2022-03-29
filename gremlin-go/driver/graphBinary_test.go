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
	"math/big"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"golang.org/x/text/language"
)

func writeToBuffer(value interface{}, buffer *bytes.Buffer) []byte {
	logHandler := newLogHandler(&defaultLogger{}, Info, language.English)
	serializer := graphBinaryTypeSerializer{NullType, nil, nil, nil, logHandler}
	val, err := serializer.write(value, buffer)
	if err != nil {
		panic(err)
	}
	return val.([]byte)
}

func readToValue(buff *bytes.Buffer) interface{} {
	logHandler := newLogHandler(&defaultLogger{}, Info, language.English)
	serializer := graphBinaryTypeSerializer{NullType, nil, nil, nil, logHandler}
	val, err := serializer.read(buff)
	if err != nil {
		panic(err)
	}
	return val
}

func TestGraphBinaryV1(t *testing.T) {
	var m = map[interface{}]interface{}{
		"marko": int32(666),
		"none":  "blah",
	}

	t.Run("test simple types", func(t *testing.T) {
		buff := bytes.Buffer{}
		t.Run("test int", func(t *testing.T) {
			var intArr = [3]int{-33000, 0, 33000}
			for _, x := range intArr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, int(readToValue(&buff).(int64)))
			}
		})
		t.Run("test bool", func(t *testing.T) {
			var boolArr = [2]bool{true, false}
			for _, x := range boolArr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test byte(uint8)", func(t *testing.T) {
			var uint8Arr = [2]uint8{0, 255}
			for _, x := range uint8Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test short(int16)", func(t *testing.T) {
			var int16Arr = [3]int16{-32768, 0, 32767}
			for _, x := range int16Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test int(uint16)", func(t *testing.T) {
			var uint16Arr = [2]uint16{0, 65535}
			for _, x := range uint16Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, uint16(readToValue(&buff).(int32)))
			}
		})
		t.Run("test int(int32)", func(t *testing.T) {
			var int32Arr = [3]int32{-2147483648, 0, 2147483647}
			for _, x := range int32Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test long(uint32)", func(t *testing.T) {
			var uint32Arr = [2]uint32{0, 4294967295}
			for _, x := range uint32Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, uint32(readToValue(&buff).(int64)))
			}
		})
		t.Run("test long(int64)", func(t *testing.T) {
			var int64Arr = [3]int64{-21474836489, 0, 21474836489}
			for _, x := range int64Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test float(float32)", func(t *testing.T) {
			var float32Arr = [2]float32{1, 0.375}
			for _, x := range float32Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test double(float64)", func(t *testing.T) {
			var float64Arr = [3]float64{1, 0.00390625, 0.1}
			for _, x := range float64Arr {
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff))
			}
		})
		t.Run("test bigint(big.Int)", func(t *testing.T) {
			// Examples from Tinkerpop IO reference
			var bigIntArr = [7]*big.Int{big.NewInt(0), big.NewInt(1), big.NewInt(127), big.NewInt(128), big.NewInt(-1), big.NewInt(-128), big.NewInt(-129)}
			t.Run("test reader and writer", func(t *testing.T) {
				for _, x := range bigIntArr {
					writeToBuffer(x, &buff)
					assert.Equal(t, x, readToValue(&buff))
				}
			})
			t.Run("test getSignedBytesFromBigInt and getBigIntFromSignedBytes func", func(t *testing.T) {
				var byteArr = [][]byte{{}, {0x01}, {0x7f}, {0x00, 0x80}, {0xff}, {0x80}, {0xff, 0x7f}}
				for i := 0; i < len(bigIntArr); i++ {
					assert.Equal(t, byteArr[i], getSignedBytesFromBigInt(bigIntArr[i]))
					assert.Equal(t, bigIntArr[i], getBigIntFromSignedBytes(byteArr[i]))
				}
			})
		})
		t.Run("test string", func(t *testing.T) {
			var x = "serialize this!"
			writeToBuffer(x, &buff)
			assert.Equal(t, x, readToValue(&buff))
		})
		t.Run("test uuid", func(t *testing.T) {
			var x, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
			writeToBuffer(x, &buff)
			assert.Equal(t, x, readToValue(&buff))
		})

		t.Run("test Graph Types", func(t *testing.T) {
			t.Run("test vertex", func(t *testing.T) {
				x := new(Vertex)
				x.Id, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
				x.Label = "Test Label"
				writeToBuffer(x, &buff)
				v := readToValue(&buff).(*Vertex)
				assert.Equal(t, x, v)
				assert.Equal(t, x.Id, v.Id)
				assert.Equal(t, x.Label, v.Label)
			})
			t.Run("test edge", func(t *testing.T) {
				x := new(Edge)
				x.Id, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
				x.Label = "Test edge Label"
				v1 := new(Vertex)
				v2 := new(Vertex)
				v1.Id, _ = uuid.Parse("8a591c22-8a06-11ec-a8a3-0242ac120002")
				v1.Label = "Test v1 Label"
				v2.Id, _ = uuid.Parse("1274f224-8a08-11ec-a8a3-0242ac120002")
				v2.Label = "Test v2 Label"
				x.InV = *v1
				x.OutV = *v2
				writeToBuffer(x, &buff)
				e := readToValue(&buff).(*Edge)
				assert.Equal(t, x, e)
				assert.Equal(t, x.Id, e.Id)
				assert.Equal(t, x.Label, e.Label)
				assert.Equal(t, x.InV, e.InV)
				assert.Equal(t, x.OutV, e.OutV)
			})
			t.Run("test property", func(t *testing.T) {
				x := new(Property)
				x.Key = "TestKey"
				x.Value = m
				writeToBuffer(x, &buff)
				p := readToValue(&buff).(*Property)
				assert.Equal(t, x, p)
				assert.Equal(t, x.Key, p.Key)
				assert.Equal(t, x.Value, p.Value)
			})
			t.Run("test Vertex property", func(t *testing.T) {
				x := new(VertexProperty)
				x.Id, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
				x.Label = "Test Label"
				x.Value = m
				writeToBuffer(x, &buff)
				v := readToValue(&buff).(*VertexProperty)
				assert.Equal(t, x, v)
				assert.Equal(t, x.Id, v.Id)
				assert.Equal(t, x.Label, v.Label)
				assert.Equal(t, x.Value, v.Value)
			})
			t.Run("test path", func(t *testing.T) {
				x := new(Path)
				l1 := NewSimpleSet("str1", "str2", "str3")
				l2 := NewSimpleSet("str4")
				x.Labels = []Set{l1, l2}
				x.Objects = []interface{}{"String1", m}
				writeToBuffer(x, &buff)
				p := readToValue(&buff).(*Path)
				assert.Equal(t, x, p)
				assert.Equal(t, x.Labels, p.Labels)
				assert.Equal(t, x.Objects, p.Objects)
			})
		})

		t.Run("Test Time types", func(t *testing.T) {
			t.Run("Test DateType/TimestampType", func(t *testing.T) {
				x := time.Now()
				writeToBuffer(x, &buff)
				assert.Equal(t, x.UnixMilli(), readToValue(&buff).(time.Time).UnixMilli())
			})
			t.Run("Test DurationType", func(t *testing.T) {
				x := time.Duration(1e9 + 1)
				writeToBuffer(x, &buff)
				assert.Equal(t, x, readToValue(&buff).(time.Duration))
			})
		})
	})

	t.Run("test nested types", func(t *testing.T) {
		buff := bytes.Buffer{}
		t.Run("test map", func(t *testing.T) {
			t.Run("test map read/write", func(t *testing.T) {
				writeToBuffer(m, &buff)
				assert.Equal(t, m, readToValue(&buff))
			})
		})
		t.Run("test slice", func(t *testing.T) {
			x := []interface{}{"a", "b", "c"}
			writeToBuffer(x, &buff)
			assert.Equal(t, x, readToValue(&buff))
		})
		t.Run("test SimpleSet", func(t *testing.T) {
			x := NewSimpleSet("a", "b", "c", "a", int32(1), int32(2), int32(3), int32(2), int32(3), int32(3))
			writeToBuffer(x, &buff)
			assert.Equal(t, x, readToValue(&buff))
		})
		t.Run("test BulkSet", func(t *testing.T) {
			buff.Write([]byte{0x2a, 00, 00, 00, 00, 02, 01, 00, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 00, 02, 02, 00, 00, 00, 00, 00, 00, 00, 00, 03, 00, 00, 00, 00, 00, 00, 00, 03})
			e := []interface{}{int32(1), int32(1), int64(3), int64(3), int64(3)}
			assert.Equal(t, e, readToValue(&buff))
		})
		t.Run("test Binding", func(t *testing.T) {
			x := &Binding{
				Key:   "key",
				Value: "value",
			}
			writeToBuffer(x, &buff)
			assert.Equal(t, x, readToValue(&buff))
		})
	})

	t.Run("test misc cases", func(t *testing.T) {
		buff := bytes.Buffer{}
		logHandler := newLogHandler(&defaultLogger{}, Info, language.English)
		serializer := graphBinaryTypeSerializer{NullType, nil, nil, nil, logHandler}

		t.Run("test bytecode writer failure", func(t *testing.T) {
			x, err := bytecodeWriter(int64(0), &buff, &serializer)
			assert.Nil(t, x)
			assert.NotNil(t, err)
		})
		t.Run("test unknown datatype to serialize failure", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("The code did not panic with an unknown datatype")
				}
			}()
			var x Unknown
			writeToBuffer(x, &buff)
		})

		t.Run("test unknown datatype to deserialize failure", func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("The code did not panic with an unknown datatype")
				}
			}()
			buff.Write([]byte{0xff})
			readToValue(&buff)
		})

		t.Run("test writeValue nil case failure", func(t *testing.T) {
			var value interface{} = nil
			val, err := serializer.writeValue(value, &buff, false)
			assert.Nil(t, val)
			assert.NotNil(t, err)
		})

		t.Run("test writeValue nil case success", func(t *testing.T) {
			var value interface{} = nil
			val, err := serializer.writeValue(value, &buff, true)
			assert.Nil(t, err)
			assert.NotNil(t, val)
			x, err := serializer.readTypeValue(&buff, &serializer, true)
			assert.Nil(t, err)
			assert.Equal(t, nil, x)
		})

		t.Run("test write nil case success", func(t *testing.T) {
			var value interface{} = nil
			val, err := serializer.write(value, &buff)
			assert.Nil(t, err)
			assert.NotNil(t, val)
			x, err := serializer.read(&buff)
			assert.Nil(t, err)
			assert.Equal(t, value, x)
		})

		t.Run("test read nil case isNull check failure", func(t *testing.T) {
			buff.Write([]byte{0xfe, 0x02})
			x, err := serializer.read(&buff)
			assert.Nil(t, x)
			assert.NotNil(t, err)
		})

		t.Run("test null buffer failure", func(t *testing.T) {
			var nullBuff *bytes.Buffer = nil
			x, err := serializer.readValue(nullBuff, byte(NullType), true)
			assert.Nil(t, x)
			assert.NotNil(t, err)
		})

		t.Run("test writeTypeValue unexpected null failure", func(t *testing.T) {
			var value interface{} = nil
			val, err := serializer.writeTypeValue(value, &buff, &serializer, false)
			assert.Nil(t, val)
			assert.NotNil(t, err)
		})

		t.Run("test writeTypeValue success", func(t *testing.T) {
			var value interface{} = nil
			val, err := serializer.writeTypeValue(value, &buff, &serializer, true)
			assert.Nil(t, err)
			assert.NotNil(t, val)
			x, err := serializer.readTypeValue(&buff, &serializer, true)
			assert.Nil(t, err)
			assert.Equal(t, x, value)
		})
	})
}

type Unknown struct{}
