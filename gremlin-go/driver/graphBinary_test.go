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
	"fmt"
	"math/big"
	"testing"

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
				assert.Equal(t, x, readToValue(&buff).(uint8) != 0)
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
	})

	t.Run("test nested types", func(t *testing.T) {
		buff := bytes.Buffer{}
		t.Run("test map", func(t *testing.T) {
			var x int32 = 666
			var m = map[interface{}]interface{}{
				"marko": x,
				"noone": "blah",
			}
			writeToBuffer(m, &buff)
			assert.Equal(t, m, readToValue(&buff))
		})
		t.Run("test slice", func(t *testing.T) {
			var x = []interface{}{"a", "b", "c"}
			writeToBuffer(x, &buff)
			assert.Equal(t, x, readToValue(&buff))
		})
	})

	t.Run("temp test to printout serialized and deserialized data", func(t *testing.T) {
		var x = []interface{}{"a", "b", "c"}
		//var y int32 = 100
		//var z int64 = 100
		//var s = "serialize this!"
		//var u, _ = uuid.Parse("41d2e28a-20a4-4ab0-b379-d810dede3786")
		//var a int64 = 666
		//var m = map[interface{}]interface{}{
		//	"marko": a,
		//	"noone": "blah",
		//}
		buff := bytes.Buffer{}
		writeToBuffer(x, &buff)
		fmt.Println(buff.Bytes())
		res := readToValue(&buff)
		assert.Equal(t, x, res)
		fmt.Println("expected: ", res)
		fmt.Println("result: ", res)
	})
}
