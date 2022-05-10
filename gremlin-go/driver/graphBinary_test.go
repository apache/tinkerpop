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
	"github.com/stretchr/testify/assert"
	"math/big"
	"testing"
)

func TestGraphBinaryV1(t *testing.T) {

	t.Run("test simple types", func(t *testing.T) {

		t.Run("read-write string", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			str := "test string"
			buf, err := stringWriter(str, &buffer, nil)
			assert.Nil(t, err)
			res := readString(&buf, &pos)
			assert.Equal(t, str, res)
		})
		t.Run("read-write int", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := int32(123)
			buf, err := intWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res := readInt(&buf, &pos)
			assert.Equal(t, source, res)
		})
		t.Run("read-write short", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := int16(123)
			buf, err := shortWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res := readShort(&buf, &pos)
			assert.Equal(t, source, res)
		})
		t.Run("read-write long", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := 123
			buf, err := longWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res := readLong(&buf, &pos)
			assert.Equal(t, int64(source), res)
		})
		t.Run("read-write bigInt", func(t *testing.T) {
			pos := 0
			var buffer bytes.Buffer
			source := big.NewInt(123)
			buf, err := bigIntWriter(source, &buffer, nil)
			assert.Nil(t, err)
			res := readBigInt(&buf, &pos)
			assert.Equal(t, source, res)
		})
	})
}
