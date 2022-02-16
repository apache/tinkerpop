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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResult(t *testing.T) {

	t.Run("Test Result.AsString() string", func(t *testing.T) {
		r := Result{"foo"}
		assert.Equal(t, r.AsString(), "foo")
	})

	t.Run("Test Result.AsString() slice", func(t *testing.T) {
		r := Result{[]int{1, 2, 3}}
		assert.Equal(t, r.AsString(), "[1 2 3]")

	})

	t.Run("Test Result.AsString() int", func(t *testing.T) {
		r := Result{1}
		assert.Equal(t, r.AsString(), "1")

	})

	t.Run("Test Result.AsString() float", func(t *testing.T) {
		r := Result{1.2}
		assert.Equal(t, r.AsString(), "1.2")

	})
}
