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
	"fmt"
	"unicode"
)

// GValue is a variable or literal value that is used in a Traversal. It is composed of a key-value pair where the key
// is the name given to the variable and the value is the object that the variable resolved to.
type GValue struct {
	name  string
	value interface{}
}

// NewGValue creates a new GValue to be used in traversals. The name must be non-empty, start with a
// Unicode letter, and contain only Unicode letters, digits, or '_'. It cannot begin with "_".
func NewGValue(name string, value interface{}) GValue {
	runes := []rune(name)
	if len(runes) > 0 && runes[0] == '_' {
		panic(fmt.Sprintf("invalid GValue name '%v'. Should not start with _.", name))
	}
	if !isValidGValueName(name) {
		panic(fmt.Sprintf("invalid GValue name '%v'.", name))
	}
	return GValue{name, value}
}

func isValidGValueName(name string) bool {
	runes := []rune(name)
	if len(runes) == 0 || !unicode.IsLetter(runes[0]) {
		return false
	}
	for _, r := range runes[1:] {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' {
			return false
		}
	}
	return true
}

// Name returns the name of the GValue.
func (gv GValue) Name() string {
	return gv.name
}

// IsNil determines if the value held is of a nil value.
func (gv GValue) IsNil() bool {
	return gv.value == nil
}

// Value returns the value held by the GValue.
func (gv GValue) Value() interface{} {
	return gv.value
}

// String returns the string representation of the GValue in the format "name=value".
func (gv GValue) String() string {
	return fmt.Sprintf("%v=%v", gv.name, gv.value)
}
