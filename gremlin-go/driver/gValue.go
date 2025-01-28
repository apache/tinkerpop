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
	"strings"
)

// GValue is a variable or literal value that is used in a Traversal. It is composed of a key-value pair where the key
// is the name given to the variable and the value is the object that the variable resolved to.
type GValue interface {
	Name() string
	IsNil() bool
	Value() interface{}
}

type gValue struct {
	name  string
	value interface{}
}

// NewGValue creates a new GValue to be used in traversals. The GValue name cannot begin with "_".
func NewGValue(name string, value interface{}) GValue {
	if strings.HasPrefix(name, "_") {
		panic(fmt.Sprintf("invalid GValue name '%v'. Should not start with _.", name))
	}
	return &gValue{name, value}
}

// Name returns the name of the GValue.
func (gv *gValue) Name() string {
	return gv.name
}

// IsNil determines if the value held is of a nil value.
func (gv *gValue) IsNil() bool {
	return gv.value == nil
}

// Value returns the value held by the GValue.
func (gv *gValue) Value() interface{} {
	return gv.value
}
