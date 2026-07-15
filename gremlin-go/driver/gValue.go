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
)

// GValue is a variable or literal value that is used in a Traversal. It is composed of a key-value
// pair where Name is the name given to the variable and Value is the object that the variable
// resolves to. Construct one directly with a struct literal, e.g. GValue{Name: "x", Value: 1}.
// A GValue's Value must not itself be a GValue (GValues cannot be nested).
type GValue struct {
	Name  string
	Value interface{}
}

// IsNil determines if the value held is of a nil value.
func (gv GValue) IsNil() bool {
	return gv.Value == nil
}

// String returns the string representation of the GValue in the format "name=value".
func (gv GValue) String() string {
	return fmt.Sprintf("%v=%v", gv.Name, gv.Value)
}
