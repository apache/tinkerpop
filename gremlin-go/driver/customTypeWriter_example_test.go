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

// An external test package, so the example compiles only against the exported API.
package gremlingo_test

import (
	"bytes"
	"reflect"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

// Point is an example provider-specific type.
type Point struct {
	Name string
	X, Y int32
}

func ExampleRegisterCustomTypeWriter() {
	pointWriter := func(value interface{}, buffer *bytes.Buffer, writer gremlingo.GraphBinaryWriter) error {
		p := value.(*Point)
		if err := writer.WriteValue(p.Name, buffer, false); err != nil {
			return err
		}
		if err := writer.WriteValue(p.X, buffer, false); err != nil {
			return err
		}
		return writer.WriteValue(p.Y, buffer, false)
	}

	gremlingo.RegisterCustomTypeWriter(reflect.TypeOf((*Point)(nil)), "example.Point", pointWriter)
}
