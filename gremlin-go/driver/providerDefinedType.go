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
	"io"
)

// ProviderDefinedType represents a provider-defined type (PDT) in GraphBinary serialization.
type ProviderDefinedType struct {
	Name       string
	Properties map[string]interface{}
}

func (p *ProviderDefinedType) String() string {
	return fmt.Sprintf("pdt[%s]%v", p.Name, p.Properties)
}

// pdtWriter serializes a ProviderDefinedType as a fully-qualified string (name) followed by a fully-qualified map (properties).
func pdtWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	pdt := value.(*ProviderDefinedType)
	if err := typeSerializer.write(pdt.Name, w); err != nil {
		return err
	}
	if pdt.Properties == nil {
		return typeSerializer.write(map[interface{}]interface{}{}, w)
	}
	m := make(map[interface{}]interface{}, len(pdt.Properties))
	for k, v := range pdt.Properties {
		m[k] = v
	}
	return typeSerializer.write(m, w)
}