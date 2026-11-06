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

// CompositePDT represents a composite provider-defined type (PDT) in GraphBinary serialization.
type CompositePDT struct {
	Name   string
	Fields map[string]interface{}
}

func (p *CompositePDT) String() string {
	return fmt.Sprintf("pdt[%s]%v", p.Name, p.Fields)
}

// pdtWriter serializes a CompositePDT as a fully-qualified string (name) followed by a fully-qualified map (fields).
func pdtWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	pdt := value.(*CompositePDT)
	if err := typeSerializer.write(pdt.Name, w); err != nil {
		return err
	}
	if pdt.Fields == nil {
		return typeSerializer.write(map[interface{}]interface{}{}, w)
	}
	m := make(map[interface{}]interface{}, len(pdt.Fields))
	for k, v := range pdt.Fields {
		m[k] = v
	}
	return typeSerializer.write(m, w)
}

// PrimitivePDT represents a primitive provider-defined type (PDT) in GraphBinary serialization.
// Wire format 0xf1: two fully-qualified Strings {name}{value}.
type PrimitivePDT struct {
	Name  string
	Value string
}

func (p *PrimitivePDT) String() string {
	return fmt.Sprintf("pdt[%s]%s", p.Name, p.Value)
}

// primitivePdtWriter serializes a PrimitivePDT as two fully-qualified strings.
func primitivePdtWriter(value interface{}, w io.Writer, typeSerializer *graphBinaryTypeSerializer) error {
	pdt := value.(*PrimitivePDT)
	if err := typeSerializer.write(pdt.Name, w); err != nil {
		return err
	}
	return typeSerializer.write(pdt.Value, w)
}
