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
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPDTRegistryRegisterFuncsAndHydrate(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterFuncs("x:Point", func(props map[string]interface{}) (interface{}, error) {
		return [2]int{props["x"].(int), props["y"].(int)}, nil
	}, nil)

	pdt := &ProviderDefinedType{Name: "x:Point", Fields: map[string]interface{}{"x": 1, "y": 2}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, [2]int{1, 2}, result)
}

func TestPDTRegistryNoAdapterReturnsRawPDT(t *testing.T) {
	reg := NewPDTRegistry()
	pdt := &ProviderDefinedType{Name: "x:Unknown", Fields: map[string]interface{}{"a": "b"}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, pdt, result)
}

func TestPDTRegistryAdapterErrorReturnsRawPDT(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterFuncs("x:Bad", func(props map[string]interface{}) (interface{}, error) {
		return nil, errors.New("fail")
	}, nil)

	pdt := &ProviderDefinedType{Name: "x:Bad", Fields: map[string]interface{}{}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, pdt, result)
}

func TestPDTRegistryNestedHydration(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterFuncs("x:Inner", func(props map[string]interface{}) (interface{}, error) {
		return props["val"].(string) + "!", nil
	}, nil)
	reg.RegisterFuncs("x:Outer", func(props map[string]interface{}) (interface{}, error) {
		return "outer:" + props["child"].(string), nil
	}, nil)

	inner := &ProviderDefinedType{Name: "x:Inner", Fields: map[string]interface{}{"val": "hi"}}
	outer := &ProviderDefinedType{Name: "x:Outer", Fields: map[string]interface{}{"child": inner}}
	result := reg.Hydrate(outer)
	assert.Equal(t, "outer:hi!", result)
}

type testPoint struct {
	X int    `pdt:"x"`
	Y int    `pdt:"y"`
	L string // no tag, uses field name
}

func TestPDTRegistryRegisterType(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterType("x:Point", reflect.TypeOf(testPoint{}))

	pdt := &ProviderDefinedType{Name: "x:Point", Fields: map[string]interface{}{"x": 3, "y": 4, "L": "label"}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, testPoint{X: 3, Y: 4, L: "label"}, result)
}

func TestPDTRegistryHydrateNil(t *testing.T) {
	reg := NewPDTRegistry()
	assert.Nil(t, reg.Hydrate(nil))
}
