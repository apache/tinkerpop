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
	reg.RegisterFuncs("x:Point", func(fields map[string]interface{}) (interface{}, error) {
		return [2]int{fields["x"].(int), fields["y"].(int)}, nil
	}, nil)

	pdt := &CompositePDT{Name: "x:Point", Fields: map[string]interface{}{"x": 1, "y": 2}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, [2]int{1, 2}, result)
}

func TestPDTRegistryNoAdapterReturnsRawPDT(t *testing.T) {
	reg := NewPDTRegistry()
	pdt := &CompositePDT{Name: "x:Unknown", Fields: map[string]interface{}{"a": "b"}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, pdt, result)
}

func TestPDTRegistryAdapterErrorReturnsRawPDT(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterFuncs("x:Bad", func(fields map[string]interface{}) (interface{}, error) {
		return nil, errors.New("fail")
	}, nil)

	pdt := &CompositePDT{Name: "x:Bad", Fields: map[string]interface{}{}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, pdt, result)
}

func TestPDTRegistryNestedHydration(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterFuncs("x:Inner", func(fields map[string]interface{}) (interface{}, error) {
		return fields["val"].(string) + "!", nil
	}, nil)
	reg.RegisterFuncs("x:Outer", func(fields map[string]interface{}) (interface{}, error) {
		return "outer:" + fields["child"].(string), nil
	}, nil)

	inner := &CompositePDT{Name: "x:Inner", Fields: map[string]interface{}{"val": "hi"}}
	outer := &CompositePDT{Name: "x:Outer", Fields: map[string]interface{}{"child": inner}}
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

	pdt := &CompositePDT{Name: "x:Point", Fields: map[string]interface{}{"x": 3, "y": 4, "L": "label"}}
	result := reg.Hydrate(pdt)
	assert.Equal(t, testPoint{X: 3, Y: 4, L: "label"}, result)
}

func TestPDTRegistryHydrateNil(t *testing.T) {
	reg := NewPDTRegistry()
	assert.Nil(t, reg.Hydrate(nil))
}

// TestPDTRegistryNestedHydration_UnregisteredOuter asserts that a registered inner PDT
// is hydrated even when the outer PDT has no registered adapter (desired contract).
func TestPDTRegistryNestedHydration_UnregisteredOuter(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterFuncs("x:Inner", func(fields map[string]interface{}) (interface{}, error) {
		return [2]int{fields["x"].(int), fields["y"].(int)}, nil
	}, nil)

	inner := &CompositePDT{Name: "x:Inner", Fields: map[string]interface{}{"x": 10, "y": 20}}
	outer := &CompositePDT{Name: "x:Unregistered", Fields: map[string]interface{}{"loc": inner, "label": "test"}}

	result := reg.Hydrate(outer)

	// The outer should remain a raw *CompositePDT (no adapter registered for it).
	pdt, ok := result.(*CompositePDT)
	assert.True(t, ok, "outer must remain *CompositePDT, got %T", result)
	assert.Equal(t, "x:Unregistered", pdt.Name)

	// The inner field "loc" must be hydrated to [2]int{10,20} because x:Inner IS registered.
	assert.Equal(t, [2]int{10, 20}, pdt.Fields["loc"], "inner registered PDT must be hydrated even inside unregistered outer")
	// Non-PDT fields remain unchanged.
	assert.Equal(t, "test", pdt.Fields["label"])
}

func TestPDTRegistryPrimitiveRegisterFuncsAndHydrate(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterPrimitiveFuncs("x:Uint32", func(s string) (interface{}, error) {
		return "uint32:" + s, nil
	}, nil)

	pdt := &PrimitivePDT{Name: "x:Uint32", Value: "42"}
	result := reg.HydratePrimitive(pdt)
	assert.Equal(t, "uint32:42", result)
}

func TestPDTRegistryPrimitiveNoAdapterReturnsRawPDT(t *testing.T) {
	reg := NewPDTRegistry()
	pdt := &PrimitivePDT{Name: "x:Unknown", Value: "val"}
	result := reg.HydratePrimitive(pdt)
	assert.Equal(t, pdt, result)
}

func TestPDTRegistryPrimitiveAdapterErrorReturnsRawPDT(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterPrimitiveFuncs("x:Bad", func(s string) (interface{}, error) {
		return nil, errors.New("fail")
	}, nil)

	pdt := &PrimitivePDT{Name: "x:Bad", Value: "val"}
	result := reg.HydratePrimitive(pdt)
	assert.Equal(t, pdt, result)
}

func TestPDTRegistryPrimitiveHydrateNil(t *testing.T) {
	reg := NewPDTRegistry()
	assert.Nil(t, reg.HydratePrimitive(nil))
}

func TestPDTRegistryPrimitiveInsideComposite(t *testing.T) {
	reg := NewPDTRegistry()
	reg.RegisterPrimitiveFuncs("x:Uint32", func(s string) (interface{}, error) {
		return "uint32:" + s, nil
	}, nil)

	inner := &PrimitivePDT{Name: "x:Uint32", Value: "7"}
	outer := &CompositePDT{Name: "x:Outer", Fields: map[string]interface{}{"id": inner}}
	result := reg.Hydrate(outer)

	pdt, ok := result.(*CompositePDT)
	assert.True(t, ok)
	assert.Equal(t, "uint32:7", pdt.Fields["id"])
}

func TestPDTRegistryPrimitiveWithType(t *testing.T) {
	type myID string
	reg := NewPDTRegistry()
	reg.RegisterPrimitiveFuncsWithType("x:MyID", reflect.TypeOf(myID("")),
		func(s string) (interface{}, error) {
			return myID(s), nil
		},
		func(obj interface{}) (string, error) {
			return string(obj.(myID)), nil
		})

	adapter := reg.GetPrimitiveAdapterByType(reflect.TypeOf(myID("")))
	assert.NotNil(t, adapter)
	assert.Equal(t, "x:MyID", adapter.TypeName)
}
