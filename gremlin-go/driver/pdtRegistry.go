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

import "reflect"

// PDTAdapter defines how to hydrate/dehydrate a provider-defined type.
type PDTAdapter struct {
	TypeName   string
	FromFields func(map[string]interface{}) (interface{}, error)
	ToFields   func(interface{}) (map[string]interface{}, error)
}

// PrimitivePDTAdapter defines how to hydrate/dehydrate a primitive provider-defined type.
type PrimitivePDTAdapter struct {
	TypeName   string
	FromString func(string) (interface{}, error)
	ToString   func(interface{}) (string, error)
}

// PDTRegistry maps type names to their hydration adapters.
type PDTRegistry struct {
	adaptersByName          map[string]*PDTAdapter
	adaptersByType          map[reflect.Type]*PDTAdapter
	primitiveAdaptersByName map[string]*PrimitivePDTAdapter
	primitiveAdaptersByType map[reflect.Type]*PrimitivePDTAdapter
}

// NewPDTRegistry creates an empty PDTRegistry.
func NewPDTRegistry() *PDTRegistry {
	return &PDTRegistry{
		adaptersByName:          make(map[string]*PDTAdapter),
		adaptersByType:          make(map[reflect.Type]*PDTAdapter),
		primitiveAdaptersByName: make(map[string]*PrimitivePDTAdapter),
		primitiveAdaptersByType: make(map[reflect.Type]*PrimitivePDTAdapter),
	}
}

// RegisterFuncs registers hydration/dehydration functions for a type name.
func (r *PDTRegistry) RegisterFuncs(typeName string, fromFields func(map[string]interface{}) (interface{}, error), toFields func(interface{}) (map[string]interface{}, error)) {
	adapter := &PDTAdapter{TypeName: typeName, FromFields: fromFields, ToFields: toFields}
	r.adaptersByName[typeName] = adapter
}

// RegisterFuncsWithType registers hydration/dehydration functions for a type name and associates a Go type for dehydration lookup.
func (r *PDTRegistry) RegisterFuncsWithType(typeName string, targetType reflect.Type, fromFields func(map[string]interface{}) (interface{}, error), toFields func(interface{}) (map[string]interface{}, error)) {
	adapter := &PDTAdapter{TypeName: typeName, FromFields: fromFields, ToFields: toFields}
	r.adaptersByName[typeName] = adapter
	r.adaptersByType[targetType] = adapter
}

// RegisterType registers a struct type for reflection-based hydration using "pdt" struct tags.
func (r *PDTRegistry) RegisterType(typeName string, targetType reflect.Type) {
	r.adaptersByName[typeName] = &PDTAdapter{
		TypeName: typeName,
		FromFields: func(fields map[string]interface{}) (interface{}, error) {
			obj := reflect.New(targetType).Elem()
			for i := 0; i < targetType.NumField(); i++ {
				field := targetType.Field(i)
				tag := field.Tag.Get("pdt")
				if tag == "" {
					tag = field.Name
				}
				if val, ok := fields[tag]; ok && val != nil {
					obj.Field(i).Set(reflect.ValueOf(val))
				}
			}
			return obj.Interface(), nil
		},
	}
}

// GetAdapterByType returns the adapter registered for the given Go type, or nil.
func (r *PDTRegistry) GetAdapterByType(t reflect.Type) *PDTAdapter {
	return r.adaptersByType[t]
}

// Hydrate converts a CompositePDT into a domain object using the registered adapter.
// Returns the raw PDT if no adapter is found or if hydration fails.
// Nested registered PDTs in Fields are always hydrated recursively, even when the outer has no adapter.
func (r *PDTRegistry) Hydrate(pdt *CompositePDT) interface{} {
	if pdt == nil {
		return nil
	}
	hydratedFields := make(map[string]interface{}, len(pdt.Fields))
	for k, v := range pdt.Fields {
		if nested, ok := v.(*CompositePDT); ok {
			hydratedFields[k] = r.Hydrate(nested)
		} else if nested, ok := v.(*PrimitivePDT); ok {
			hydratedFields[k] = r.HydratePrimitive(nested)
		} else {
			hydratedFields[k] = v
		}
	}
	adapter, ok := r.adaptersByName[pdt.Name]
	if !ok {
		return &CompositePDT{Name: pdt.Name, Fields: hydratedFields}
	}
	result, err := adapter.FromFields(hydratedFields)
	if err != nil {
		return pdt
	}
	return result
}

// RegisterPrimitiveFuncs registers hydration/dehydration functions for a primitive type name.
func (r *PDTRegistry) RegisterPrimitiveFuncs(typeName string, fromString func(string) (interface{}, error), toString func(interface{}) (string, error)) {
	adapter := &PrimitivePDTAdapter{TypeName: typeName, FromString: fromString, ToString: toString}
	r.primitiveAdaptersByName[typeName] = adapter
}

// RegisterPrimitiveFuncsWithType registers hydration/dehydration functions for a primitive type name
// and associates a Go type for dehydration lookup.
func (r *PDTRegistry) RegisterPrimitiveFuncsWithType(typeName string, targetType reflect.Type, fromString func(string) (interface{}, error), toString func(interface{}) (string, error)) {
	adapter := &PrimitivePDTAdapter{TypeName: typeName, FromString: fromString, ToString: toString}
	r.primitiveAdaptersByName[typeName] = adapter
	r.primitiveAdaptersByType[targetType] = adapter
}

// GetPrimitiveAdapterByType returns the primitive adapter registered for the given Go type, or nil.
func (r *PDTRegistry) GetPrimitiveAdapterByType(t reflect.Type) *PrimitivePDTAdapter {
	return r.primitiveAdaptersByType[t]
}

// HydratePrimitive converts a PrimitivePDT into a domain object using the registered primitive adapter.
// Returns the raw PDT if no adapter is found or if hydration fails.
func (r *PDTRegistry) HydratePrimitive(pdt *PrimitivePDT) interface{} {
	if pdt == nil {
		return nil
	}
	adapter, ok := r.primitiveAdaptersByName[pdt.Name]
	if !ok {
		return pdt
	}
	result, err := adapter.FromString(pdt.Value)
	if err != nil {
		return pdt
	}
	return result
}
