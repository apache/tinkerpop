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
	"reflect"
)

// Bytecode a list of ordered instructions for traversal that can be serialized between environments and machines.
type Bytecode struct {
	sourceInstructions []instruction
	stepInstructions   []instruction
	bindings           map[string]interface{}
}

// NewBytecode creates a new Bytecode to be used in traversals.
func NewBytecode(bc *Bytecode) *Bytecode {
	sourceInstructions := make([]instruction, 0)
	stepInstructions := make([]instruction, 0)
	bindingMap := make(map[string]interface{})
	if bc != nil {
		sourceInstructions = append(sourceInstructions, bc.sourceInstructions...)
		stepInstructions = append(stepInstructions, bc.stepInstructions...)
	}

	return &Bytecode{
		sourceInstructions: sourceInstructions,
		stepInstructions:   stepInstructions,
		bindings:           bindingMap,
	}
}

func (bytecode *Bytecode) createInstruction(operator string, args ...interface{}) (*instruction, error) {
	instruction := &instruction{
		operator:  operator,
		arguments: make([]interface{}, 0),
	}

	for _, arg := range args {
		converted, err := bytecode.convertArgument(arg)
		if err != nil {
			return nil, err
		}
		instruction.arguments = append(instruction.arguments, converted)
	}

	return instruction, nil
}

// AddSource add a traversal source instruction to the bytecode.
func (bytecode *Bytecode) AddSource(sourceName string, args ...interface{}) error {
	instruction, err := bytecode.createInstruction(sourceName, args...)
	if err != nil {
		return err
	}

	bytecode.sourceInstructions = append(bytecode.sourceInstructions, *instruction)
	return err
}

// AddStep adds a traversal instruction to the bytecode
func (bytecode *Bytecode) AddStep(stepName string, args ...interface{}) error {
	instruction, err := bytecode.createInstruction(stepName, args...)
	if err != nil {
		return err
	}

	bytecode.stepInstructions = append(bytecode.stepInstructions, *instruction)
	return err
}

func (bytecode *Bytecode) convertArgument(arg interface{}) (interface{}, error) {
	if arg == nil {
		return nil, nil
	}
	t := reflect.TypeOf(arg)
	if t == nil {
		return nil, nil
	}
	switch t.Kind() {
	case reflect.Map:
		newMap := make(map[interface{}]interface{})
		iter := reflect.ValueOf(arg).MapRange()
		for iter.Next() {
			k := iter.Key().Interface()
			v := iter.Value().Interface()
			convertedKey, err := bytecode.convertArgument(k)
			if err != nil {
				return nil, err
			}
			convertedValue, err := bytecode.convertArgument(v)
			if err != nil {
				return nil, err
			}
			newMap[convertedKey] = convertedValue
		}
		return newMap, nil
	case reflect.Slice:
		newSlice := make([]interface{}, 0)
		oldSlice := reflect.ValueOf(arg)
		for i := 0; i < oldSlice.Len(); i++ {
			converted, err := bytecode.convertArgument(oldSlice.Index(i).Interface())
			if err != nil {
				return nil, err
			}
			newSlice = append(newSlice, converted)
		}
		return newSlice, nil
	default:
		switch v := arg.(type) {
		case *Binding:
			convertedValue, err := bytecode.convertArgument(v.Value)
			if err != nil {
				return nil, err
			}
			bytecode.bindings[v.Key] = v.Value
			return &Binding{
				Key:   v.Key,
				Value: convertedValue,
			}, nil
		case *GraphTraversal:
			if v.graph != nil {
				return nil, newError(err1001ConvertArgumentChildTraversalNotFromAnonError)
			}
			for k, val := range v.Bytecode.bindings {
				bytecode.bindings[k] = val
			}
			return v.Bytecode, nil
		default:
			return arg, nil
		}
	}

}

type instruction struct {
	operator  string
	arguments []interface{}
}

// Binding associates a string variable with a value
type Binding struct {
	Key   string
	Value interface{}
}

// String returns the key value binding in string format
func (b *Binding) String() string {
	return fmt.Sprintf("binding[%v=%v]", b.Key, b.Value)
}

// Bindings are used to associate a variable with a value. They enable the creation of Binding, usually used with
// Lambda scripts to avoid continued recompilation costs. Bindings allow a remote engine to cache traversals that
// will be reused over and over again save that some parameterization may change.
// Used as g.V().Out(&Bindings{}.Of("key", value))
type Bindings struct{}

// Of creates a Binding
func (*Bindings) Of(key string, value interface{}) *Binding {
	return &Binding{
		Key:   key,
		Value: value,
	}
}
