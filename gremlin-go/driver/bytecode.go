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
)

type bytecode struct {
	sourceInstructions []instruction
	stepInstructions   []instruction
	bindings           map[string]interface{}
}

func newBytecode(bc *bytecode) *bytecode {
	sourceInstructions := make([]instruction, 0)
	stepInstructions := make([]instruction, 0)
	bindingMap := make(map[string]interface{})
	if bc != nil {
		sourceInstructions = append(sourceInstructions, bc.sourceInstructions...)
		stepInstructions = append(stepInstructions, bc.stepInstructions...)
	}

	return &bytecode{
		sourceInstructions: sourceInstructions,
		stepInstructions:   stepInstructions,
		bindings:           bindingMap,
	}
}

func (bytecode *bytecode) createInstruction(operator string, args ...interface{}) (*instruction, error) {
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

func (bytecode *bytecode) addSource(sourceName string, args ...interface{}) error {
	instruction, err := bytecode.createInstruction(sourceName, args...)
	if err != nil {
		return err
	}

	bytecode.sourceInstructions = append(bytecode.sourceInstructions, *instruction)
	return err
}

func (bytecode *bytecode) addStep(stepName string, args ...interface{}) error {
	instruction, err := bytecode.createInstruction(stepName, args...)
	if err != nil {
		return err
	}

	bytecode.stepInstructions = append(bytecode.stepInstructions, *instruction)
	return err
}

func (bytecode *bytecode) convertArgument(arg interface{}) (interface{}, error) {
	t := reflect.TypeOf(arg)
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
		case binding:
			convertedValue, err := bytecode.convertArgument(v.value)
			if err != nil {
				return nil, err
			}
			bytecode.bindings[v.key] = v.value
			return binding{
				key:   v.key,
				value: convertedValue,
			}, nil
		case Traversal:
			if v.graph != nil {
				return nil, errors.New("the child traversal was not spawned anonymously - use the __ class rather than a TraversalSource to construct the child traversal")
			}
			for k, val := range v.bytecode.bindings {
				bytecode.bindings[k] = val
			}
			return v.bytecode, nil
		default:
			return arg, nil
		}
	}

}

type instruction struct {
	operator  string
	arguments []interface{}
}

// TODO: AN-1018 Export this
type binding struct {
	key   string
	value interface{}
}
