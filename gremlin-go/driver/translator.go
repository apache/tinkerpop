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
	"strings"
)

type Translator interface {
	Translate(bytecode *Bytecode) (string, error)
}

type translator struct {
	source string
}

func NewTranslator(source string) Translator {
	return &translator{source}
}

func (t *translator) Translate(bytecode *Bytecode) (string, error) {
	return t.translate(bytecode, true)
}

func (t *translator) translate(bytecode *Bytecode, initial bool) (string, error) {
	translated := ""

	if initial {
		translated += t.source
		appendDot(&translated)
	}

	for _, instruction := range bytecode.sourceInstructions {
		if len(instruction.arguments) == 0 {
			continue
		}

		appendDot(&translated)

		translatedInstruction, err := t.translateInstruction(instruction)
		if err != nil {
			return "", err
		}
		translated += translatedInstruction
	}

	appendDot(&translated)

	for _, instruction := range bytecode.stepInstructions {

		appendDot(&translated)

		translatedInstruction, err := t.translateInstruction(instruction)
		if err != nil {
			return "", err
		}
		translated += translatedInstruction
	}

	return translated, nil
}

func (t *translator) translateInstruction(instruction instruction) (string, error) {
	instructionString := fmt.Sprintf("%v(", instruction.operator)

	for index, arg := range instruction.arguments {
		if index > 0 {
			instructionString += ","
		}

		argString, err := t.toString(arg)
		if err != nil {
			return "", err
		}
		instructionString += argString

	}

	instructionString += ")"

	return instructionString, nil
}

func (t *translator) translateMap(arg interface{}, isClassParamsMap bool) (string, error) {
	instructionString := ""
	iter := reflect.ValueOf(arg).MapRange()

	index := 0
	for iter.Next() {
		if index == 0 && !isClassParamsMap {
			instructionString += "{"
		}

		k := iter.Key().Interface()
		kString, err := t.toString(k)
		if err != nil {
			return "", err
		}
		if isClassParamsMap {
			kString = strings.Replace(kString, "'", "", -1)
		}
		v := iter.Value().Interface()
		vString, err := t.toString(v)
		if err != nil {
			return "", err
		}
		instructionString += fmt.Sprintf("%v:%v", kString, vString)

		if index < reflect.ValueOf(arg).Len()-1 {
			instructionString += ","
		}

		if index == reflect.ValueOf(arg).Len()-1 && !isClassParamsMap {
			instructionString += "}"
		}
		index++

	}

	return instructionString, nil
}

func (t *translator) translateSlice(arg interface{}) (string, error) {
	instructionString := "["
	oldSlice := reflect.ValueOf(arg)
	if oldSlice.Len() == 0 {
		return "", nil
	}

	for i := 0; i < oldSlice.Len(); i++ {
		if i > 0 {
			instructionString += ","
		}
		instructionStepString, err := t.toString(oldSlice.Index(i).Interface())
		if err != nil {
			return "", err
		}
		instructionString += fmt.Sprintf("%v", instructionStepString)
	}
	instructionString += "]"

	return instructionString, nil
}

func (t *translator) translateTextPredicate(v *textP) (string, error) {
	if v.operator == "" || len(v.values) == 0 {
		return "", nil
	}

	instructionString := ""
	instructionString += v.operator
	instructionString += "("
	if len(v.values) == 1 {
		argString, err := t.toString(v.values[0])
		if err != nil {
			return "", err
		}
		instructionString += argString
	} else if len(v.values) > 1 {
		instructionString += "["
		for index, arg := range v.values {
			argString, err := t.toString(arg)
			if err != nil {
				return "", err
			}

			instructionString += argString
			if index < len(v.values)-1 && argString != "" {
				instructionString += ","
			}
		}
		instructionString += "]"
	}

	instructionString += ")"

	return instructionString, nil
}

func (t *translator) translatePredicate(v *p) (string, error) {

	if v.operator == "" || len(v.values) == 0 {
		return "", nil
	}

	instructionString := ""
	instructionString += v.operator
	instructionString += "("
	if len(v.values) == 1 {
		argString, err := t.toString(v.values[0])
		if err != nil {
			return "", err
		}
		instructionString += argString
	} else if len(v.values) > 1 {
		instructionString += "["
		for index, arg := range v.values {
			argString, err := t.toString(arg)
			if err != nil {
				return "", err
			}

			instructionString += argString
			if index < len(v.values)-1 && argString != "" {
				instructionString += ","
			}
		}
		instructionString += "]"
	}

	instructionString += ")"

	return instructionString, nil
}

func (t *translator) translateTraversalStrategy(v *traversalStrategy) (string, error) {

	instructionString := ""
	className := getGroovyClassName(v.name)
	instructionString += fmt.Sprintf("new %v(", className)
	instructionConfigurationString, err := t.translateMap(interface{}(v.configuration), true)
	if err != nil {
		return "", err
	}
	instructionString += instructionConfigurationString
	instructionString += ")"

	return instructionString, nil

}

func (t *translator) toString(arg interface{}) (string, error) {
	if arg == nil {
		return "null", nil
	}
	reflectedArg := reflect.TypeOf(arg)
	switch reflectedArg.Kind() {

	case reflect.Map:
		return t.translateMap(arg, false)
	case reflect.Slice:
		return t.translateSlice(arg)
	default:
		switch v := arg.(type) {
		case AnonymousTraversal:
		case *AnonymousTraversal:
			return t.toString(arg)
		case *Binding:
			return v.String(), nil
		case GraphTraversal:
		case *GraphTraversal:
			return t.translate(v.Bytecode, false)
		case traversalStrategy:
		case *traversalStrategy:
			return t.translateTraversalStrategy(v)
		case *Bytecode:
			return t.translate(v, false)
		case textP:
		case *textP:
			return t.translateTextPredicate(v)
		case p:
		case *p:
			return t.translatePredicate(v)
		default:
			{
				switch arg.(type) {
				case string:
					{
						return fmt.Sprintf("'%v'", arg), nil
					}
				default:
					{
						return fmt.Sprintf("%v", arg), nil
					}
				}
			}
		}
	}

	return "", nil

}

func appendDot(s *string) {
	if len(*s) == 0 {
		return
	}

	if (*s)[len(*s)-1] != '.' {
		*s += "."
	}
}

func getGroovyClassName(classPath string) string {
	classPathArray := strings.Split(classPath, ".")
	return classPathArray[len(classPathArray)-1]
}
