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
	"go/token"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type GremlinLang struct {
	emptyArray        []interface{}
	gremlin           *strings.Builder
	parameters        map[string]interface{}
	optionsStrategies []*traversalStrategy
	paramCount        *atomic.Uint64
}

// NewGremlinLang creates a new GremlinLang to be used in traversals.
func NewGremlinLang(gl *GremlinLang) *GremlinLang {
	gremlin := &strings.Builder{}
	parameters := make(map[string]interface{})
	optionsStrategies := make([]*traversalStrategy, 0)
	paramCount := atomic.Uint64{}
	if gl != nil {
		gremlin = gl.gremlin
		parameters = gl.parameters
		optionsStrategies = gl.optionsStrategies
		paramCount.Store(gl.paramCount.Load())
	}

	return &GremlinLang{
		gremlin:           gremlin,
		parameters:        parameters,
		optionsStrategies: optionsStrategies,
		paramCount:        &paramCount,
	}
}

func (gl *GremlinLang) addToGremlin(name string, args ...interface{}) error {
	flattenedArgs := gl.flattenArguments(args...)
	if name == "CardinalityValueTraversal" {
		gl.gremlin.WriteString("Cardinality.")
		str0, err := gl.argAsString(flattenedArgs[0])
		if err != nil {
			return err
		}
		str1, err := gl.argAsString(flattenedArgs[1])
		if err != nil {
			return err
		}
		gl.gremlin.WriteString(str0)
		gl.gremlin.WriteString("(")
		gl.gremlin.WriteString(str1)
		gl.gremlin.WriteString(")")
	}

	gl.gremlin.WriteString(".")
	gl.gremlin.WriteString(name)
	gl.gremlin.WriteString("(")

	for i := 0; i < len(flattenedArgs); i++ {
		if i > 0 {
			gl.gremlin.WriteString(",")
		}
		convertArg, err := gl.convertArgument(flattenedArgs[i]) //.Index(i).Interface())
		if err != nil {
			return err
		}
		argStr, err := gl.argAsString(convertArg)
		if err != nil {
			return err
		}
		gl.gremlin.WriteString(argStr)
	}
	gl.gremlin.WriteString(")")
	return nil
}

func (gl *GremlinLang) argAsString(arg interface{}) (string, error) {
	if arg == nil {
		return "null", nil
	}
	// we are concerned with both single and double quotes and %q in fmt only escapes double quotes
	escapeQuotes := strings.NewReplacer(`'`, `\'`, `"`, `\"`)

	switch v := arg.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", escapeQuotes.Replace(v)), nil
	case bool:
		return strconv.FormatBool(v), nil
	case uint8:
		return fmt.Sprintf("%dB", v), nil
	case int8, int16:
		return fmt.Sprintf("%dS", v), nil
	case int32, uint16:
		return fmt.Sprintf("%d", v), nil
	case int:
		if v <= math.MaxInt32 && v >= math.MinInt32 {
			return fmt.Sprintf("%d", v), nil
		} else {
			return fmt.Sprintf("%dL", v), nil
		}
	case int64, uint32:
		return fmt.Sprintf("%dL", v), nil
	case uint, uint64, *big.Int:
		return fmt.Sprintf("%dN", v), nil
	case float32:
		if math.IsNaN(float64(v)) {
			return "Nan", nil
		}
		if math.IsInf(float64(v), 1) {
			return "Infinity", nil
		}
		if math.IsInf(float64(v), -1) {
			return "-Infinity", nil
		}
		return fmt.Sprintf("%vF", v), nil
	case float64:
		if math.IsNaN(v) {
			return "Nan", nil
		}
		if math.IsInf(v, 1) {
			return "Infinity", nil
		}
		if math.IsInf(v, -1) {
			return "-Infinity", nil
		}
		return fmt.Sprintf("%vD", v), nil
	case *BigDecimal, BigDecimal:
		return fmt.Sprintf("%vM", v), nil
	case time.Time:
		return fmt.Sprintf("datetime(\"%v\")", v.Format(time.RFC3339)), nil
	case cardinality, column, direction, operator, order, pick, pop, barrier, scope, t, merge:
		name := reflect.ValueOf(v).Type().Name()
		return fmt.Sprintf("%s.%s", strings.ToUpper(name[:1])+name[1:], v), nil
	case *Vertex:
		id, _ := gl.argAsString(v.Id)
		return fmt.Sprintf("new ReferenceVertex(%s,\"%s\")", escapeQuotes.Replace(id), escapeQuotes.Replace(v.Label)), nil
	case textP:
		return gl.translateTextPredicate(&v)
	case *textP:
		return gl.translateTextPredicate(v)
	case p:
		return gl.translatePredicate(&v)
	case *p:
		return gl.translatePredicate(v)
	case Traversal:
		gremlinLang := v.GremlinLang
		for key, val := range gremlinLang.parameters {
			gl.parameters[key] = val
		}
		return gremlinLang.GetGremlin("__"), nil
	case *Traversal:
		gremlinLang := v.GremlinLang
		for key, val := range gremlinLang.parameters {
			gl.parameters[key] = val
		}
		return gremlinLang.GetGremlin("__"), nil
	case GraphTraversal:
		gremlinLang := v.GremlinLang
		for key, val := range gremlinLang.parameters {
			gl.parameters[key] = val
		}
		return gremlinLang.GetGremlin("__"), nil
	case *GraphTraversal:
		gremlinLang := v.GremlinLang
		for key, val := range gremlinLang.parameters {
			gl.parameters[key] = val
		}
		return gremlinLang.GetGremlin("__"), nil
	case GremlinLang:
		for key, val := range v.parameters {
			gl.parameters[key] = val
		}
		return v.GetGremlin("__"), nil
	case *GremlinLang:
		for key, val := range v.parameters {
			gl.parameters[key] = val
		}
		return v.GetGremlin("__"), nil
	case GValue:
		key := v.Name()
		if !token.IsIdentifier(key) {
			panic(fmt.Sprintf("invalid parameter name '%v'.", key))
		}
		value := v.Value()
		if val, ok := gl.parameters[key]; ok {
			if reflect.TypeOf(val).Kind() == reflect.Slice || reflect.TypeOf(value).Kind() == reflect.Slice ||
				reflect.TypeOf(val).Kind() == reflect.Map || reflect.TypeOf(value).Kind() == reflect.Map {
				if !reflect.DeepEqual(val, value) {
					panic(fmt.Sprintf("parameter with name '%v' already exists.", key))
				}
			} else if val != value {
				panic(fmt.Sprintf("parameter with name '%v' already exists.", key))
			}
		} else {
			gl.parameters[key] = v.Value()
		}
		return key, nil
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Map:
			return gl.translateMap(arg)
		case reflect.Slice:
			return gl.translateSlice(arg)
		default:
			return gl.asParameter(arg), nil
		}
	}
}

func (gl *GremlinLang) translateMap(arg interface{}) (string, error) {
	sb := strings.Builder{}
	sb.WriteString("[")
	size := reflect.ValueOf(arg).Len()
	if size == 0 {
		sb.WriteString(":")
	} else {
		iter := reflect.ValueOf(arg).MapRange()
		for iter.Next() {
			k := iter.Key().Interface()
			kString, err := gl.argAsString(k)
			if err != nil {
				return "", err
			}
			v := iter.Value().Interface()
			vString, err := gl.argAsString(v)
			if err != nil {
				return "", err
			}
			sb.WriteString(kString)
			sb.WriteByte(':')
			sb.WriteString(vString)
			size--
			if size > 0 {
				sb.WriteString(",")
			}
		}
	}

	sb.WriteString("]")
	return sb.String(), nil
}

func (gl *GremlinLang) translateSlice(arg interface{}) (string, error) {
	sb := strings.Builder{}
	sb.WriteString("[")
	list := reflect.ValueOf(arg)

	for i := 0; i < list.Len(); i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		vString, err := gl.argAsString(list.Index(i).Interface())
		if err != nil {
			return "", err
		}
		sb.WriteString(vString)
	}
	sb.WriteString("]")
	return sb.String(), nil
}

func (gl *GremlinLang) translateTextPredicate(v *textP) (string, error) {
	if v.operator == "" || len(v.values) == 0 {
		return "", nil
	}

	instructionString := ""
	instructionString += v.operator
	instructionString += "("
	if len(v.values) == 1 {
		argString, err := gl.argAsString(v.values[0])
		if err != nil {
			return "", err
		}
		instructionString += argString
	} else if len(v.values) > 1 {
		for index, arg := range v.values {
			argString, err := gl.argAsString(arg)
			if err != nil {
				return "", err
			}

			instructionString += argString
			if index < len(v.values)-1 && argString != "" {
				instructionString += ","
			}
		}
	}

	instructionString += ")"

	return instructionString, nil
}

func (gl *GremlinLang) translatePredicate(v *p) (string, error) {

	if v.operator == "" || len(v.values) == 0 {
		return "", nil
	}

	instructionString := ""
	instructionString += v.operator
	instructionString += "("
	if len(v.values) == 1 {
		argString, err := gl.argAsString(v.values[0])
		if err != nil {
			return "", err
		}
		instructionString += argString
	} else if len(v.values) > 1 {
		if v.operator != "between" && v.operator != "inside" {
			instructionString += "["
		}
		for index, arg := range v.values {
			argString, err := gl.argAsString(arg)
			if err != nil {
				return "", err
			}

			instructionString += argString
			if index < len(v.values)-1 && argString != "" {
				instructionString += ","
			}
		}
		if v.operator != "between" && v.operator != "inside" {
			instructionString += "]"
		}
	}

	instructionString += ")"

	return instructionString, nil
}
func (gl *GremlinLang) asParameter(arg interface{}) string {
	paramName := fmt.Sprintf("_%d", gl.paramCount.Load())
	gl.paramCount.Add(1)
	gl.parameters[paramName] = arg
	return paramName
}

func (gl *GremlinLang) GetGremlin(arg ...string) string {
	var g string
	gremlin := gl.gremlin.String()
	if len(arg) == 0 {
		g = "g"
	} else {
		g = arg[0]
	}
	// special handling for CardinalityValueTraversal
	if gl.gremlin.Len() != 0 && string(gremlin[0]) != "." {
		return gremlin
	}
	return g + gremlin
}

func (gl *GremlinLang) GetParameters() map[string]interface{} {
	return gl.parameters
}

func (gl *GremlinLang) AddG(g string) {
	gl.parameters["g"] = g
}

func (gl *GremlinLang) Reset() {
	gl.paramCount.Store(0)
}

func (gl *GremlinLang) AddSource(name string, arguments ...interface{}) {
	if name == "withStrategies" && len(arguments) != 0 {
		args := gl.buildStrategyArgs(arguments...)
		// possible to have empty strategies list to send
		if len(args) != 0 {
			gl.gremlin.WriteString(".withStrategies(")
			gl.gremlin.WriteString(args)
			gl.gremlin.WriteString(")")
		}
		return
	}
	gl.addToGremlin(name, arguments)
}

func (gl *GremlinLang) buildStrategyArgs(args ...interface{}) string {
	sb := strings.Builder{}
	c := 0
	for _, arg := range args {
		if c > 0 {
			sb.WriteString(",")
		}
		strategy, ok := arg.(*traversalStrategy)
		if !ok {
			// error?
			continue
		}
		// special handling for OptionsStrategy
		if strategy.name == "OptionsStrategy" {
			gl.optionsStrategies = append(gl.optionsStrategies, strategy)
			continue
		}
		if len(strategy.configuration) == 0 {
			sb.WriteString(strategy.name)
		} else {
			sb.WriteString("new ")
			sb.WriteString(strategy.name)
			sb.WriteString("(")
			ct := 0
			for key, val := range strategy.configuration {
				if ct > 0 {
					sb.WriteString(",")
				}
				sb.WriteString(key)
				sb.WriteString(":")
				traversal, ok := val.(*Traversal)
				if ok {
					stringTraversal, _ := gl.argAsString(traversal.GremlinLang)
					sb.WriteString(stringTraversal)
				} else {
					stringArg, _ := gl.argAsString(val)
					sb.WriteString(stringArg)
				}
				ct++
			}
			sb.WriteString(")")
		}
		c++
	}
	return sb.String()
}

func (gl *GremlinLang) AddStep(stepName string, arguments ...interface{}) error {
	err := gl.addToGremlin(stepName, arguments)
	return err
}

func (gl *GremlinLang) GetOptionsStrategies() []*traversalStrategy {
	return gl.optionsStrategies
}

func (gl *GremlinLang) IsEmpty() bool {
	return gl.gremlin.Len() == 0
}

func (gl *GremlinLang) flattenArguments(arguments ...interface{}) []interface{} {
	if arguments == nil || len(arguments) == 0 {
		return gl.emptyArray
	}
	flatArgs := make([]interface{}, 0)
	for _, argument := range arguments {
		arg, ok := argument.([]interface{})
		if ok {
			for _, nestedArg := range arg {
				converted, _ := gl.convertArgument(nestedArg)
				flatArgs = append(flatArgs, converted)
			}
		} else {
			converted, _ := gl.convertArgument(argument)
			flatArgs = append(flatArgs, converted)
		}
	}
	return flatArgs
}

func (gl *GremlinLang) convertArgument(arg interface{}) (interface{}, error) {
	if arg == nil {
		return nil, nil
	}
	switch v := arg.(type) {
	case *Traversal:
		if v.graph != nil {
			return nil, fmt.Errorf("the child traversal of %s was not spawned anonymously - "+
				"use the __ class rather than a TraversalSource to construct the child traversal", arg)
		}
		return v.GremlinLang, nil
	default:
		switch reflect.TypeOf(arg).Kind() {
		case reflect.Map:
			argMap := reflect.ValueOf(arg)
			newMap := make(map[interface{}]interface{}, argMap.Len())
			for _, key := range argMap.MapKeys() {
				convertKey, _ := gl.convertArgument(key.Interface())
				convertValue, _ := gl.convertArgument(argMap.MapIndex(key).Interface())
				newMap[convertKey] = convertValue
			}
			return newMap, nil
		case reflect.Array, reflect.Slice:
			argList := reflect.ValueOf(arg)
			newList := make([]interface{}, argList.Len())
			for i := 0; i < argList.Len(); i++ {
				convertValue, _ := gl.convertArgument(argList.Index(i).Interface())
				newList[i] = convertValue
			}
			return newList, nil
		default:
			return arg, nil
		}
	}
}

// TODO revisit and remove if necessary
//var withOptionsMap map[any]string = map[any]string{
//	WithOptions.Tokens:  "WithOptions.tokens",
//	WithOptions.None:    "WithOptions.none",
//	WithOptions.Ids:     "WithOptions.ids",
//	WithOptions.Labels:  "WithOptions.labels",
//	WithOptions.Keys:    "WithOptions.keys",
//	WithOptions.Values:  "WithOptions.values",
//	WithOptions.All:     "WithOptions.all",
//	WithOptions.Indexer: "WithOptions.indexer",
//	WithOptions.List:    "WithOptions.list",
//	WithOptions.Map:     "WithOptions.map",
//}
