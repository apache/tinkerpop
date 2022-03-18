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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/apache/tinkerpop/gremlin-go/driver"
	"github.com/cucumber/godog"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// TODO proper error handling
type tinkerPopGraph struct {
	*TinkerPopWorld
}

var parsers map[*regexp.Regexp]func(string, string) interface{}

func init() {
	parsers = map[*regexp.Regexp]func(string, string) interface{}{
		regexp.MustCompile(`^d\[(.*)]\.[lfdm]$`): toNumeric,
		regexp.MustCompile(`^d\[(.*)]\.[i]$`):    toInt32,
		regexp.MustCompile(`^v\[(.+)]$`):         toVertex,
		regexp.MustCompile(`^v\[(.+)]\.id$`):     toVertexId,
		regexp.MustCompile(`^e\[(.+)]$`):         toEdge,
		regexp.MustCompile(`^v\[(.+)]\.sid$`):    toVertexIdString,
		regexp.MustCompile(`^e\[(.+)]\.id$`):     toEdgeId,
		regexp.MustCompile(`^e\[(.+)]\.sid$`):    toEdgeIdString,
		regexp.MustCompile(`^p\[(.+)]$`):         toPath,
		regexp.MustCompile(`^l\[(.*)]$`):         toList,
		regexp.MustCompile(`^s\[(.*)]$`):         toSet,
		regexp.MustCompile(`^m\[(.+)]$`):         toMap,
		regexp.MustCompile(`^c\[(.+)]$`):         toLambda,
		regexp.MustCompile(`^t\[(.+)]$`):         toT,
		regexp.MustCompile(`^D\[(.+)]$`):         toDirection,
	}
}

func parseValue(value string, graphName string) interface{} {
	if regexp.MustCompile(`^null$`).MatchString(value) {
		return nil
	}
	var extractedValue string
	var parser func(string, string) interface{}
	for key, element := range parsers {
		var match = key.FindAllStringSubmatch(value, -1)
		if len(match) > 0 {
			parser = element
			extractedValue = match[0][1]
			break
		}
	}
	if parser == nil {
		return value
	} else {
		return parser(extractedValue, graphName)
	}
}

// Parse numeric.
func toNumeric(stringVal, graphName string) interface{} {
	if strings.Contains(stringVal, ".") {
		val, err := strconv.ParseFloat(stringVal, 64)
		if err != nil {
			return nil
		}
		return val
	}
	val, err := strconv.ParseInt(stringVal, 10, 64)
	if err != nil {
		return nil
	}
	return val
}

// Parse int32.
func toInt32(stringVal, graphName string) interface{} {
	val, err := strconv.ParseInt(stringVal, 10, 32)
	if err != nil {
		return nil
	}
	return int32(val)
}

// Parse vertex.
func toVertex(name, graphName string) interface{} {
	return tg.getDataGraphFromMap(graphName).vertices[name]
}

// Parse vertex id.
func toVertexId(name, graphName string) interface{} {
	if tg.getDataGraphFromMap(graphName).vertices[name] == nil {
		return nil
	}
	return tg.getDataGraphFromMap(graphName).vertices[name].Id
}

// Parse vertex id as string.
func toVertexIdString(name, graphName string) interface{} {
	if tg.getDataGraphFromMap(graphName).vertices[name] == nil {
		return nil
	}
	return fmt.Sprint(tg.getDataGraphFromMap(graphName).vertices[name].Id)
}

// Parse edge.
func toEdge(name, graphName string) interface{} {
	return tg.getDataGraphFromMap(graphName).edges[name]
}

// Parse edge id.
func toEdgeId(name, graphName string) interface{} {
	if tg.getDataGraphFromMap(graphName).edges[name] == nil {
		return nil
	}
	return tg.getDataGraphFromMap(graphName).edges[name].Id
}

// Parse edge id as string.
func toEdgeIdString(name, graphName string) interface{} {
	if tg.getDataGraphFromMap(graphName).edges[name] == nil {
		return nil
	}
	return fmt.Sprint(tg.getDataGraphFromMap(graphName).edges[name].Id)
}

// Parse path.
func toPath(stringObjects, graphName string) interface{} {
	objects := make([]interface{}, 0)
	for _, str := range strings.Split(stringObjects, ",") {
		objects = append(objects, parseValue(str, graphName))
	}
	return &gremlingo.Path{
		Labels:  []gremlingo.Set{},
		Objects: objects,
	}
}

// Parse list.
func toList(stringList, graphName string) interface{} {
	listVal := make([]interface{}, 0)
	if len(stringList) == 0 {
		return listVal
	}

	for _, str := range strings.Split(stringList, ",") {
		listVal = append(listVal, parseValue(str, graphName))
	}
	return listVal
}

// Parse set to simple set.
func toSet(stringSet, graphName string) interface{} {
	setVal := gremlingo.NewSimpleSet()
	if len(stringSet) == 0 {
		return setVal
	}
	for _, str := range strings.Split(stringSet, ",") {
		setVal.Add(parseValue(str, graphName))
	}
	return setVal
}

// Parse json as a map.
func toMap(name, graphName string) interface{} {
	var jsonMap interface{}
	err := json.Unmarshal([]byte(name), &jsonMap)
	if err != nil {
		return nil
	}
	return parseMapValue(jsonMap, graphName)
}

func parseMapValue(mapVal interface{}, graphName string) interface{} {
	if mapVal == nil {
		return nil
	}
	switch reflect.TypeOf(mapVal).Kind() {
	case reflect.String:
		return parseValue(mapVal.(string), graphName)
	case reflect.Float64, reflect.Int64:
		return mapVal
	case reflect.Array, reflect.Slice:
		var valSlice []interface{}
		oriSlice := reflect.ValueOf(mapVal)
		for i := 0; i < oriSlice.Len(); i++ {
			valSlice = append(valSlice, parseMapValue(oriSlice.Index(i).Interface(), graphName))
		}
		return valSlice
	case reflect.Map:
		valMap := make(map[interface{}]interface{})
		v := reflect.ValueOf(mapVal)
		keys := v.MapKeys()
		for _, k := range keys {
			convKey := k.Convert(v.Type().Key())
			val := v.MapIndex(convKey)
			keyVal := parseMapValue(k.Interface(), graphName)
			if reflect.ValueOf(keyVal).Kind() == reflect.Slice {
				// Turning map keys of slice type into string type for comparison purposes
				// string slices should also be converted into slices more easily
				valMap[fmt.Sprint(keyVal)] = parseMapValue(val.Interface(), graphName)
			} else {
				valMap[keyVal] = parseMapValue(val.Interface(), graphName)
			}
		}
		return valMap
	default:
		// Not supported types.
		return nil
	}
}

// Parse lambda.
func toLambda(name, graphName string) interface{} {
	return &gremlingo.Lambda{Script: name}
}

func toT(name, graphName string) interface{} {
	// Return as is, since T values are just strings.
	return name
}

func toDirection(name, graphName string) interface{} {
	// Return as is, since Direction values are just strings.
	return name
}

func (tg *tinkerPopGraph) anUnsupportedTest() error {
	return nil
}

func (tg *tinkerPopGraph) iteratedNext() error {
	if tg.traversal == nil {
		// Return pending because this is not currently implemented.
		return godog.ErrPending
	}
	result, err := tg.traversal.Next()
	if err != nil {
		return err
	}
	var nextResults []interface{}
	switch result.GetType().Kind() {
	case reflect.Array, reflect.Slice:
		resSlice := reflect.ValueOf(result.GetInterface())
		for i := 0; i < resSlice.Len(); i++ {
			nextResults = append(nextResults, resSlice.Index(i).Interface())
		}
	default:
		simpleSet, ok := result.GetInterface().(*gremlingo.SimpleSet)
		if ok {
			nextResults = simpleSet.ToSlice()
		} else {
			nextResults = append(nextResults, result)
		}
	}

	tg.result = nextResults
	return nil
}

func (tg *tinkerPopGraph) iteratedToList() error {
	if tg.traversal == nil {
		// Return pending because this is not currently implemented.
		return godog.ErrPending
	}
	results, err := tg.traversal.ToList()
	if err != nil {
		return err
	}
	var listResults []interface{}
	for _, res := range results {
		listResults = append(listResults, res)
	}
	tg.result = listResults
	return nil
}

func (tg *tinkerPopGraph) nothingShouldHappenBecause(arg1 *godog.DocString) error {
	return nil
}

// Choose the graph.
func (tg *tinkerPopGraph) chooseGraph(graphName string) error {
	tg.graphName = graphName
	data := tg.graphDataMap[graphName]
	tg.g = gremlingo.Traversal_().WithRemote(data.connection)
	if graphName == "empty" {
		err := tg.cleanEmptyDataGraph(tg.g)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tg *tinkerPopGraph) theGraphInitializerOf(arg1 *godog.DocString) error {
	traversal, err := GetTraversal(tg.scenario.Name, tg.g, tg.parameters)
	if err != nil {
		return err
	}
	_, future, err := traversal.Iterate()
	if err != nil {
		return err
	}
	<-future
	return nil
}

func (tg *tinkerPopGraph) theResultShouldHaveACountOf(expectedCount int) error {
	actualCount := len(tg.result)
	if actualCount != expectedCount {
		if actualCount == 1 {
			switch reflect.TypeOf(tg.result).Kind() {
			case reflect.Slice, reflect.Array:
				result := tg.result[0].(*gremlingo.Result).GetInterface()
				switch reflect.TypeOf(result).Kind() {
				case reflect.Map:
					actualCount = len(result.(map[interface{}]interface{}))
				}
			}
			if actualCount != expectedCount {
				return fmt.Errorf("result should return %d for count, but returned %d", expectedCount, actualCount)
			}
		} else {
			return fmt.Errorf("result should return %d for count, but returned %d", expectedCount, actualCount)
		}
	}
	return nil
}

func (tg *tinkerPopGraph) theGraphShouldReturnForCountOf(expectedCount int, traversalText string) error {
	traversal, err := GetTraversal(tg.scenario.Name, tg.g, tg.parameters)
	if err != nil {
		return err
	}
	results, err := traversal.ToList()
	if err != nil {
		return err
	}
	if len(results) != expectedCount {
		return errors.New("graph did not return the correct count")
	}
	return nil
}

func (tg *tinkerPopGraph) theResultShouldBeEmpty() error {
	if len(tg.result) != 0 {
		return errors.New("actual result is not empty as expected")
	}
	return nil
}

func (tg *tinkerPopGraph) theResultShouldBe(characterizedAs string, table *godog.Table) error {
	ordered := characterizedAs == "ordered"
	// For comparing ordered gremlingo.SimpleSet case.
	var expectSet bool
	var expectPath bool
	switch characterizedAs {
	case "ordered", "unordered", "of":
		var expectedResult []interface{}
		for idx, row := range table.Rows {
			if idx == 0 {
				// Skip the header line.
				continue
			}
			val := parseValue(row.Cells[0].Value, tg.graphName)
			v, ok := val.(*gremlingo.Path)
			expectPath = ok
			if ok {
				// Clear the labels since we don't define them in feature files.
				v.Labels = []gremlingo.Set{}
				val = v
			}
			_, expectSet = val.(*gremlingo.SimpleSet)
			expectedResult = append(expectedResult, val)
		}
		var actualResult []interface{}
		if len(tg.result) == 1 {
			switch r := tg.result[0].(type) {
			case *gremlingo.Result:
				val, ok := r.GetInterface().(*gremlingo.Path)
				if !expectPath && ok {
					actualResult = val.Objects
				} else {
					actualResult = append(actualResult, r.GetInterface())
				}
			default:
				actualResult = append(actualResult, r)
			}
		} else {
			for _, res := range tg.result {
				switch r := res.(type) {
				case *gremlingo.Result:
					actualResult = append(actualResult, r.GetInterface())
				default:
					actualResult = append(actualResult, r)
				}
			}
		}
		if characterizedAs != "of" && (len(actualResult) != len(expectedResult)) {
			err := fmt.Sprintf("actual result length does not equal expected (%d!=%d).", len(actualResult), len(expectedResult))
			return errors.New(err)
		}
		if ordered {
			if expectSet {
				for i, a := range actualResult {
					if fmt.Sprint(a.(*gremlingo.SimpleSet).ToSlice()) != fmt.Sprint(expectedResult[i].(*gremlingo.SimpleSet).ToSlice()) {
						return fmt.Errorf("actual result does not match expected (order expected)\nActual: %v\nExpected: %v", actualResult, expectedResult)
					}
				}
			} else {
				if fmt.Sprint(actualResult) != fmt.Sprint(expectedResult) {
					return fmt.Errorf("actual result does not match expected (order expected)\nActual: %v\nExpected: %v", actualResult, expectedResult)
				}
			}
		} else {
			if characterizedAs == "of" {
				if !compareListEqualsWithOf(expectedResult, actualResult) {
					return fmt.Errorf("actual result does not match expected (order not expected)\nActual: %v\nExpected: %v", actualResult, expectedResult)
				}
			} else {
				if !compareListEqualsWithoutOrder(expectedResult, actualResult) {
					return fmt.Errorf("actual result does not match expected (order not expected)\nActual: %v\nExpected: %v", actualResult, expectedResult)
				}
			}
		}
		return nil
	default:
		return errors.New("scenario not supported")
	}
}

func compareMapEquals(expected map[interface{}]interface{}, actual map[interface{}]interface{}) bool {
	for k, a := range actual {
		var e interface{}
		containsKey := false
		for ke, ee := range expected {
			if fmt.Sprint(k) == fmt.Sprint(ke) {
				containsKey = true
				e = ee
				break
			} else {
				if reflect.ValueOf(k).Kind() == reflect.Ptr &&
					reflect.ValueOf(ke).Kind() == reflect.Ptr {
					switch k.(type) {
					case *gremlingo.Vertex:
						switch ke.(type) {
						case *gremlingo.Vertex:
							if fmt.Sprint(*k.(*gremlingo.Vertex)) == fmt.Sprint(*ke.(*gremlingo.Vertex)) {
								containsKey = true
							}
						default:
							// Not equal.
						}
					default:
						// If we are here we probably need to implement an additional type like the Vertex above.
						if fmt.Sprint(*k.(*interface{})) == fmt.Sprint(*ke.(*interface{})) {
							fmt.Println("WARNING: Encountered unknown pointer type as map key.")
							containsKey = true
						}
					}
					if containsKey {
						e = ee
						break
					}
				}
			}
		}
		if !containsKey {
			fmt.Printf("Map comparison error: Failed to find key %s in %v\n", k, expected)
			return false
		}

		if a == nil && e == nil {
			continue
		} else if a == nil || e == nil {
			// One value is nil, other is not. They are not equal.
			fmt.Printf("Map comparison error: One map has a nil key, other does not.\n")
			return false
		} else {
			switch reflect.TypeOf(a).Kind() {
			case reflect.Array, reflect.Slice:
				switch reflect.TypeOf(e).Kind() {
				case reflect.Array, reflect.Slice:
					// Compare arrays
					if !compareListEqualsWithoutOrder(e.([]interface{}), a.([]interface{})) {
						return false
					}
				default:
					fmt.Printf("Map comparison error: Expected type is Array/Slice, actual is %s.\n", reflect.TypeOf(a).Kind())
					return false
				}
			case reflect.Map:
				switch reflect.TypeOf(a).Kind() {
				case reflect.Map:
					// Compare maps
					if !compareMapEquals(e.(map[interface{}]interface{}), a.(map[interface{}]interface{})) {
						return false
					}
				default:
					fmt.Printf("Map comparison error: Expected type is Map, actual is %s.\n", reflect.TypeOf(a).Kind())
					return false
				}
			default:
				if fmt.Sprint(a) != fmt.Sprint(e) {
					fmt.Printf("Map comparison error: Expected != Actual (%s!=%s)\n", fmt.Sprint(a), fmt.Sprint(e))
					return false
				}
			}
		}
	}
	return true
}

func compareListEqualsWithoutOrder(expected []interface{}, actual []interface{}) bool {
	// This is a little weird, but there isn't a good solution to either of these problems:
	// 		1. Comparison of types in Go. No deep equals which actually works properly. Needs to be done manually.
	// 		2. In place deletion in a loop.
	// So to do in place deletion in a loop we can do the following:
	// 		1. Loop from back to wrong (don't need to worry about deleted indices that way.
	//		2. Create a new slice with the index removed when we fix the item we want to delete.
	// To do an orderless copy, a copy of the expected result is created. Results are removed as they are found. This stops
	// the following from returning equal [1 2 2 2] and [1 1 1 2]
	expectedCopy := make([]interface{}, len(expected))
	copy(expectedCopy, expected)
	for _, a := range actual {
		found := false
		if a == nil {
			for i := len(expectedCopy) - 1; i >= 0; i-- {
				if expectedCopy[i] == nil {
					expectedCopy = append(expectedCopy[:i], expectedCopy[i+1:]...)
					found = true
					break
				}
			}
		} else {
			switch reflect.TypeOf(a).Kind() {
			case reflect.Array, reflect.Slice:
				for i := len(expectedCopy) - 1; i >= 0; i-- {
					if expectedCopy[i] != nil {
						switch reflect.TypeOf(expectedCopy[i]).Kind() {
						case reflect.Array, reflect.Slice:
							if compareListEqualsWithoutOrder(expectedCopy[i].([]interface{}), a.([]interface{})) {
								expectedCopy = append(expectedCopy[:i], expectedCopy[i+1:]...)
								found = true
							}
						}
						if found {
							break
						}
					}
				}
			case reflect.Map:
				for i := len(expectedCopy) - 1; i >= 0; i-- {
					if expectedCopy[i] != nil {
						switch reflect.TypeOf(expectedCopy[i]).Kind() {
						case reflect.Map:
							if compareMapEquals(expectedCopy[i].(map[interface{}]interface{}), a.(map[interface{}]interface{})) {
								expectedCopy = append(expectedCopy[:i], expectedCopy[i+1:]...)
								found = true
							}
						}
						if found {
							break
						}
					}
				}
			default:
				for i := len(expectedCopy) - 1; i >= 0; i-- {
					if fmt.Sprint(a) == fmt.Sprint(expectedCopy[i]) {
						expectedCopy = append(expectedCopy[:i], expectedCopy[i+1:]...)
						found = true
						break
					}
				}
			}
		}
		if !found {
			fmt.Printf("Failed to find %v in %v\n", a, expected)
			return false
		}
	}
	return true
}

func compareListEqualsWithOf(expected []interface{}, actual []interface{}) bool {
	// When comparing with "of", we expect cases like [1 2] (expected) and [1 1 1 2] (actual) , or
	// [1 1 1 2] (expected) and [1 2] (actual) to return equal.
	for _, a := range actual {
		found := false
		if a == nil {
			for i := len(expected) - 1; i >= 0; i-- {
				if expected[i] == nil {
					found = true
					break
				}
			}
		} else {
			switch reflect.TypeOf(a).Kind() {
			case reflect.Array, reflect.Slice:
				for i := len(expected) - 1; i >= 0; i-- {
					if expected[i] != nil {
						switch reflect.TypeOf(expected[i]).Kind() {
						case reflect.Array, reflect.Slice:
							if compareListEqualsWithoutOrder(expected[i].([]interface{}), a.([]interface{})) {
								found = true
							}
						}
						if found {
							break
						}
					}
				}
			case reflect.Map:
				for i := len(expected) - 1; i >= 0; i-- {
					if expected[i] != nil {
						switch reflect.TypeOf(expected[i]).Kind() {
						case reflect.Map:
							if compareMapEquals(expected[i].(map[interface{}]interface{}), a.(map[interface{}]interface{})) {
								found = true
							}
						}
						if found {
							break
						}
					}
				}
			default:
				for i := len(expected) - 1; i >= 0; i-- {
					if fmt.Sprint(a) == fmt.Sprint(expected[i]) {
						found = true
						break
					}
				}
			}
		}
		if !found {
			fmt.Printf("Failed to find %v in %v\n", a, expected)
			return false
		}
	}
	return true
}

func (tg *tinkerPopGraph) theTraversalOf(arg1 *godog.DocString) error {
	traversal, err := GetTraversal(tg.scenario.Name, tg.g, tg.parameters)
	if err != nil {
		return err
	}
	tg.traversal = traversal
	return nil
}

func (tg *tinkerPopGraph) usingTheParameterDefined(name string, params string) error {
	if tg.graphName == "empty" {
		tg.reloadEmptyData()
	}
	tg.parameters[name] = parseValue(strings.Replace(params, "\\\"", "\"", -1), tg.graphName)
	return nil
}

func (tg *tinkerPopGraph) usingTheParameterOfP(paramName, pVal, stringVal string) error {
	predicate := reflect.ValueOf(gremlingo.P).MethodByName(strings.Title(pVal)).Interface().(func(...interface{}) gremlingo.Predicate)
	values := parseValue(stringVal, tg.graphName)
	switch reflect.TypeOf(values).Kind() {
	case reflect.Array, reflect.Slice:
		tg.parameters[paramName] = predicate(values.([]interface{})...)
	default:
		tg.parameters[paramName] = predicate(values)
	}
	return nil
}

var tg = &tinkerPopGraph{
	NewTinkerPopWorld(),
}

func InitializeTestSuite(ctx *godog.TestSuiteContext) {
	ctx.BeforeSuite(func() {
		tg.loadAllDataGraph()
	})
	ctx.AfterSuite(func() {
		err := tg.closeAllDataGraphConnection()
		if err != nil {
			return
		}
	})
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	ctx.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		tg.scenario = sc
		// Add tg.recreateAllDataGraphConnection() here and tg.closeAllDataGraphConnection() in an After scenario
		// hook if necessary to isolate failing tests that closes the shared connection.
		return ctx, nil
	})

	ctx.Step(`^an unsupported test$`, tg.anUnsupportedTest)
	ctx.Step(`^iterated next$`, tg.iteratedNext)
	ctx.Step(`^iterated to list$`, tg.iteratedToList)
	ctx.Step(`^nothing should happen because$`, tg.nothingShouldHappenBecause)
	ctx.Step(`^the (.+) graph$`, tg.chooseGraph)
	ctx.Step(`^the graph initializer of$`, tg.theGraphInitializerOf)
	ctx.Step(`^the graph should return (\d+) for count of "(.+)"$`, tg.theGraphShouldReturnForCountOf)
	ctx.Step(`^the result should be empty$`, tg.theResultShouldBeEmpty)
	ctx.Step(`^the result should be (o\w+)$`, tg.theResultShouldBe)
	ctx.Step(`^the result should be (u\w+)$`, tg.theResultShouldBe)
	ctx.Step(`^the result should have a count of (\d+)$`, tg.theResultShouldHaveACountOf)
	ctx.Step(`^the traversal of$`, tg.theTraversalOf)
	ctx.Step(`^using the parameter (.+) defined as "(.+)"$`, tg.usingTheParameterDefined)
	ctx.Step(`^using the parameter (.+) of P\.(.+)\("(.+)"\)$`, tg.usingTheParameterOfP)
}
