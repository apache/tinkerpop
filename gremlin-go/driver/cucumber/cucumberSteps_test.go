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
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/cucumber/godog"
	"github.com/google/uuid"
)

type tinkerPopGraph struct {
	*CucumberWorld
	sync.Mutex
}

var parsers map[*regexp.Regexp]func(string, string) interface{}

func init() {
	parsers = map[*regexp.Regexp]func(string, string) interface{}{
		regexp.MustCompile(`^str\[(.*)]$`):        func(stringVal, graphName string) interface{} { return stringVal }, //returns the string value as is
		regexp.MustCompile(`^dt\[(.*)]$`):         toDateTime,
		regexp.MustCompile(`^uuid\[(.*)]$`):       toUuid,
		regexp.MustCompile(`^d\[(.*)]\.[bslfd]$`): toNumeric,
		regexp.MustCompile(`^d\[(.*)]\.[m]$`):     toBigDecimal,
		regexp.MustCompile(`^d\[(.*)]\.[n]$`):     toBigInt,
		regexp.MustCompile(`^d\[(.*)]\.[i]$`):     toInt32,
		regexp.MustCompile(`^vp\[(.+)]$`):         toVertexProperty,
		regexp.MustCompile(`^v\[(.+)]$`):          toVertex,
		regexp.MustCompile(`^v\[(.+)]\.id$`):      toVertexId,
		regexp.MustCompile(`^e\[(.+)]$`):          toEdge,
		regexp.MustCompile(`^v\[(.+)]\.sid$`):     toVertexIdString,
		regexp.MustCompile(`^e\[(.+)]\.id$`):      toEdgeId,
		regexp.MustCompile(`^e\[(.+)]\.sid$`):     toEdgeIdString,
		regexp.MustCompile(`^p\[(.+)]$`):          toPath,
		regexp.MustCompile(`^l\[(.*)]$`):          toList,
		regexp.MustCompile(`^s\[(.*)]$`):          toSet,
		regexp.MustCompile(`^m\[(.+)]$`):          toMap,
		regexp.MustCompile(`^t\[(.+)]$`):          toT,
		regexp.MustCompile(`^D\[(.+)]$`):          toDirection,
		regexp.MustCompile(`^M\[(.+)]$`):          toMerge,
	}
}

func parseValue(value string, graphName string) interface{} {
	var extractedValue string
	var parser func(string, string) interface{}
	if regexp.MustCompile(`^null$`).MatchString(value) {
		return nil
	}
	if regexp.MustCompile(`^true$`).MatchString(value) {
		return true
	}
	if regexp.MustCompile(`^false$`).MatchString(value) {
		return false
	}
	if regexp.MustCompile(`^d\[NaN]$`).MatchString(value) {
		return math.NaN()
	}
	if regexp.MustCompile(`^d\[Infinity]$`).MatchString(value) {
		return math.Inf(1)
	}
	if regexp.MustCompile(`^d\[-Infinity]$`).MatchString(value) {
		return math.Inf(-1)
	}

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

// Parse dateTime.
func toDateTime(stringVal, graphName string) interface{} {
	val, err := time.Parse(time.RFC3339Nano, stringVal)
	if err != nil {
		return nil
	}
	_, os := val.Zone()
	// go doesn't expose getting the abbreviated zone names from offset despite parsing into them, so we'll need
	// to update the location into UTC+/-HH:SS format for evaluation purposes
	return val.In(gremlingo.GetTimezoneFromOffset(os))
}

// Parse uuid.
func toUuid(stringVal, graphName string) interface{} {
	val, err := uuid.Parse(stringVal)
	if err != nil {
		return nil
	}
	return val
}

// Parse numeric.
func toNumeric(stringVal, graphName string) interface{} {
	if strings.Contains(stringVal, ".") {

		if strings.Contains(stringVal, "f") || strings.Contains(stringVal, "F") {
			val, err := strconv.ParseFloat(stringVal, 32)
			if err != nil {
				return nil
			}
			return float32(val)
		} else {
			val, err := strconv.ParseFloat(stringVal, 64)
			if err != nil {
				return nil
			}
			return val
		}
	}
	val, err := strconv.ParseInt(stringVal, 10, 64)
	if err != nil {
		return nil
	}
	return val
}

// Parse bigInt.
func toBigInt(stringVal, graphName string) interface{} {
	return gremlingo.ParseBigInt(stringVal)
}

// Parse bigDecimal.
func toBigDecimal(stringVal, graphName string) interface{} {
	return gremlingo.ParseBigDecimal(stringVal)
}

// Parse int32.
func toInt32(stringVal, graphName string) interface{} {
	val, err := strconv.ParseInt(stringVal, 10, 32)
	if err != nil {
		return nil
	}
	return int32(val)
}

// Parse vertex property.
func toVertexProperty(name, graphName string) interface{} {
	if vp, ok := tg.getDataGraphFromMap(graphName).vertexProperties[name]; ok {
		return vp
	} else {
		return fmt.Errorf("VertexProperty with key %s not found", name)
	}
}

// Parse vertex.
func toVertex(name, graphName string) interface{} {
	if v, ok := tg.getDataGraphFromMap(graphName).vertices[name]; ok {
		return v
	} else {
		return &gremlingo.Vertex{
			Element: gremlingo.Element{Id: name, Label: "vertex"},
		}
	}
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
	if e, ok := tg.getDataGraphFromMap(graphName).edges[name]; ok {
		return e
	} else {
		return fmt.Errorf("edge with key %s not found", name)
	}
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
	case reflect.Bool:
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

func toT(name, graphName string) interface{} {
	// Return as is, since T values are just strings.
	if name == "label" {
		return gremlingo.T.Label
	} else if name == "id" {
		return gremlingo.T.Id
	} else if name == "key" {
		return gremlingo.T.Key
	} else if name == "value" {
		return gremlingo.T.Value
	} else {
		return name
	}
}

func toDirection(name, graphName string) interface{} {
	// Return as is, since Direction values are just strings.
	if name == "IN" {
		return gremlingo.Direction.In
	} else if name == "OUT" {
		return gremlingo.Direction.Out
	} else if name == "BOTH" {
		return gremlingo.Direction.Both
	} else if name == "from" {
		return gremlingo.Direction.From
	} else if name == "to" {
		return gremlingo.Direction.To
	} else {
		return name
	}
}

func toMerge(name, graphName string) interface{} {
	// Return as is, since Merge values are just strings.
	if name == "outV" {
		return gremlingo.Merge.OutV
	} else if name == "inV" {
		return gremlingo.Merge.InV
	} else if name == "onCreate" {
		return gremlingo.Merge.OnCreate
	} else if name == "onMatch" {
		return gremlingo.Merge.OnMatch
	} else {
		return name
	}
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
		tg.error[true] = err.Error()
		return nil
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
		tg.error[true] = err.Error()
		return nil
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
	tg.g = gremlingo.Traversal_().With(data.connection)
	if graphName == "empty" {
		err := tg.cleanEmptyDataGraph(tg.g)
		if err != nil {
			return err
		}
	}

	// TODO: Uncoment code here to use WithComputer once this is implemented.
	// In this version strategies are not implemented (and therefore WithComputer also isn't implmented).
	for _, tag := range tg.scenario.Tags {
		if tag.Name == "@GraphComputerOnly" {
			return godog.ErrPending
			// tg.g.WithComputer()
		} else if tag.Name == "@AllowNullPropertyValues" {
			// The GLV suite does not test against a graph that has null property values enabled, skipping via Pending Error
			return godog.ErrPending
		}
	}
	return nil
}

func (tg *tinkerPopGraph) theGraphInitializerOf(arg1 *godog.DocString) error {
	traversal, err := GetTraversal(tg.scenario.Name, tg.g, tg.parameters, tg.sideEffects)
	if err != nil {
		return err
	}
	future := traversal.Iterate()
	return <-future
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
	traversal, err := GetTraversal(tg.scenario.Name, tg.g, tg.parameters, tg.sideEffects)
	if err != nil {
		return err
	}
	results, err := traversal.ToList()
	if err != nil {
		return err
	}
	if len(results) != expectedCount {
		return fmt.Errorf("graph returned count of %d when %d was expected", len(results), expectedCount)
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
					actualSlice := a.(*gremlingo.SimpleSet).ToSlice()
					expectedSlice := expectedResult[i].(*gremlingo.SimpleSet).ToSlice()
					if !compareListEqualsWithOrder(expectedSlice, actualSlice) {
						return fmt.Errorf("actual result does not match expected (order expected)\nActual: %v\nExpected: %v", actualResult, expectedResult)
					}
				}
			} else if len(actualResult) == 1 && len(expectedResult) == 1 && reflect.TypeOf(actualResult[0]).Kind() == reflect.Map &&
				reflect.TypeOf(expectedResult[0]).Kind() == reflect.Map {
				if !compareMapEquals(expectedResult[0].(map[interface{}]interface{}), actualResult[0].(map[interface{}]interface{})) {
					return fmt.Errorf("actual result does not match expected (order expected)\nActual: %v\nExpected: %v", actualResult, expectedResult)
				}
			} else {
				if !compareListEqualsWithOrder(expectedResult, actualResult) {
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

// compareMapEquals compares two maps for equality by checking if they have the same length
// and if every key-value pair in the actual map has a logically equal counterpart in the expected map.
func compareMapEquals(expected map[interface{}]interface{}, actual map[interface{}]interface{}) bool {
	if len(actual) != len(expected) {
		return false
	}
	for k, a := range actual {
		var e interface{}
		containsKey := false
		for ke, ee := range expected {
			if compareSingleEqual(k, ke) {
				containsKey = true
				e = ee
				break
			}
		}
		if !containsKey {
			fmt.Printf("Map comparison error: Failed to find key %s in %v\n", k, expected)
			return false
		}

		if !compareSingleEqual(a, e) {
			fmt.Printf("Map comparison error: Expected != Actual (%v!=%v)\n", e, a)
			return false
		}
	}
	return true
}

// compareSingleEqual is the core logical equality function used for Gremlin Gherkin tests.
// It avoids using fmt.Sprint which can include memory addresses for pointers (like Vertices or Edges),
// leading to false negatives in tests. It performs recursive comparisons for complex types
// and handles Gremlin-specific elements by comparing their IDs.
func compareSingleEqual(actual interface{}, expected interface{}) bool {
	if actual == nil && expected == nil {
		return true
	} else if actual == nil || expected == nil {
		return false
	}

	actualSet, ok1 := actual.(*gremlingo.SimpleSet)
	expectedSet, ok2 := expected.(*gremlingo.SimpleSet)
	if ok1 && ok2 {
		return compareListEqualsWithoutOrder(expectedSet.ToSlice(), actualSet.ToSlice())
	} else if ok1 || ok2 {
		return false
	}

	actualPath, ok1 := actual.(*gremlingo.Path)
	expectedPath, ok2 := expected.(*gremlingo.Path)
	if ok1 && ok2 {
		return compareListEqualsWithOrder(expectedPath.Objects, actualPath.Objects)
	} else if ok1 || ok2 {
		return false
	}

	actualV, ok1 := actual.(*gremlingo.Vertex)
	expectedV, ok2 := expected.(*gremlingo.Vertex)
	if ok1 && ok2 {
		return actualV.Id == expectedV.Id
	}

	actualE, ok1 := actual.(*gremlingo.Edge)
	expectedE, ok2 := expected.(*gremlingo.Edge)
	if ok1 && ok2 {
		return actualE.Id == expectedE.Id
	}

	actualVP, ok1 := actual.(*gremlingo.VertexProperty)
	expectedVP, ok2 := expected.(*gremlingo.VertexProperty)
	if ok1 && ok2 {
		return actualVP.Id == expectedVP.Id
	}

	actualP, ok1 := actual.(*gremlingo.Property)
	expectedP, ok2 := expected.(*gremlingo.Property)
	if ok1 && ok2 {
		return actualP.Key == expectedP.Key && compareSingleEqual(actualP.Value, expectedP.Value)
	}

	switch a := actual.(type) {
	case map[interface{}]interface{}:
		e, ok := expected.(map[interface{}]interface{})
		if !ok {
			return false
		}
		return compareMapEquals(e, a)
	case []interface{}:
		e, ok := expected.([]interface{})
		if !ok {
			return false
		}
		return compareListEqualsWithOrder(e, a)
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return compareNumeric(actual, expected)
	default:
		return fmt.Sprint(actual) == fmt.Sprint(expected)
	}
}

// compareNumeric normalizes different numeric types to float64 to allow for comparison
// between different precisions and representations (e.g. int64 vs float32).
// It also explicitly handles NaN equality which is required for some Gremlin tests.
func compareNumeric(actual interface{}, expected interface{}) bool {
	var aVal, eVal float64
	switch a := actual.(type) {
	case int:
		aVal = float64(a)
	case int8:
		aVal = float64(a)
	case int16:
		aVal = float64(a)
	case int32:
		aVal = float64(a)
	case int64:
		aVal = float64(a)
	case uint:
		aVal = float64(a)
	case uint8:
		aVal = float64(a)
	case uint16:
		aVal = float64(a)
	case uint32:
		aVal = float64(a)
	case uint64:
		aVal = float64(a)
	case float32:
		aVal = float64(a)
	case float64:
		aVal = a
	default:
		return fmt.Sprint(actual) == fmt.Sprint(expected)
	}

	switch e := expected.(type) {
	case int:
		eVal = float64(e)
	case int8:
		eVal = float64(e)
	case int16:
		eVal = float64(e)
	case int32:
		eVal = float64(e)
	case int64:
		eVal = float64(e)
	case uint:
		eVal = float64(e)
	case uint8:
		eVal = float64(e)
	case uint16:
		eVal = float64(e)
	case uint32:
		eVal = float64(e)
	case uint64:
		eVal = float64(e)
	case float32:
		eVal = float64(e)
	case float64:
		eVal = e
	default:
		return fmt.Sprint(actual) == fmt.Sprint(expected)
	}

	if math.IsNaN(aVal) && math.IsNaN(eVal) {
		return true
	}
	return aVal == eVal
}

// compareListEqualsWithOrder compares two slices for equality, ensuring that they have
// the same length and that elements at each index are logically equal.
func compareListEqualsWithOrder(expected []interface{}, actual []interface{}) bool {
	if len(expected) != len(actual) {
		return false
	}
	for i := range actual {
		if !compareSingleEqual(actual[i], expected[i]) {
			return false
		}
	}
	return true
}

// compareListEqualsWithoutOrder compares two slices for equality regardless of order.
// It ensures they have the same length and that every element in the actual slice
// has a corresponding logically equal element in the expected slice.
func compareListEqualsWithoutOrder(expected []interface{}, actual []interface{}) bool {
	if len(expected) != len(actual) {
		return false
	}
	expectedCopy := make([]interface{}, len(expected))
	copy(expectedCopy, expected)
	for _, a := range actual {
		found := false
		for i := len(expectedCopy) - 1; i >= 0; i-- {
			if compareSingleEqual(a, expectedCopy[i]) {
				expectedCopy = append(expectedCopy[:i], expectedCopy[i+1:]...)
				found = true
				break
			}
		}
		if !found {
			fmt.Printf("Failed to find %v in %v\n", a, expected)
			return false
		}
	}
	return true
}

// compareListEqualsWithOf compares two slices for "of" equality.
// This is used for Gherkin "of" assertions where the actual result must be
// a subset of the expected results or vice versa depending on the context,
// but generally confirms that all elements in 'actual' exist in 'expected'.
func compareListEqualsWithOf(expected []interface{}, actual []interface{}) bool {
	for _, a := range actual {
		found := false
		for i := len(expected) - 1; i >= 0; i-- {
			if compareSingleEqual(a, expected[i]) {
				found = true
				break
			}
		}
		if !found {
			fmt.Printf("Failed to find %v in %v\n", a, expected)
			return false
		}
	}
	return true
}

func makeSortedStringArrayFromSet(set *gremlingo.SimpleSet) []string {
	var sortedStrings []string
	for _, element := range set.ToSlice() {
		sortedStrings = append(sortedStrings, fmt.Sprintf("%v", element))
	}
	sort.Sort(sort.StringSlice(sortedStrings))

	return sortedStrings
}

func (tg *tinkerPopGraph) theTraversalOf(arg1 *godog.DocString) error {
	traversal, err := GetTraversal(tg.scenario.Name, tg.g, tg.parameters, tg.sideEffects)
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

func (tg *tinkerPopGraph) usingTheSideEffectDefined(key string, value string) error {
	if tg.graphName == "empty" {
		tg.reloadEmptyData()
	}
	tg.sideEffects[key] = parseValue(strings.Replace(value, "\\\"", "\"", -1), tg.graphName)
	return nil
}

func (tg *tinkerPopGraph) theTraversalWillRaiseAnError() error {
	if _, ok := tg.error[true]; ok {
		return nil
	}
	return fmt.Errorf("expected the traversal to raise an error")
}

func (tg *tinkerPopGraph) theTraversalWillRaiseAnErrorWithMessageContainingTextOf(comparison, expectedMessage string) error {
	if _, ok := tg.error[true]; !ok {
		return fmt.Errorf("expected the traversal to raise an error")
	}
	switch comparison {
	case "containing":
		if strings.Contains(tg.error[true], expectedMessage) {
			return nil
		} else {
			return fmt.Errorf("traversal error message must contain %s", expectedMessage)
		}
	case "starting":
		if strings.Contains(tg.error[true], expectedMessage) {
			return nil
		} else {
			return fmt.Errorf("traversal error message must contain %s", expectedMessage)
		}
	case "ending":
		if strings.Contains(tg.error[true], expectedMessage) {
			return nil
		} else {
			return fmt.Errorf("traversal error message must contain %s", expectedMessage)
		}
	default:
		return fmt.Errorf("unknow comparison %s - must be: containing, ending or starting", comparison)
	}
}

var tg = &tinkerPopGraph{
	NewCucumberWorld(),
	sync.Mutex{},
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
		tg.Lock()
		return ctx, nil
	})

	ctx.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		tg.Unlock()
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
	ctx.Step(`^using the side effect (.+) defined as"(.+)"$`, tg.usingTheSideEffectDefined)
	ctx.Step(`^the traversal will raise an error$`, tg.theTraversalWillRaiseAnError)
	ctx.Step(`^the traversal will raise an error with message (\w+) text of "(.+)"$`, tg.theTraversalWillRaiseAnErrorWithMessageContainingTextOf)
}

func skipTestsIfNotEnabled(t *testing.T, testSuiteName string, testSuiteEnabled bool) {
	if !testSuiteEnabled {
		t.Skip(fmt.Sprintf("Skipping %s because %s tests are not enabled.", t.Name(), testSuiteName))
	}
}

func getEnvOrDefaultBool(key string, defaultValue bool) bool {
	value := getEnvOrDefaultString(key, "")
	if len(value) != 0 {
		boolValue, err := strconv.ParseBool(value)
		if err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func TestCucumberFeatures(t *testing.T) {
	skipTestsIfNotEnabled(t, "cucumber godog tests",
		getEnvOrDefaultBool("RUN_INTEGRATION_WITH_ALIAS_TESTS", true))
	suite := godog.TestSuite{
		TestSuiteInitializer: InitializeTestSuite,
		ScenarioInitializer:  InitializeScenario,
		Options: &godog.Options{
			Tags:     "~@GraphComputerOnly && ~@AllowNullPropertyValues && ~@StepSubgraph && ~@StepTree",
			Format:   "pretty",
			Paths:    []string{getEnvOrDefaultString("CUCUMBER_FEATURE_FOLDER", "../../../gremlin-test/src/main/resources/org/apache/tinkerpop/gremlin/test/features")},
			TestingT: t, // Testing instance that will run subtests.
		},
	}

	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}
