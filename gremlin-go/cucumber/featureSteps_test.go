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
	"github.com/cucumber/godog"
	"github.com/lyndonb-bq/tinkerpop/gremlin-go/driver"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

// TODO proper error handling
type tinkerPopGraph struct {
	*TinkerPopWorld
}

type SliceKey struct {
	KeyValue []interface{}
}

var parsers map[*regexp.Regexp]func(string, string) interface{}

func init() {
	parsers = map[*regexp.Regexp]func(string, string) interface{}{
		regexp.MustCompile(`^d\[(.*)]\.[ilfdm]$`): toNumeric,
		regexp.MustCompile(`^v\[(.+)]$`):          toVertex,
		regexp.MustCompile(`^v\[(.+)]\.id$`):      toVertexId,
		regexp.MustCompile(`^e\[(.+)]$`):          toEdge,
		regexp.MustCompile(`^v\[(.+)]\.sid$`):     toVertexIdString,
		regexp.MustCompile(`^e\[(.+)]\.id$`):      toEdgeId,
		regexp.MustCompile(`^e\[(.+)]\.sid$`):     toEdgeIdString,
		regexp.MustCompile(`^p\[(.+)]$`):          toPath,
		regexp.MustCompile(`^l\[(.*)$]`):          toList,
		regexp.MustCompile(`^s\[(.*)$]`):          toSet,
		regexp.MustCompile(`^m\[(.+)]$`):          toMap,
		regexp.MustCompile(`^c\[(.+)]$`):          toLambda,
		regexp.MustCompile(`^t\[(.+)]$`):          toT,
		regexp.MustCompile(`^null$`):              func(string, string) interface{} { return nil },
	}
}

func parseValue(value string, graphName string) interface{} {
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
	return fmt.Sprint(tg.getDataGraphFromMap(graphName).edges[name])
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
		// Use SliceKey struct or convert to array if this will be used as key.
		return valSlice
	case reflect.Map:
		valMap := make(map[interface{}]interface{})
		v := reflect.ValueOf(mapVal)
		keys := v.MapKeys()
		for _, k := range keys {
			convKey := k.Convert(v.Type().Key())
			val := v.MapIndex(convKey)
			valMap[parseMapValue(k.Interface(), graphName)] = parseMapValue(val.Interface(), graphName)
		}
		return valMap
	default:
		// Not supported types.
		return nil
	}
}

// TODO add with lambda implementation - AN-1037
// Parse lambda.
func toLambda(name, graphName string) interface{} {
	// NOTE: This may cause null pointer exceptions on server in tests that need this parameter
	return nil
}

func toT(name, graphName string) interface{} {
	// Return as is, since T values are just strings.
	return name
}

func (tg *tinkerPopGraph) anUnsupportedTest() error {
	return nil
}

func (tg *tinkerPopGraph) iteratedNext() error {
	if tg.traversal == nil {
		return errors.New("nil traversal, feature need to be implemented in go")
	}
	result, err := tg.traversal.Next()
	if err != nil {
		return err
	}
	tg.result = append(tg.result, result)
	return nil
}

func (tg *tinkerPopGraph) iteratedToList() error {
	if tg.traversal == nil {
		return errors.New("nil traversal, feature need to be implemented in go")
	}
	results, err := tg.traversal.ToList()
	if err != nil {
		return err
	}
	tg.result = results
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
	// We may have modified the so-called `empty` graph.
	if tg.graphName == "empty" {
		tg.reloadEmptyData()
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
			if ok {
				// Clear the labels since we don't define them in feature files.
				v.Labels = []gremlingo.Set{}
				val = v
			}
			expectedResult = append(expectedResult, val)
		}
		var actualResult []interface{}
		for _, res := range tg.result {
			actualResult = append(actualResult, res.GetInterface())
		}
		if characterizedAs != "of" && len(actualResult) != len(expectedResult) {
			err := fmt.Sprintf("actual result length %d does not equal to expected result length %d.", len(actualResult), len(expectedResult))
			return errors.New(err)
		}
		if ordered {
			for idx, res := range actualResult {
				if !reflect.DeepEqual(expectedResult[idx], res) {
					return errors.New("actual result is not ordered")
				}
			}
		} else {
			for _, res := range actualResult {
				if !contains(expectedResult, res) {
					return errors.New("actual result does not match expected result")
				}
			}
		}
		return nil
	default:
		return errors.New("scenario not supported")
	}
}

func contains(list []interface{}, item interface{}) bool {
	for _, v := range list {
		if reflect.DeepEqual(v, item) {
			return true
		}
	}
	return false
}

func (tg *tinkerPopGraph) theResultShouldHaveACountOf(expectedCount int) error {
	actualCount := len(tg.result)
	if len(tg.result) != expectedCount {
		err := fmt.Sprintf("result should return %d for count, but returned %d.", expectedCount, actualCount)
		return errors.New(err)
	}
	return nil
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
	var in []reflect.Value
	values := parseValue(stringVal, tg.graphName)
	switch reflect.TypeOf(values).Kind() {
	case reflect.Array, reflect.Slice:
		for _, value := range values.([]interface{}) {
			in = append(in, reflect.ValueOf(value))
		}
	default:
		in = append(in, reflect.ValueOf(values))
	}
	var p = reflect.ValueOf(gremlingo.P).MethodByName(pVal).Call(in)
	tg.parameters[paramName] = p
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
		tg.closeAllDataGraphConnection()
	})
}

func InitializeScenario(ctx *godog.ScenarioContext) {
	ctx.Before(func(ctx context.Context, sc *godog.Scenario) (context.Context, error) {
		tg.scenario = sc
		//tg.loadAllDataGraph()
		tg.recreateAllDataGraphConnection()
		return ctx, nil
	})

	ctx.After(func(ctx context.Context, sc *godog.Scenario, err error) (context.Context, error) {
		tg.closeAllDataGraphConnection()
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
