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

func convertStrategyVarargs(strategies []TraversalStrategy) []interface{} {
	converted := make([]interface{}, 0)
	for _, strategy := range strategies {
		converted = append(converted, strategy)
	}
	return converted
}

// GraphTraversalSource can be used to start GraphTraversal.
type GraphTraversalSource struct {
	graph            *Graph
	gremlinLang      *GremlinLang
	remoteConnection *DriverRemoteConnection
	graphTraversal   *GraphTraversal
}

// NewGraphTraversalSource creates a new GraphTraversalSource from a Graph, DriverRemoteConnection, and any TraversalStrategy.
// Graph and DriverRemoteConnection can be set to nil as valid default values.
func NewGraphTraversalSource(graph *Graph, remoteConnection *DriverRemoteConnection,
	traversalStrategies ...TraversalStrategy) *GraphTraversalSource {
	// TODO: revisit when updating strategies
	gl := NewGremlinLang(nil)
	return &GraphTraversalSource{graph: graph, gremlinLang: gl, remoteConnection: remoteConnection}
}

// NewDefaultGraphTraversalSource creates a new graph GraphTraversalSource without a graph, strategy, or existing traversal.
func NewDefaultGraphTraversalSource() *GraphTraversalSource {
	return &GraphTraversalSource{graph: nil, gremlinLang: NewGremlinLang(nil), remoteConnection: nil}
}

func (gts *GraphTraversalSource) GetGremlinLang() *GremlinLang {
	return gts.gremlinLang
}

// GetGraphTraversal gets the graph traversal associated with this graph traversal source.
func (gts *GraphTraversalSource) GetGraphTraversal() *GraphTraversal {
	return NewGraphTraversal(gts.graph, NewGremlinLang(gts.gremlinLang), gts.remoteConnection)
}

func (gts *GraphTraversalSource) clone() *GraphTraversalSource {
	return cloneGraphTraversalSource(gts.graph, NewGremlinLang(gts.gremlinLang), gts.remoteConnection)
}

func cloneGraphTraversalSource(graph *Graph, gl *GremlinLang, remoteConnection *DriverRemoteConnection) *GraphTraversalSource {
	return &GraphTraversalSource{graph: graph,
		gremlinLang:      gl,
		remoteConnection: remoteConnection,
	}
}

// WithBulk allows for control of bulking operations.
func (gts *GraphTraversalSource) WithBulk(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.gremlinLang.AddSource("withBulk", args...)
	return source
}

// WithPath adds a path to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithPath(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.gremlinLang.AddSource("withPath", args...)
	return source
}

// WithSack adds a sack to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithSack(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	// Force int32 serialization for valid number values for server compatibility
	source.gremlinLang.AddSource("withSack", int32Args(args)...)
	return source
}

// WithSideEffect adds a side effect to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithSideEffect(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.gremlinLang.AddSource("withSideEffect", args...)
	return source
}

// WithStrategies adds an arbitrary collection of TraversalStrategies instances to the traversal source.
func (gts *GraphTraversalSource) WithStrategies(args ...TraversalStrategy) *GraphTraversalSource {
	convertedArgs := convertStrategyVarargs(args)
	source := gts.clone()
	source.gremlinLang.AddSource("withStrategies", convertedArgs...)
	return source
}

// WithoutStrategies removes an arbitrary collection of TraversalStrategies instances to the traversal source.
func (gts *GraphTraversalSource) WithoutStrategies(args ...TraversalStrategy) *GraphTraversalSource {
	convertedArgs := convertStrategyVarargs(args)
	source := gts.clone()
	source.gremlinLang.AddSource("withoutStrategies", convertedArgs...)
	return source
}

// With provides a configuration to a traversal in the form of a key value pair.
func (gts *GraphTraversalSource) With(key interface{}, value interface{}) *GraphTraversalSource {
	source := gts.clone()

	//TODO verify remote when connection is set-up
	var optionsStrategy TraversalStrategy = nil
	if len(gts.gremlinLang.optionsStrategies) != 0 {
		optionsStrategy = gts.gremlinLang.optionsStrategies[0]
	}

	if optionsStrategy == nil {
		optionsStrategy = OptionsStrategy(map[string]interface{}{key.(string): value})
		return source.WithStrategies(optionsStrategy)
	}

	options := optionsStrategy.(*traversalStrategy)
	options.configuration[key.(string)] = value
	return source
}

// WithRemote adds a remote to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithRemote(remoteConnection *DriverRemoteConnection) *GraphTraversalSource {
	gts.remoteConnection = remoteConnection
	if gts.graphTraversal != nil {
		gts.graphTraversal.remote = remoteConnection
	}
	return gts.clone()
}

// E reads edges from the graph to start the traversal.
func (gts *GraphTraversalSource) E(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("E", args...)
	return traversal
}

// V reads vertices from the graph to start the traversal.
func (gts *GraphTraversalSource) V(args ...interface{}) *GraphTraversal {
	for i := 0; i < len(args); i++ {
		if v, ok := args[i].(*Vertex); ok {
			args[i] = v.Id
		}
	}
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("V", args...)
	return traversal
}

// AddE adds an Edge to start the traversal.
func (gts *GraphTraversalSource) AddE(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("addE", args...)
	return traversal
}

// AddV adds a Vertex to start the traversal.
func (gts *GraphTraversalSource) AddV(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("addV", args...)
	return traversal
}

// Call starts the traversal by executing a procedure
func (gts *GraphTraversalSource) Call(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("call", args...)
	return traversal
}

// Inject inserts arbitrary objects to start the traversal.
func (gts *GraphTraversalSource) Inject(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	// Force int32 serialization for valid number values for server compatibility
	traversal.GremlinLang.AddStep("inject", int32Args(args)...)
	return traversal
}

// Io adds the io steps to start the traversal.
func (gts *GraphTraversalSource) Io(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("io", args...)
	return traversal
}

// MergeE uses an upsert-like operation to add an Edge to start the traversal.
func (gts *GraphTraversalSource) MergeE(args ...interface{}) *GraphTraversal {
	if len(args) != 0 {
		if m, ok := args[0].(map[interface{}]interface{}); ok {
			if v, ok := m[Direction.Out].(*Vertex); ok {
				m[Direction.Out] = v.Id
			}
			if v, ok := m[Direction.In].(*Vertex); ok {
				m[Direction.In] = v.Id
			}
		}
	}
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("mergeE", args...)
	return traversal
}

// MergeV uses an upsert-like operation to add a Vertex to start the traversal.
func (gts *GraphTraversalSource) MergeV(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("mergeV", args...)
	return traversal
}

// Union allows for a multi-branched start to a traversal.
func (gts *GraphTraversalSource) Union(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.GremlinLang.AddStep("union", args...)
	return traversal
}

func (gts *GraphTraversalSource) Tx() *Transaction {
	return &Transaction{g: gts, remoteConnection: gts.remoteConnection}
}
