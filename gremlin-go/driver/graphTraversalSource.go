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

// TraversalStrategies is interceptor methods to alter the execution of the traversal (e.g. query re-writing).
type TraversalStrategies struct {
}

// GraphTraversalSource can be used to start GraphTraversal.
type GraphTraversalSource struct {
	graph               *Graph
	traversalStrategies *TraversalStrategies
	bytecode            *bytecode
	remoteConnection    *DriverRemoteConnection
	graphTraversal      *GraphTraversal
}

// NewGraphTraversalSource creates a graph traversal source, the primary DSL of the Gremlin traversal machine.
func NewGraphTraversalSource(graph *Graph, traversalStrategies *TraversalStrategies, bytecode *bytecode, remoteConnection *DriverRemoteConnection) *GraphTraversalSource {
	return &GraphTraversalSource{graph: graph, traversalStrategies: traversalStrategies, bytecode: bytecode, remoteConnection: remoteConnection}
}

// NewDefaultGraphTraversalSource creates a new graph GraphTraversalSource without a graph, strategy, or existing traversal.
func NewDefaultGraphTraversalSource() *GraphTraversalSource {
	return &GraphTraversalSource{graph: nil, traversalStrategies: nil, bytecode: newBytecode(nil)}
}

// GetBytecode gets the traversal bytecode associated with this graph traversal source.
func (gts *GraphTraversalSource) GetBytecode() *bytecode {
	return gts.bytecode
}

// GetGraphTraversal gets the graph traversal associated with this graph traversal source.
func (gts *GraphTraversalSource) GetGraphTraversal() *GraphTraversal {
	return NewGraphTraversal(gts.graph, gts.traversalStrategies, newBytecode(gts.bytecode), gts.remoteConnection)
}

// GetTraversalStrategies gets the graph traversal strategies associated with this graph traversal source.
func (gts *GraphTraversalSource) GetTraversalStrategies() *TraversalStrategies {
	return gts.traversalStrategies
}

func (gts *GraphTraversalSource) clone() *GraphTraversalSource {
	return NewGraphTraversalSource(gts.graph, gts.traversalStrategies, newBytecode(gts.bytecode), gts.remoteConnection)
}

// WithBulk allows for control of bulking operations.
func (gts *GraphTraversalSource) WithBulk(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	err := source.bytecode.addSource("withBulk", args...)
	if err != nil {
		return nil
	}
	return source
}

// WithPath adds a path to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithPath(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.bytecode.addSource("withPath", args...)
	return source
}

// WithSack adds a sack to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithSack(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.bytecode.addSource("withSack", args...)
	return source
}

// WithSideEffect adds a side effect to be used throughout the life of a spawned Traversal.
func (gts *GraphTraversalSource) WithSideEffect(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.bytecode.addSource("withSideEffect", args...)
	return source
}

// WithStrategies adds an arbitrary collection of TraversalStrategies instances to the traversal source.
func (gts *GraphTraversalSource) WithStrategies(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.bytecode.addSource("withStrategies", args...)
	return source
}

// WithoutStrategies removes an arbitrary collection of TraversalStrategies instances to the traversal source.
func (gts *GraphTraversalSource) WithoutStrategies(args ...interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.bytecode.addSource("withoutStrategies", args...)
	return source
}

// With provides a configuration to a traversal in the form of a key value pair.
func (gts *GraphTraversalSource) With(key interface{}, value interface{}) *GraphTraversalSource {
	source := gts.clone()
	source.bytecode.addSource("withStrategies", key, value)
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
	traversal.bytecode.addStep("E", args...)
	return traversal
}

// V reads vertices from the graph to start the traversal.
func (gts *GraphTraversalSource) V(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.bytecode.addStep("V", args...)
	return traversal
}

// AddE adds an Edge to start the traversal.
func (gts *GraphTraversalSource) AddE(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.bytecode.addStep("addE", args...)
	return traversal
}

// AddV adds a Vertex to start the traversal.
func (gts *GraphTraversalSource) AddV(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.bytecode.addStep("addV", args...)
	return traversal
}

// Inject inserts arbitrary objects to start the traversal.
func (gts *GraphTraversalSource) Inject(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.bytecode.addStep("inject", args...)
	return traversal
}

// Io adds the io steps to start the traversal.
func (gts *GraphTraversalSource) Io(args ...interface{}) *GraphTraversal {
	traversal := gts.GetGraphTraversal()
	traversal.bytecode.addStep("io", args...)
	return traversal
}
