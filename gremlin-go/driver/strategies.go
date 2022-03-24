/*
Licensed to the Apache Software Foundation (ASF) Under one
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

const (
	baseNamespace               = "org.apache.tinkerpop.gremlin.process.traversal.strategy."
	decorationNamespace         = baseNamespace + "decoration."
	finalizationNamespace       = baseNamespace + "finalization."
	optimizationNamespace       = baseNamespace + "optimization."
	verificationNamespace       = baseNamespace + "verification."
	computerDecorationNamespace = "org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration."
)

// Decoration strategies

// ConnectiveStrategy rewrites the binary conjunction form of a.And().b into a AndStep of
// And(a,b) (likewise for OrStep).
func ConnectiveStrategy() *traversalStrategy {
	return &traversalStrategy{name: decorationNamespace + "ConnectiveStrategy"}
}

// ElementIdStrategy provides a degree of control over element identifier assignment as some Graphs don't provide
// that feature. This strategy provides for identifier assignment by enabling users to utilize Vertex and Edge indices
// under the hood, thus simulating that capability.
// By default, when an identifier is not supplied by the user, newly generated identifiers are UUID objects.
func ElementIdStrategy() *traversalStrategy {
	return &traversalStrategy{name: decorationNamespace + "ElementIdStrategy"}
}

func HaltedTraverserStrategy(haltedTraverserFactoryName string) *traversalStrategy {
	config := make(map[string]interface{})
	if haltedTraverserFactoryName != "" {
		config["haltedTraverserFactory"] = haltedTraverserFactoryName
	}
	return &traversalStrategy{name: decorationNamespace + "HaltedTraverserStrategy", configuration: config}
}

// OptionsStrategy will not alter the Traversal. It is only a holder for configuration options associated with the
// Traversal meant to be accessed by steps or other classes that might have some interaction with it. It is
// essentially a way for users to provide Traversal level configuration options that can be used in various ways
// by different Graph providers.
func OptionsStrategy(options map[string]interface{}) *traversalStrategy {
	return &traversalStrategy{name: decorationNamespace + "OptionsStrategy", configuration: options}
}

// PartitionStrategy partitions the Vertices, Edges and Vertex properties of a Graph into String named
// partitions (i.e. buckets, subgraphs, etc.).  It blinds a Traversal from "seeing" specified areas of
// the graph. The Traversal will ignore all Graph elements not in those "read" partitions.
func PartitionStrategy(partitionKey, writePartition string, readPartitions []string, includeMetaProperties bool) *traversalStrategy {
	config := map[string]interface{}{"includeMetaProperties": includeMetaProperties}
	if partitionKey != "" {
		config["partitionKey"] = partitionKey
	}
	if writePartition != "" {
		config["writePartition"] = writePartition
	}
	if len(readPartitions) != 0 {
		config["readPartitions"] = readPartitions
	}
	return &traversalStrategy{name: decorationNamespace + "PartitionStrategy", configuration: config}
}

// SeedStrategy resets the specified Seed value for Seedable steps, which in turn will produce deterministic
// results from those steps. It is important to note that when using this strategy that it only guarantees
// deterministic results from a step but not from an entire Traversal. For example, if a Graph does no guarantee
// iteration order for g.V() then repeated runs of g.V().Coin(0.5) with this strategy will return the same number
// of results but not necessarily the same ones. The same problem can occur in OLAP-based Taversals where
// iteration order is not explicitly guaranteed. The only way to ensure completely deterministic results in that
// sense is to apply some form of order() in these cases.
func SeedStrategy(seed int64) *traversalStrategy {
	config := map[string]interface{}{"seed": seed}
	return &traversalStrategy{name: decorationNamespace + "SeedStrategy", configuration: config}
}

// SubgraphStrategy provides a way to limit the view of a Traversal. By providing Traversal representations that
// represent a form of filtering criterion for Vertices and/or Edges, this strategy will inject that criterion into
// the appropriate places of a Traversal thus restricting what it Traverses and returns.
func SubgraphStrategy(vertices, edges, vertexProperties interface{}) *traversalStrategy { // all vars can be Traversal or null
	config := make(map[string]interface{})
	if vertices != nil {
		config["vertices"] = vertices
	}
	if edges != nil {
		config["edges"] = edges
	}
	if vertexProperties != nil {
		config["vertexProperties"] = vertexProperties
	}
	return &traversalStrategy{name: decorationNamespace + "SubgraphStrategy", configuration: config}
}

func VertexProgramStrategy(graphComputer, persist, result string, workers int,
	vertices, edges interface{}, configuration map[string]interface{}) *traversalStrategy {
	config := make(map[string]interface{})
	if graphComputer != "" {
		config["graphComputer"] = graphComputer
	}
	if persist != "" {
		config["persist"] = persist
	}
	if result != "" {
		config["result"] = result
	}
	if workers != 0 {
		config["workers"] = workers
	}
	if vertices != nil {
		config["vertices"] = vertices
	}
	if edges != nil {
		config["edges"] = edges
	}
	if configuration != nil {
		for k, v := range configuration {
			config[k] = v
		}
	}
	return &traversalStrategy{name: computerDecorationNamespace + "VertexProgramStrategy", configuration: config}
}

// Verification strategies

// EdgeLabelVerificationStrategy does not allow Edge traversal steps to have no label specified.
// Providing one or more labels is considered to be a best practice, however, TinkerPop will not force
// the specification of Edge labels; instead, providers or users will have to enable this strategy explicitly.
func EdgeLabelVerificationStrategy(logWarning, throwException bool) *traversalStrategy {
	config := map[string]interface{}{
		"logWarning":     logWarning,
		"throwException": throwException,
	}

	return &traversalStrategy{name: verificationNamespace + "EdgeLabelVerificationStrategy", configuration: config}
}

// LambdaRestrictionStrategy does not allow lambdas to be used in a Traversal. The contents of a lambda
// cannot be analyzed/optimized and thus, reduces the ability of other TraversalStrategy instances to reason
// about the traversal. This strategy is not activated by default. However, graph system providers may choose
// to make this a default strategy in order to ensure their respective strategies are better able to operate.
func LambdaRestrictionStrategy() *traversalStrategy {
	return &traversalStrategy{name: verificationNamespace + "LambdaRestrictionStrategy"}
}

// ReadOnlyStrategy detects steps marked with Mutating and returns an error if one is found.
func ReadOnlyStrategy() *traversalStrategy {
	return &traversalStrategy{name: verificationNamespace + "ReadOnlyStrategy"}
}

// ReservedKeysVerificationStrategy detects property keys that should not be used by the traversal.
// A term may be reserved by a particular graph implementation or as a convention given best practices.
func ReservedKeysVerificationStrategy(logWarning, throwException bool, keys []string) *traversalStrategy {
	config := map[string]interface{}{
		"logWarning":     logWarning,
		"throwException": throwException,
	}
	if keys != nil {
		config["keys"] = keys
	}
	return &traversalStrategy{name: verificationNamespace + "ReservedKeysVerificationStrategy", configuration: config}
}

// Optimization strategies

// AdjacentToIncidentStrategy looks for Vertex- and value-emitting steps followed by a CountGlobalStep and replaces
// the pattern with an Edge- or Property-emitting step followed by a CountGlobalStep. Furthermore, if a vertex-
// or value-emitting step is the last step in a .Has(traversal), .And(traversal, ...) or .Or(traversal, ...)
// child traversal, it is replaced by an appropriate Edge- or Property-emitting step.
// Performing this replacement removes situations where the more expensive trip to an adjacent Graph Element (e.g.
// the Vertex on the other side of an Edge) can be satisfied by trips to incident Graph Elements (e.g. just the Edge
// itself).
func AdjacentToIncidentStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "AdjacentToIncidentStrategy"}
}

// ByModulatorOptimizationStrategy looks for standard traversals in By-modulators and replaces them with more
// optimized traversals (e.g. TokenTraversal) if possible.
func ByModulatorOptimizationStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "ByModulatorOptimizationStrategy"}
}

// CountStrategy optimizes any occurrence of CountGlobalStep followed by an IsStep The idea is to limit
// the number of incoming elements in a way that it's enough for the IsStep to decide whether it evaluates
// true or false. If the traversal already contains a user supplied limit, the strategy won't modify it.
func CountStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "CountStrategy"}
}

// EarlyLimitStrategy looks for RangeGlobalSteps that can be moved further left in the traversal and thus be applied
// earlier. It will also try to merge multiple RangeGlobalSteps into one.
// If the logical consequence of one or multiple RangeGlobalSteps is an empty result, the strategy will remove
// as many steps as possible and add a NoneStep instead.
func EarlyLimitStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "EarlyLimitStrategy"}
}

// FilterRankingStrategy reorders filter- and order-steps according to their rank. Step ranks are defined within
// the strategy and indicate when it is reasonable for a step to move in front of another. It will also do its best to
// push step labels as far "right" as possible in order to keep Traversers as small and bulkable as possible prior to
// the absolute need for Path-labeling.
func FilterRankingStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "FilterRankingStrategy"}
}

// IdentityRemovalStrategy looks for IdentityStep instances and removes them.
// If the identity step is labeled, its labels are added to the previous step.
// If the identity step is labeled and it's the first step in the traversal, it stays.
func IdentityRemovalStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "IdentityRemovalStrategy"}
}

// IncidentToAdjacentStrategy looks for .OutE().InV(), .InE().OutV() and .BothE().OtherV()
// and replaces these step sequences with .Out(), .In() or .Both() respectively.
// The strategy won't modify the traversal if:
//   the Edge step is labeled
//   the traversal contains a Path step
//   the traversal contains a Lambda step
func IncidentToAdjacentStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "IncidentToAdjacentStrategy"}
}

// InlineFilterStrategy analyzes filter-steps with child traversals that themselves are pure filters. If
// the child traversals are pure filters then the wrapping parent filter is not needed and thus, the children
// can be "inlined". Normalizing  pure filters with inlining reduces the number of variations of a filter that
// a graph provider may need to reason about when writing their own strategies. As a result, this strategy helps
// increase the likelihood that a provider's filtering optimization will succeed at re-writing the traversal.
func InlineFilterStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "InlineFilterStrategy"}
}

// LazyBarrierStrategy is an OLTP-only strategy that automatically inserts a NoOpBarrierStep after every
// FlatMapStep if neither Path-tracking nor partial Path-tracking is required, and the next step is not the
// traversal's last step or a Barrier. NoOpBarrierSteps allow Traversers to be bulked, thus this strategy
// is meant to reduce memory requirements and improve the overall query performance.
func LazyBarrierStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "LazyBarrierStrategy"}
}

// MatchPredicateStrategy will fold any post-Where() step that maintains a traversal constraint into
// Match(). MatchStep is intelligent with traversal constraint applications and thus, can more
// efficiently use the constraint of WhereTraversalStep or WherePredicateStep.
func MatchPredicateStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "MatchPredicateStrategy"}
}

// OrderLimitStrategy is an OLAP strategy that folds a RangeGlobalStep into a preceding
// OrderGlobalStep. This helps to eliminate traversers early in the traversal and can
// significantly reduce the amount of memory required by the OLAP execution engine.
func OrderLimitStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "OrderLimitStrategy"}
}

// PathProcessorStrategy  is an OLAP strategy that does its best to turn non-local children in Where()
// and Select() into local children by inlining components of the non-local child. In this way,
// PathProcessorStrategy helps to ensure that more traversals meet the local child constraint imposed
// on OLAP traversals.
func PathProcessorStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "PathProcessorStrategy"}
}

// PathRetractionStrategy will remove Paths from the Traversers and increase the likelihood of bulking
// as Path data is not required after Select('b').
func PathRetractionStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "PathRetractionStrategy"}
}

// ProductiveByStrategy takes an argument of by() and wraps it CoalesceStep so that the result is either
// the initial Traversal argument or null. In this way, the By() is always "productive". This strategy
// is an "optimization" but it is perhaps more of a "decoration", but it should follow
// ByModulatorOptimizationStrategy which features optimizations relevant to this one.
func ProductiveByStrategy(productiveKeys []string) *traversalStrategy {
	config := make(map[string]interface{})
	config["productiveKeys"] = productiveKeys

	return &traversalStrategy{name: optimizationNamespace + "ProductiveByStrategy", configuration: config}
}

// RepeatUnrollStrategy is an OLTP-only strategy that unrolls any RepeatStep if it uses a constant
// number of loops (Times(x)) and doesn't emit intermittent elements. If any of the following 3 steps appears
// within the Repeat-traversal, the strategy will not be applied:
//   DedupGlobalStep
//   LoopsStep
//   LambdaHolder
func RepeatUnrollStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "RepeatUnrollStrategy"}
}

//

func RemoteStrategy(connection DriverRemoteConnection) *traversalStrategy {
	a := func(g GraphTraversal) {
		result, err := g.getResults()
		if err != nil || result != nil {
			return
		}

		rs, err := connection.submitBytecode(g.bytecode)
		if err != nil {
			return
		}
		g.results = rs
	}

	return &traversalStrategy{name: "RemoteStrategy", apply: a}
}
