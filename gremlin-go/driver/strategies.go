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
	baseNamespace         = "org.apache.tinkerpop.gremlin.process.traversal.strategy."
	decorationNamespace   = baseNamespace + "decoration."
	finalizationNamespace = baseNamespace + "finalization."
	optimizationNamespace = baseNamespace + "optimization."
	verificationNamespace = baseNamespace + "verification."
)

// Decoration strategies

func PartitionStrategy(partitionKey, writePartition, readPartitions, includeMetaProperties string) *traversalStrategy {
	config := make(map[string]interface{})
	if partitionKey != "" {
		config["partitionKey"] = partitionKey
	}
	if writePartition != "" {
		config["writePartition"] = writePartition
	}
	if readPartitions != "" {
		config["readPartitions"] = readPartitions
	}
	if includeMetaProperties != "" {
		config["includeMetaProperties"] = includeMetaProperties
	}
	return &traversalStrategy{name: decorationNamespace + "PartitionStrategy", configuration: config}
}

// Verification strategies

func ReadOnlyStrategy() *traversalStrategy {
	return &traversalStrategy{name: verificationNamespace + "ReadOnlyStrategy"}
}

// Optimization strategies

// AdjacentToIncidentStrategy looks for Vertex- and value-emitting steps followed by a CountGlobalStep and replaces
// the pattern with an edge- or property-emitting step followed by a CountGlobalStep. Furthermore, if a vertex-
// or value-emitting step is the last step in a .Has(traversal), .And(traversal, ...) or .Or(traversal, ...)
// child traversal, it is replaced by an appropriate edge- or property-emitting step.
// Performing this replacement removes situations where the more expensive trip to an adjacent graph element (e.g.
// the vertex on the other side of an edge) can be satisfied by trips to incident graph elements (e.g. just the edge
// itself).
func AdjacentToIncidentStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "AdjacentToIncidentStrategy"}
}

// ByModulatorOptimizationStrategy looks for standard traversals in by-modulators and replaces them with more
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
// push step labels as far "right" as possible in order to keep traversers as small and bulkable as possible prior to
// the absolute need for path-labeling.
func FilterRankingStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "FilterRankingStrategy"}
}

// IdentityRemovalStrategy looks for IdentityStep instances and removes them.
// If the identity step is labeled, its labels are added to the previous step.
// If the identity step is labeled and it's the first step in the traversal, it stays.
func IdentityRemovalStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "IdentityRemovalStrategy"}
}

// IncidentToAdjacentStrategy looks for .outE().inV(), .inE().outV() and .bothE().otherV()
// and replaces these step sequences with .out(), .in() or .both() respectively.
// The strategy won't modify the traversal if:
//   the edge step is labeled
//   the traversal contains a path step
//   the traversal contains a lambda step
func IncidentToAdjacentStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "IncidentToAdjacentStrategy"}
}

// InlineFilterStrategy analyzes filter-steps with child traversals that themselves are pure filters. If
// the child traversals are pure filters then the wrapping parent filter is not needed and thus, the children
// can be "inlined." Normalizing  pure filters with inlining reduces the number of variations of a filter that
// a graph provider may need to reason about when writing their own strategies. As a result, this strategy helps
// increase the likelihood that a provider's filtering optimization will succeed at re-writing the traversal.
func InlineFilterStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "InlineFilterStrategy"}
}

// LazyBarrierStrategy is an OLTP-only strategy that automatically inserts a NoOpBarrierStep after every
// FlatMapStep if neither path-tracking nor partial path-tracking is required, and the next step is not the
// traversal's last step or a Barrier. NoOpBarrierSteps allow traversers to be bulked, thus this strategy
// is meant to reduce memory requirements and improve the overall query performance.
func LazyBarrierStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "LazyBarrierStrategy"}
}

// MatchPredicateStrategy will fold any post-where() step that maintains a traversal constraint into
// match(). MatchStep is intelligent with traversal constraint applications and thus, can more
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

// PathProcessorStrategy  is an OLAP strategy that does its best to turn non-local children in where()
// and select() into local children by inlining components of the non-local child. In this way,
// PathProcessorStrategy helps to ensure that more traversals meet the local child constraint imposed
// on OLAP traversals.
func PathProcessorStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "PathProcessorStrategy"}
}

// PathRetractionStrategy will remove paths from the traversers and increase the likelihood of bulking
// as path data is not required after select('b').
func PathRetractionStrategy() *traversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "PathRetractionStrategy"}
}

// ProductiveByStrategy takes an argument of by() and wraps it CoalesceStep so that the result is either
// the initial Traversal argument or null. In this way, the by() is always "productive". This strategy
// is an "optimization" but it is perhaps more of a "decoration", but it should follow
// ByModulatorOptimizationStrategy which features optimizations relevant to this one.
func ProductiveByStrategy(productiveKeys []string) *traversalStrategy {
	config := make(map[string]interface{})
	config["productiveKeys"] = productiveKeys

	return &traversalStrategy{name: optimizationNamespace + "ProductiveByStrategy", configuration: config}
}

// RepeatUnrollStrategy is an OLTP-only strategy that unrolls any RepeatStep if it uses a constant
// number of loops (times(x)) and doesn't emit intermittent elements. If any of the following 3 steps appears
// within the repeat-traversal, the strategy will not be applied:
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
