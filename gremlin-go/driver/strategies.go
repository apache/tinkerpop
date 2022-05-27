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

type TraversalStrategy interface {
}

type traversalStrategy struct {
	name          string
	configuration map[string]interface{}
	apply         func(g GraphTraversal)
}

// Decoration strategies

// ConnectiveStrategy rewrites the binary conjunction form of a.And().b into a AndStep of
// And(a,b) (likewise for OrStep).
func ConnectiveStrategy() TraversalStrategy {
	return &traversalStrategy{name: decorationNamespace + "ConnectiveStrategy"}
}

// ElementIdStrategy provides a degree of control over element identifier assignment as some Graphs don't provide
// that feature. This strategy provides for identifier assignment by enabling users to utilize Vertex and Edge indices
// under the hood, thus simulating that capability.
// By default, when an identifier is not supplied by the user, newly generated identifiers are UUID objects.
func ElementIdStrategy() TraversalStrategy {
	return &traversalStrategy{name: decorationNamespace + "ElementIdStrategy"}
}

func HaltedTraverserStrategy(config HaltedTraverserStrategyConfig) TraversalStrategy {
	configMap := make(map[string]interface{})
	if config.HaltedTraverserFactoryName != "" {
		configMap["haltedTraverserFactory"] = config.HaltedTraverserFactoryName
	}
	return &traversalStrategy{name: decorationNamespace + "HaltedTraverserStrategy", configuration: configMap}
}

// HaltedTraverserStrategyConfig provides configuration options for HaltedTraverserStrategy.
// Zeroed (unset) values are ignored.
type HaltedTraverserStrategyConfig struct {
	HaltedTraverserFactoryName string
}

// OptionsStrategy will not alter the Traversal. It is only a holder for configuration options associated with the
// Traversal meant to be accessed by steps or other classes that might have some interaction with it. It is
// essentially a way for users to provide Traversal level configuration options that can be used in various ways
// by different Graph providers.
func OptionsStrategy(options map[string]interface{}) TraversalStrategy {
	return &traversalStrategy{name: decorationNamespace + "OptionsStrategy", configuration: options}
}

// PartitionStrategy partitions the Vertices, Edges and Vertex properties of a Graph into String named
// partitions (i.e. buckets, subgraphs, etc.). It blinds a Traversal from "seeing" specified areas of
// the graph. The Traversal will ignore all Graph elements not in those "read" partitions.
func PartitionStrategy(config PartitionStrategyConfig) TraversalStrategy {
	configMap := map[string]interface{}{"includeMetaProperties": config.IncludeMetaProperties}
	if config.PartitionKey != "" {
		configMap["partitionKey"] = config.PartitionKey
	}
	if config.WritePartition != "" {
		configMap["writePartition"] = config.WritePartition
	}
	if len(config.ReadPartitions) != 0 {
		configMap["readPartitions"] = config.ReadPartitions
	}
	return &traversalStrategy{name: decorationNamespace + "PartitionStrategy", configuration: configMap}
}

// PartitionStrategyConfig provides configuration options for PartitionStrategy.
// Zeroed (unset) values are ignored except IncludeMetaProperties.
type PartitionStrategyConfig struct {
	PartitionKey          string
	WritePartition        string
	ReadPartitions        []string
	IncludeMetaProperties bool
}

// SeedStrategy resets the specified Seed value for Seedable steps, which in turn will produce deterministic
// results from those steps. It is important to note that when using this strategy that it only guarantees
// deterministic results from a step but not from an entire Traversal. For example, if a Graph does no guarantee
// iteration order for g.V() then repeated runs of g.V().Coin(0.5) with this strategy will return the same number
// of results but not necessarily the same ones. The same problem can occur in OLAP-based Taversals where
// iteration order is not explicitly guaranteed. The only way to ensure completely deterministic results in that
// sense is to apply some form of order() in these cases.
func SeedStrategy(config SeedStrategyConfig) TraversalStrategy {
	configMap := map[string]interface{}{"seed": config.Seed}
	return &traversalStrategy{name: decorationNamespace + "SeedStrategy", configuration: configMap}
}

// SeedStrategyConfig provides configuration options for SeedStrategy. Zeroed (unset) values are used.
type SeedStrategyConfig struct {
	Seed int64
}

// SubgraphStrategy provides a way to limit the view of a Traversal. By providing Traversal representations that
// represent a form of filtering criterion for Vertices and/or Edges, this strategy will inject that criterion into
// the appropriate places of a Traversal thus restricting what it Traverses and returns.
func SubgraphStrategy(config SubgraphStrategyConfig) TraversalStrategy {
	configMap := make(map[string]interface{})
	if config.Vertices != nil {
		configMap["vertices"] = config.Vertices
	}
	if config.Edges != nil {
		configMap["edges"] = config.Edges
	}
	if config.VertexProperties != nil {
		configMap["vertexProperties"] = config.VertexProperties
	}
	if config.CheckAdjacentVertices != nil {
		configMap["checkAdjacentVertices"] = config.CheckAdjacentVertices.(bool)
	}
	return &traversalStrategy{name: decorationNamespace + "SubgraphStrategy", configuration: configMap}
}

// SubgraphStrategyConfig provides configuration options for SubgraphStrategy. Zeroed (unset) values are ignored.
type SubgraphStrategyConfig struct {
	Vertices              *GraphTraversal
	Edges                 *GraphTraversal
	VertexProperties      *GraphTraversal
	CheckAdjacentVertices interface{}
}

func VertexProgramStrategy(config VertexProgramStrategyConfig) TraversalStrategy {
	configMap := make(map[string]interface{})
	if config.GraphComputer != "" {
		configMap["graphComputer"] = config.GraphComputer
	}
	if config.Workers != 0 {
		configMap["workers"] = config.Workers
	}
	if config.Persist != "" {
		configMap["persist"] = config.Persist
	}
	if config.Result != "" {
		configMap["result"] = config.Result
	}
	if config.Vertices != nil {
		configMap["vertices"] = config.Vertices
	}
	if config.Edges != nil {
		configMap["edges"] = config.Edges
	}
	for k, v := range config.Configuration {
		configMap[k] = v
	}
	return &traversalStrategy{name: computerDecorationNamespace + "VertexProgramStrategy", configuration: configMap}
}

// VertexProgramStrategyConfig provides configuration options for VertexProgramStrategy.
// Zeroed (unset) values are ignored.
type VertexProgramStrategyConfig struct {
	GraphComputer string
	Workers       int
	Persist       string
	Result        string
	Vertices      *GraphTraversal
	Edges         *GraphTraversal
	Configuration map[string]interface{}
}

// Finalization strategies

func MatchAlgorithmStrategy(config MatchAlgorithmStrategyConfig) TraversalStrategy {
	configMap := make(map[string]interface{})
	if config.MatchAlgorithm != "" {
		configMap["matchAlgorithm"] = config.MatchAlgorithm
	}
	return &traversalStrategy{name: finalizationNamespace + "MatchAlgorithmStrategy", configuration: configMap}
}

// MatchAlgorithmStrategyConfig provides configuration options for MatchAlgorithmStrategy.
// Zeroed (unset) values are ignored.
type MatchAlgorithmStrategyConfig struct {
	MatchAlgorithm string
}

// Verification strategies

// EdgeLabelVerificationStrategy does not allow Edge traversal steps to have no label specified.
// Providing one or more labels is considered to be a best practice, however, TinkerPop will not force
// the specification of Edge labels; instead, providers or users will have to enable this strategy explicitly.
func EdgeLabelVerificationStrategy(config EdgeLabelVerificationStrategyConfig) TraversalStrategy {
	configMap := map[string]interface{}{
		"logWarning":     config.LogWarning,
		"throwException": config.ThrowExcecption,
	}

	return &traversalStrategy{name: verificationNamespace + "EdgeLabelVerificationStrategy", configuration: configMap}
}

// EdgeLabelVerificationStrategyConfig provides configuration options for EdgeLabelVerificationStrategy.
// Zeroed (unset) values are used.
type EdgeLabelVerificationStrategyConfig struct {
	LogWarning      bool
	ThrowExcecption bool
}

// LambdaRestrictionStrategy does not allow lambdas to be used in a Traversal. The contents of a lambda
// cannot be analyzed/optimized and thus, reduces the ability of other traversalStrategy instances to reason
// about the traversal. This strategy is not activated by default. However, graph system providers may choose
// to make this a default strategy in order to ensure their respective strategies are better able to operate.
func LambdaRestrictionStrategy() TraversalStrategy {
	return &traversalStrategy{name: verificationNamespace + "LambdaRestrictionStrategy"}
}

// ReadOnlyStrategy detects steps marked with Mutating and returns an error if one is found.
func ReadOnlyStrategy() TraversalStrategy {
	return &traversalStrategy{name: verificationNamespace + "ReadOnlyStrategy"}
}

// ReservedKeysVerificationStrategy detects property keys that should not be used by the traversal.
// A term may be reserved by a particular graph implementation or as a convention given best practices.
func ReservedKeysVerificationStrategy(config ReservedKeysVerificationStrategyConfig) TraversalStrategy {
	configMap := map[string]interface{}{
		"logWarning":     config.LogWarning,
		"throwException": config.ThrowException,
	}
	if len(config.Keys) != 0 {
		configMap["keys"] = config.Keys
	}
	return &traversalStrategy{name: verificationNamespace + "ReservedKeysVerificationStrategy", configuration: configMap}
}

// ReservedKeysVerificationStrategyConfig provides configuration options for ReservedKeysVerificationStrategy.
// Zeroed (unset) values are used except Keys.
type ReservedKeysVerificationStrategyConfig struct {
	LogWarning     bool
	ThrowException bool
	Keys           []string
}

// Optimization strategies

// AdjacentToIncidentStrategy looks for Vertex- and value-emitting steps followed by a CountGlobalStep and replaces
// the pattern with an Edge- or Property-emitting step followed by a CountGlobalStep. Furthermore, if a vertex-
// or value-emitting step is the last step in a .Has(traversal), .And(traversal, ...) or .Or(traversal, ...)
// child traversal, it is replaced by an appropriate Edge- or Property-emitting step.
// Performing this replacement removes situations where the more expensive trip to an adjacent Graph Element (e.g.
// the Vertex on the other side of an Edge) can be satisfied by trips to incident Graph Elements (e.g. just the Edge
// itself).
func AdjacentToIncidentStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "AdjacentToIncidentStrategy"}
}

// ByModulatorOptimizationStrategy looks for standard traversals in By-modulators and replaces them with more
// optimized traversals (e.g. TokenTraversal) if possible.
func ByModulatorOptimizationStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "ByModulatorOptimizationStrategy"}
}

// CountStrategy optimizes any occurrence of CountGlobalStep followed by an IsStep The idea is to limit
// the number of incoming elements in a way that it's enough for the IsStep to decide whether it evaluates
// true or false. If the traversal already contains a user supplied limit, the strategy won't modify it.
func CountStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "CountStrategy"}
}

// EarlyLimitStrategy looks for RangeGlobalSteps that can be moved further left in the traversal and thus be applied
// earlier. It will also try to merge multiple RangeGlobalSteps into one.
// If the logical consequence of one or multiple RangeGlobalSteps is an empty result, the strategy will remove
// as many steps as possible and add a NoneStep instead.
func EarlyLimitStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "EarlyLimitStrategy"}
}

// FilterRankingStrategy reorders filter- and order-steps according to their rank. Step ranks are defined within
// the strategy and indicate when it is reasonable for a step to move in front of another. It will also do its best to
// push step labels as far "right" as possible in order to keep Traversers as small and bulkable as possible prior to
// the absolute need for Path-labeling.
func FilterRankingStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "FilterRankingStrategy"}
}

// IdentityRemovalStrategy looks for IdentityStep instances and removes them.
// If the identity step is labeled, its labels are added to the previous step.
// If the identity step is labeled and it's the first step in the traversal, it stays.
func IdentityRemovalStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "IdentityRemovalStrategy"}
}

// IncidentToAdjacentStrategy looks for .OutE().InV(), .InE().OutV() and .BothE().OtherV()
// and replaces these step sequences with .Out(), .In() or .Both() respectively.
// The strategy won't modify the traversal if:
//   the Edge step is labeled
//   the traversal contains a Path step
//   the traversal contains a Lambda step
func IncidentToAdjacentStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "IncidentToAdjacentStrategy"}
}

// InlineFilterStrategy analyzes filter-steps with child traversals that themselves are pure filters. If
// the child traversals are pure filters then the wrapping parent filter is not needed and thus, the children
// can be "inlined". Normalizing  pure filters with inlining reduces the number of variations of a filter that
// a graph provider may need to reason about when writing their own strategies. As a result, this strategy helps
// increase the likelihood that a provider's filtering optimization will succeed at re-writing the traversal.
func InlineFilterStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "InlineFilterStrategy"}
}

// LazyBarrierStrategy is an OLTP-only strategy that automatically inserts a NoOpBarrierStep after every
// FlatMapStep if neither Path-tracking nor partial Path-tracking is required, and the next step is not the
// traversal's last step or a barrier. NoOpBarrierSteps allow Traversers to be bulked, thus this strategy
// is meant to reduce memory requirements and improve the overall query performance.
func LazyBarrierStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "LazyBarrierStrategy"}
}

// MatchPredicateStrategy will fold any post-Where() step that maintains a traversal constraint into
// Match(). MatchStep is intelligent with traversal constraint applications and thus, can more
// efficiently use the constraint of WhereTraversalStep or WherePredicateStep.
func MatchPredicateStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "MatchPredicateStrategy"}
}

// OrderLimitStrategy is an OLAP strategy that folds a RangeGlobalStep into a preceding
// OrderGlobalStep. This helps to eliminate traversers early in the traversal and can
// significantly reduce the amount of memory required by the OLAP execution engine.
func OrderLimitStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "OrderLimitStrategy"}
}

// PathProcessorStrategy  is an OLAP strategy that does its best to turn non-local children in Where()
// and Select() into local children by inlining components of the non-local child. In this way,
// PathProcessorStrategy helps to ensure that more traversals meet the local child constraint imposed
// on OLAP traversals.
func PathProcessorStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "PathProcessorStrategy"}
}

// PathRetractionStrategy will remove Paths from the Traversers and increase the likelihood of bulking
// as Path data is not required after Select('b').
func PathRetractionStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "PathRetractionStrategy"}
}

// ProductiveByStrategy takes an argument of By() and wraps it CoalesceStep so that the result is either
// the initial Traversal argument or null. In this way, the By() is always "productive". This strategy
// is an "optimization" but it is perhaps more of a "decoration", but it should follow
// ByModulatorOptimizationStrategy which features optimizations relevant to this one.
func ProductiveByStrategy(config ProductiveByStrategyConfig) TraversalStrategy {
	configMap := make(map[string]interface{})
	configMap["productiveKeys"] = config.ProductiveKeys

	return &traversalStrategy{name: optimizationNamespace + "ProductiveByStrategy", configuration: configMap}
}

// ProductiveByStrategyConfig provides configuration options for ProductiveByStrategy.
// Zeroed (unset) values are used.
type ProductiveByStrategyConfig struct {
	ProductiveKeys []string
}

// RepeatUnrollStrategy is an OLTP-only strategy that unrolls any RepeatStep if it uses a constant
// number of loops (Times(x)) and doesn't emit intermittent elements. If any of the following 3 steps appears
// within the Repeat-traversal, the strategy will not be applied:
//   DedupGlobalStep
//   LoopsStep
//   LambdaHolder
func RepeatUnrollStrategy() TraversalStrategy {
	return &traversalStrategy{name: optimizationNamespace + "RepeatUnrollStrategy"}
}

// RemoteStrategy reconstructs a Traversal by appending a RemoteStep to its end. That step will submit the Traversal to
// a RemoteConnection instance which will typically send it to a remote server for execution and return results.
func RemoteStrategy(connection DriverRemoteConnection) TraversalStrategy {
	a := func(g GraphTraversal) {
		result, err := g.GetResultSet()
		if err != nil || result != nil {
			return
		}

		rs, err := connection.submitBytecode(g.Bytecode)
		if err != nil {
			return
		}
		g.results = rs
	}

	return &traversalStrategy{name: "RemoteStrategy", apply: a}
}
