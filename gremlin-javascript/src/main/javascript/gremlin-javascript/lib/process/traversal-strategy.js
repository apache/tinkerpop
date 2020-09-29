/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

/**
 * @author Jorge Bay Gondra
 */
'use strict';

const Traversal = require('./traversal').Traversal;

class TraversalStrategies {
  /**
   * Creates a new instance of TraversalStrategies.
   * @param {TraversalStrategies} [parent] The parent strategies from where to clone the values from.
   * @constructor
   */
  constructor(parent) {
    if (parent) {
      // Clone the strategies
      this.strategies = [...parent.strategies];
    }
    else {
      this.strategies = [];
    }
  }

  /** @param {TraversalStrategy} strategy */
  addStrategy(strategy) {
    this.strategies.push(strategy);
  }

  /**
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  applyStrategies(traversal) {
    // Apply all strategies serially
    return this.strategies.reduce((promise, strategy) => {
      return promise.then(() => strategy.apply(traversal));
    }, Promise.resolve());
  }
}

/** @abstract */
class TraversalStrategy {

  /**
   * @param {String} fqcn fully qualified class name in Java of the strategy
   * @param {Object} configuration for the strategy
   */
  constructor(fqcn, configuration = {}) {
    this.fqcn = fqcn;
    this.configuration = configuration;
  }

  /**
   * @abstract
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  apply(traversal) {

  }
}

class ConnectiveStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy");
  }
}

class ElementIdStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy");
  }
}

class HaltedTraverserStrategy extends TraversalStrategy {

  /**
   * @param {String} haltedTraverserFactory full qualified class name in Java of a {@code HaltedTraverserFactory} implementation
   */
  constructor(haltedTraverserFactory) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy");
    if (haltedTraverserFactory !== undefined)
      this.configuration["haltedTraverserFactory"] = haltedTraverserFactory;
  }
}

class OptionsStrategy extends TraversalStrategy {
  constructor(options) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy", options);
  }
}

class PartitionStrategy extends TraversalStrategy {
  /**
   * @param {Object} [options]
   * @param {String} [options.partitionKey] name of the property key to partition by
   * @param {String} [options.writePartition] the value of the currently write partition
   * @param {Array<String>} [options.readPartitions] list of strings representing the partitions to include for reads
   * @param {boolean} [options.includeMetaProperties] determines if meta-properties should be included in partitioning defaulting to false
   */
  constructor(options) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy", options);
  }
}

class SubgraphStrategy extends TraversalStrategy {
  /**
   * @param {Object} [options]
   * @param {GraphTraversal} [options.vertices] name of the property key to partition by
   * @param {GraphTraversal} [options.edges] the value of the currently write partition
   * @param {GraphTraversal} [options.vertexProperties] list of strings representing the partitions to include for reads
   * @param {boolean} [options.checkAdjacentVertices] enables the strategy to apply the {@code vertices} filter to the adjacent vertices of an edge.
   */
  constructor(options) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy", options);
    if (this.configuration.vertices instanceof Traversal)
      this.configuration.vertices = this.configuration.vertices.bytecode;
    if (this.configuration.edges instanceof Traversal)
      this.configuration.edges = this.configuration.edges.bytecode;
    if (this.configuration.vertexProperties instanceof Traversal)
      this.configuration.vertexProperties = this.configuration.vertexProperties.bytecode;
  }
}

class VertexProgramStrategy extends TraversalStrategy {

  constructor(options) {
    super("org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy", options);
  }
}

class MatchAlgorithmStrategy extends TraversalStrategy {
  /**
   * @param matchAlgorithm
   */
  constructor(matchAlgorithm) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy");
    if (matchAlgorithm !== undefined)
      this.configuration["matchAlgorithm"] = matchAlgorithm;
  }
}

class AdjacentToIncidentStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy");
  }
}

class FilterRankingStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy");
  }
}

class IdentityRemovalStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy");
  }
}

class IncidentToAdjacentStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy");
  }
}

class InlineFilterStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy");
  }
}

class LazyBarrierStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy");
  }
}

class MatchPredicateStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy");
  }
}

class OrderLimitStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy");
  }
}

class PathProcessorStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy");
  }
}

class PathRetractionStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy");
  }
}

class CountStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy");
  }
}

class RepeatUnrollStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy");
  }
}

class GraphFilterStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.GraphFilterStrategy");
  }
}

class EarlyLimitStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy");
  }
}

class LambdaRestrictionStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy");
  }
}

class ReadOnlyStrategy extends TraversalStrategy {
  constructor() {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy");
  }
}

class EdgeLabelVerificationStrategy extends TraversalStrategy {
  /**
   * @param {boolean} logWarnings determines if warnings should be written to the logger when verification fails
   * @param {boolean} throwException determines if exceptions should be thrown when verifications fails
   */
  constructor(logWarnings = false, throwException=false) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy",
        {logWarnings: logWarnings, throwException: throwException});
  }
}

class ReservedKeysVerificationStrategy extends TraversalStrategy {
  /**
   * @param {boolean} logWarnings determines if warnings should be written to the logger when verification fails
   * @param {boolean} throwException determines if exceptions should be thrown when verifications fails
   * @param {Array<String>} keys the list of reserved keys to verify
   */
  constructor(logWarnings = false, throwException=false, keys=["id", "label"]) {
    super("org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy",
        {logWarnings: logWarnings, throwException: throwException, keys: keys});
  }
}

module.exports = {
  TraversalStrategies: TraversalStrategies,
  TraversalStrategy: TraversalStrategy,
  // decoration
  ConnectiveStrategy: ConnectiveStrategy,
  ElementIdStrategy: ElementIdStrategy,
  HaltedTraverserStrategy: HaltedTraverserStrategy,
  OptionsStrategy: OptionsStrategy,
  PartitionStrategy: PartitionStrategy,
  SubgraphStrategy: SubgraphStrategy,
  VertexProgramStrategy: VertexProgramStrategy,
  // finalization
  MatchAlgorithmStrategy: MatchAlgorithmStrategy,
  // optimization
  AdjacentToIncidentStrategy: AdjacentToIncidentStrategy,
  FilterRankingStrategy: FilterRankingStrategy,
  IdentityRemovalStrategy: IdentityRemovalStrategy,
  IncidentToAdjacentStrategy: IncidentToAdjacentStrategy,
  InlineFilterStrategy: InlineFilterStrategy,
  LazyBarrierStrategy: LazyBarrierStrategy,
  MatchPredicateStrategy: MatchPredicateStrategy,
  OrderLimitStrategy: OrderLimitStrategy,
  PathProcessorStrategy: PathProcessorStrategy,
  PathRetractionStrategy: PathRetractionStrategy,
  CountStrategy: CountStrategy,
  RepeatUnrollStrategy: RepeatUnrollStrategy,
  GraphFilterStrategy: GraphFilterStrategy,
  EarlyLimitStrategy: EarlyLimitStrategy,
  // verification
  EdgeLabelVerificationStrategy: EdgeLabelVerificationStrategy,
  LambdaRestrictionStrategy: LambdaRestrictionStrategy,
  ReadOnlyStrategy: ReadOnlyStrategy,
  ReservedKeysVerificationStrategy: ReservedKeysVerificationStrategy
};