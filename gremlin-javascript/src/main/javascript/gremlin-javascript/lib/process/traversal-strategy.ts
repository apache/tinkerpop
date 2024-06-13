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

import { RemoteConnection } from '../driver/remote-connection.js';
import { Traversal } from './traversal.js';

export class TraversalStrategies {
  readonly strategies: TraversalStrategy[];

  /**
   * Creates a new instance of TraversalStrategies.
   * @param {TraversalStrategies} [parent] The parent strategies from where to clone the values from.
   * @constructor
   */
  constructor(parent?: TraversalStrategies) {
    if (parent) {
      // Clone the strategies
      this.strategies = [...parent.strategies];
    } else {
      this.strategies = [];
    }
  }

  /** @param {TraversalStrategy} strategy */
  addStrategy(strategy: TraversalStrategy) {
    this.strategies.push(strategy);
  }

  /** @param {TraversalStrategy} strategy */
  removeStrategy(strategy: TraversalStrategy) {
    const idx = this.strategies.findIndex((s) => s.fqcn === strategy.fqcn);
    if (idx !== -1) {
      return this.strategies.splice(idx, 1)[0];
    }

    return undefined;
  }

  /**
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  applyStrategies(traversal: Traversal) {
    // Apply all strategies serially
    return this.strategies.reduce(
      (promise, strategy) => promise.then(() => strategy.apply(traversal)),
      Promise.resolve(),
    );
  }
}

export type TraversalStrategyConfiguration = any;

export abstract class TraversalStrategy {
  connection?: RemoteConnection;

  /**
   * @param {String} fqcn fully qualified class name in Java of the strategy
   * @param {TraversalStrategyConfiguration} configuration for the strategy
   */
  constructor(
    public fqcn: string,
    public configuration: TraversalStrategyConfiguration = {},
  ) {}

  /**
   * @abstract
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  async apply(traversal: Traversal): Promise<void> {}
}

export class ConnectiveStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy');
  }
}

export class ElementIdStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy');
  }
}

export class HaltedTraverserStrategy extends TraversalStrategy {
  /**
   * @param {String} haltedTraverserFactory full qualified class name in Java of a {@code HaltedTraverserFactory} implementation
   */
  constructor(haltedTraverserFactory: string) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy');
    if (haltedTraverserFactory !== undefined) {
      this.configuration['haltedTraverserFactory'] = haltedTraverserFactory;
    }
  }
}

export class OptionsStrategy extends TraversalStrategy {
  constructor(options: TraversalStrategyConfiguration) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy', options);
  }
}

export class PartitionStrategy extends TraversalStrategy {
  /**
   * @param {Object} [options]
   * @param {String} [options.partitionKey] name of the property key to partition by
   * @param {String} [options.writePartition] the value of the currently write partition
   * @param {Array<String>} [options.readPartitions] list of strings representing the partitions to include for reads
   * @param {boolean} [options.includeMetaProperties] determines if meta-properties should be included in partitioning defaulting to false
   */
  constructor(options: TraversalStrategyConfiguration) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy', options);
  }
}

export class SubgraphStrategy extends TraversalStrategy {
  /**
   * @param {Object} [options]
   * @param {GraphTraversal} [options.vertices] name of the property key to partition by
   * @param {GraphTraversal} [options.edges] the value of the currently write partition
   * @param {GraphTraversal} [options.vertexProperties] list of strings representing the partitions to include for reads
   * @param {boolean} [options.checkAdjacentVertices] enables the strategy to apply the {@code vertices} filter to the adjacent vertices of an edge.
   */
  constructor(options: TraversalStrategyConfiguration) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy', options);
    if (this.configuration.vertices instanceof Traversal) {
      this.configuration.vertices = this.configuration.vertices.bytecode;
    }
    if (this.configuration.edges instanceof Traversal) {
      this.configuration.edges = this.configuration.edges.bytecode;
    }
    if (this.configuration.vertexProperties instanceof Traversal) {
      this.configuration.vertexProperties = this.configuration.vertexProperties.bytecode;
    }
  }
}

export class ProductiveByStrategy extends TraversalStrategy {
  /**
   * @param {Object} [options]
   * @param {Array<String>} [options.productiveKeys] set of keys that will always be productive
   */
  constructor(options: TraversalStrategyConfiguration) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy', options);
  }
}

export class VertexProgramStrategy extends TraversalStrategy {
  constructor(options: TraversalStrategyConfiguration) {
    super('org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy', options);
  }
}

export class MatchAlgorithmStrategy extends TraversalStrategy {
  /**
   * @param matchAlgorithm
   */
  constructor(matchAlgorithm: string) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy');
    if (matchAlgorithm !== undefined) {
      this.configuration['matchAlgorithm'] = matchAlgorithm;
    }
  }
}

export class AdjacentToIncidentStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy');
  }
}

export class FilterRankingStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy');
  }
}

export class IdentityRemovalStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy');
  }
}

export class IncidentToAdjacentStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy');
  }
}

export class InlineFilterStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy');
  }
}

export class LazyBarrierStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy');
  }
}

export class MatchPredicateStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy');
  }
}

export class OrderLimitStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy');
  }
}

export class PathProcessorStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy');
  }
}

export class PathRetractionStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy');
  }
}

export class CountStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy');
  }
}

export class RepeatUnrollStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy');
  }
}

export class GraphFilterStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.GraphFilterStrategy');
  }
}

export class EarlyLimitStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy');
  }
}

export class LambdaRestrictionStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy');
  }
}

export class ReadOnlyStrategy extends TraversalStrategy {
  constructor() {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy');
  }
}

export class EdgeLabelVerificationStrategy extends TraversalStrategy {
  /**
   * @param {boolean} logWarnings determines if warnings should be written to the logger when verification fails
   * @param {boolean} throwException determines if exceptions should be thrown when verifications fails
   */
  constructor(logWarnings = false, throwException = false) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy', {
      logWarnings: logWarnings,
      throwException: throwException,
    });
  }
}

export class ReservedKeysVerificationStrategy extends TraversalStrategy {
  /**
   * @param {boolean} logWarnings determines if warnings should be written to the logger when verification fails
   * @param {boolean} throwException determines if exceptions should be thrown when verifications fails
   * @param {Array<String>} keys the list of reserved keys to verify
   */
  constructor(logWarnings = false, throwException = false, keys = ['id', 'label']) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy', {
      logWarnings: logWarnings,
      throwException: throwException,
      keys: keys,
    });
  }
}

export type SeedStrategyOptions = { seed: number };

export class SeedStrategy extends TraversalStrategy {
  /**
   * @param {SeedStrategyOptions} [options]
   * @param {number} [options.seed] the seed to provide to the random number generator for the traversal
   */
  constructor(options: SeedStrategyOptions) {
    super('org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy', {
      seed: options.seed,
    });
  }
}
