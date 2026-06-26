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

import type { RemoteConnection } from '../driver/remote-connection.js';
import { Traversal } from './traversal.js';

export class TraversalStrategies {
  readonly strategies: TraversalStrategy[];

  /**
   * Creates a new instance of TraversalStrategies.
   * @param {TraversalStrategies} [parent] The parent strategies from where to clone the values from.
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
    const idx = this.strategies.findIndex((s) => s.strategyName === strategy.strategyName);
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
  public strategyName: string;

  /**
   * @param {TraversalStrategyConfiguration} configuration for the strategy
   */
  constructor(
    public configuration: TraversalStrategyConfiguration = {},
  ) {
    this.strategyName = this.constructor.name;
  }

  /**
   * @abstract
   * @param {Traversal} traversal
   * @returns {Promise}
   */
  async apply(traversal: Traversal): Promise<void> {}
}

export class ConnectiveStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class ElementIdStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class HaltedTraverserStrategy extends TraversalStrategy {
  /**
   * @param {String} haltedTraverserFactory full qualified class name in Java of a `HaltedTraverserFactory` implementation
   */
  constructor({haltedTraverserFactory = ""}) {
    super({haltedTraverserFactory: haltedTraverserFactory});
  }
}

export class MessagePassingReductionStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class OptionsStrategy extends TraversalStrategy {
  constructor(options: TraversalStrategyConfiguration) {
    super(options);
  }
}

export class PartitionStrategy extends TraversalStrategy {
  /**
   * @param options
   * @param options.partitionKey - name of the property key to partition by
   * @param options.writePartition - the value of the currently write partition
   * @param options.readPartitions - list of strings representing the partitions to include for reads
   * @param options.includeMetaProperties - determines if meta-properties should be included in partitioning defaulting to false
   */
  constructor({partitionKey, writePartition, readPartitions, includeMetaProperties}: {partitionKey?: string, writePartition?: string, readPartitions?: string[], includeMetaProperties?: boolean} = {}) {
    const config: Record<string, any> = {};
    if (partitionKey !== undefined) config.partitionKey = partitionKey;
    if (writePartition !== undefined) config.writePartition = writePartition;
    if (readPartitions !== undefined) config.readPartitions = readPartitions;
    if (includeMetaProperties !== undefined) config.includeMetaProperties = includeMetaProperties;
    super(config);
  }
}

export class ProfileStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class SubgraphStrategy extends TraversalStrategy {
  /**
   * @param options
   * @param options.vertices - traversal to filter vertices
   * @param options.edges - traversal to filter edges
   * @param options.vertexProperties - traversal to filter vertex properties
   * @param options.checkAdjacentVertices - enables the strategy to apply the `vertices` filter to the adjacent vertices of an edge.
   */
  constructor({vertices, edges, vertexProperties, checkAdjacentVertices}: {vertices?: any, edges?: any, vertexProperties?: any, checkAdjacentVertices?: boolean} = {}) {
    const config: Record<string, any> = {};
    if (vertices !== undefined) config.vertices = vertices instanceof Traversal ? vertices.gremlinLang : vertices;
    if (edges !== undefined) config.edges = edges instanceof Traversal ? edges.gremlinLang : edges;
    if (vertexProperties !== undefined) config.vertexProperties = vertexProperties instanceof Traversal ? vertexProperties.gremlinLang : vertexProperties;
    if (checkAdjacentVertices !== undefined) config.checkAdjacentVertices = checkAdjacentVertices;
    super(config);
  }
}

export class ProductiveByStrategy extends TraversalStrategy {
  /**
   * @param options
   * @param options.productiveKeys - set of keys that will always be productive
   */
  constructor({productiveKeys = []} = {}) {
    super({productiveKeys});
  }
}

export class ReferenceElementStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class VertexProgramStrategy extends TraversalStrategy {
  constructor(options: TraversalStrategyConfiguration) {
    super(options);
  }
}

export class MatchAlgorithmStrategy extends TraversalStrategy {
  /**
   * @param matchAlgorithm
   */
  constructor({matchAlgorithm = ""}) {
    super({matchAlgorithm: matchAlgorithm});
  }
}

export class ComputerFinalizationStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class AdjacentToIncidentStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class FilterRankingStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class ByModulatorOptimizationStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class IdentityRemovalStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class IncidentToAdjacentStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class InlineFilterStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class LazyBarrierStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class MatchPredicateStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class OrderLimitStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class PathProcessorStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class PathRetractionStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class CountStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class RepeatUnrollStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class GraphFilterStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class EarlyLimitStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class ComputerVerificationStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class LambdaRestrictionStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class ReadOnlyStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class EdgeLabelVerificationStrategy extends TraversalStrategy {
  /**
   * @param options
   * @param options.logWarnings - determines if warnings should be written to the logger when verification fails
   * @param options.throwException - determines if exceptions should be thrown when verifications fails
   */
  constructor({logWarnings = false, throwException = false} = {}) {
    super({
      logWarnings: logWarnings,
      throwException: throwException,
    });
  }
}

export class ReservedKeysVerificationStrategy extends TraversalStrategy {
  /**
   * @param options
   * @param options.logWarnings - determines if warnings should be written to the logger when verification fails
   * @param options.throwException - determines if exceptions should be thrown when verifications fails
   * @param options.keys - the list of reserved keys to verify
   */
  constructor({ logWarnings = false, throwException = false, keys = ['id', 'label'] } = {}) {
    super({
      logWarnings: logWarnings,
      throwException: throwException,
      keys: keys,
    });
  }
}

export class VertexProgramRestrictionStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export class StandardVerificationStrategy extends TraversalStrategy {
  constructor() {
    super();
  }
}

export type SeedStrategyOptions = { seed: number };

export class SeedStrategy extends TraversalStrategy {
  /**
   * @param {SeedStrategyOptions} [options]
   * @param {number} [options.seed] the seed to provide to the random number generator for the traversal
   */
  constructor(options: SeedStrategyOptions) {
    super({
      seed: options.seed,
    });
  }
}
