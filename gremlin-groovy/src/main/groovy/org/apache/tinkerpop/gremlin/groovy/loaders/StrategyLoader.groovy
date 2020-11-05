/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.groovy.loaders

import org.apache.commons.configuration.MapConfiguration
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class StrategyLoader {

    static void load() {
        // EventStrategy/SackStrategy are pretty much the oddballs here along with a few pure "internal" strategies
        // which don't have create(Configuration). We don't support either EventStrategy or SackStrategy in non-JVM
        // variants because of their style of usage (JVM specific class structures and lambdas respectively). this
        // perhaps points to a shortcoming somewhere in the APIs, though i'm not sure EventStrategy would ever work
        // properly off the JVM and SackStrategy doesn't need to since it's technically like OptionsStrategy which is
        // constructed via bytecode translation where the lambdas are readily supported.

        // decoration
        // # ConnectiveStrategy is singleton
        ElementIdStrategy.metaClass.constructor << { Map conf -> ElementIdStrategy.create(new MapConfiguration(conf)) }
        //EventStrategy.metaClass.constructor << { Map conf -> EventStrategy.create(new MapConfiguration(conf)) }
        HaltedTraverserStrategy.metaClass.constructor << { Map conf -> HaltedTraverserStrategy.create(new MapConfiguration(conf)) }
        OptionsStrategy.metaClass.constructor << { Map conf -> OptionsStrategy.create(new MapConfiguration(conf)) }
        PartitionStrategy.metaClass.constructor << { Map conf -> PartitionStrategy.create(new MapConfiguration(conf)) }
        // # RequirementsStrategy is internal
        // SackStrategy.metaClass.constructor << { Map conf -> SackStrategy.create(new MapConfiguration(conf)) }
        // # SideEffectStrategy is internal
        SubgraphStrategy.metaClass.constructor << { Map conf -> SubgraphStrategy.create(new MapConfiguration(conf)) }

        // finalization
        MatchAlgorithmStrategy.metaClass.constructor << { Map conf -> MatchAlgorithmStrategy.create(new MapConfiguration(conf)) }
        // # ProfileStrategy is singleton/internal
        // # ReferenceElementStrategy is singleton/internal

        // optimization
        // # AdjacentToIncidentStrategy is singleton/internal
        // # CountStrategy is singleton/internal
        // # EarlyLimitStrategy is singleton/internal
        // # FilterRankingStrategy is singleton/internal
        // # IdentityRemovalStrategy is singleton/internal
        // # IncidentToAdjacentStrategy is singleton/internal
        // # InlineFilterStrategy is singleton/internal
        // # LazyBarrierStrategy is singleton/internal
        // # MatchPredicateStrategy is singleton/internal
        // # OrderLimitStrategy is singleton/internal
        // # PathProcessorStrategy is singleton/internal
        // # PathRetractionStrategy is singleton/internal
        // # RepeatUnrollStrategy is singleton/internal

        // verification
        // # ComputerVerificationStrategy is singleton/internal
        EdgeLabelVerificationStrategy.metaClass.constructor << { Map conf -> EdgeLabelVerificationStrategy.create(new MapConfiguration(conf)) }
        // # LambdaRestrictionStrategy is singleton
        // # ReadOnlyStrategy is singleton
        ReservedKeysVerificationStrategy.metaClass.constructor << { Map conf -> ReservedKeysVerificationStrategy.create(new MapConfiguration(conf)) }
        // # StandardVerificationStrategy is singleton/internal
    }

}
