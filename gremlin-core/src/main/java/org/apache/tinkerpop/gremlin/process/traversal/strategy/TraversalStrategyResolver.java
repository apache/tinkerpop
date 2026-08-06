/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.strategy;

import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization.ComputerFinalizationStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.remote.traversal.strategy.decoration.RemoteStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ConnectiveStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SackStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SideEffectStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ReferenceElementStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ByModulatorOptimizationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.EarlyLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.FilterRankingStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.GValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IdentityRemovalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.LazyBarrierStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.MatchPredicateStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.OrderLimitStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathProcessorStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.PathRetractionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.ProductiveByStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.RepeatUnrollStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.provider.ProviderGValueReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.EdgeLabelVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReservedKeysVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.StandardVerificationStrategy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Resolves traversal strategy class names supplied by typed GraphSON and GraphBinary payloads against an explicit
 * registry. Deserializers use this class instead of direct class loading so that an incoming {@code fqcn} can only
 * name a strategy that is part of TinkerPop's built-in set or one that a provider added to the resolver.
 * <p>
 * There are two validation points for traversal strategy proxies. The serializer-specific resolver validates the
 * strategy name while bytes are being decoded. {@code JavaTranslator} later validates the resulting
 * {@link TraversalStrategyProxy} before invoking {@code instance()} or {@code create(Configuration)} on the strategy
 * class. The second check cannot refer back to the serializer instance that produced the proxy, so created resolvers
 * also contribute their allowed classes to a process-wide set used by {@code JavaTranslator}.
 */
public final class TraversalStrategyResolver {

    private static final Collection<Class<? extends TraversalStrategy>> DEFAULT_STRATEGIES = Collections.unmodifiableList(Arrays.asList(
            ConnectiveStrategy.class,
            ElementIdStrategy.class,
            EventStrategy.class,
            HaltedTraverserStrategy.class,
            OptionsStrategy.class,
            PartitionStrategy.class,
            RequirementsStrategy.class,
            SackStrategy.class,
            SubgraphStrategy.class,
            SeedStrategy.class,
            SideEffectStrategy.class,
            LazyBarrierStrategy.class,
            MatchAlgorithmStrategy.class,
            AdjacentToIncidentStrategy.class,
            ByModulatorOptimizationStrategy.class,
            ProductiveByStrategy.class,
            CountStrategy.class,
            GValueReductionStrategy.class,
            FilterRankingStrategy.class,
            IdentityRemovalStrategy.class,
            IncidentToAdjacentStrategy.class,
            InlineFilterStrategy.class,
            MatchPredicateStrategy.class,
            OrderLimitStrategy.class,
            PathProcessorStrategy.class,
            PathRetractionStrategy.class,
            RepeatUnrollStrategy.class,
            ProviderGValueReductionStrategy.class,
            ComputerVerificationStrategy.class,
            LambdaRestrictionStrategy.class,
            ReadOnlyStrategy.class,
            StandardVerificationStrategy.class,
            EarlyLimitStrategy.class,
            EdgeLabelVerificationStrategy.class,
            ReservedKeysVerificationStrategy.class,
            ReferenceElementStrategy.class,
            ComputerFinalizationStrategy.class,
            MessagePassingReductionStrategy.class,
            ProfileStrategy.class,
            VertexProgramRestrictionStrategy.class,
            GraphFilterStrategy.class,
            VertexProgramStrategy.class,
            RemoteStrategy.class
    ));

    /*
     * The global set is the union of strategy classes allowed by resolvers created in this JVM. It is not used by
     * deserializers, which validate against their own resolver instance. It exists for JavaTranslator, where bytecode
     * already contains a TraversalStrategyProxy and there is no serializer-local resolver to consult before reflective
     * strategy creation.
     */
    private static final Set<Class<? extends TraversalStrategy>> GLOBALLY_ALLOWED_STRATEGIES = ConcurrentHashMap.newKeySet();
    private static final TraversalStrategyResolver DEFAULT_RESOLVER = build().create();

    private final Map<String, Class<? extends TraversalStrategy>> strategiesByName;

    private TraversalStrategyResolver(final Collection<Class<? extends TraversalStrategy>> allowedStrategies) {
        final Map<String, Class<? extends TraversalStrategy>> strategies = new LinkedHashMap<>();
        allowedStrategies.forEach(strategy -> strategies.put(strategy.getName(), strategy));
        this.strategiesByName = Collections.unmodifiableMap(strategies);
    }

    /**
     * Creates a builder initialized with the default TinkerPop traversal strategy classes.
     */
    public static Builder build() {
        return new Builder();
    }

    /**
     * Gets the resolver for TinkerPop's default traversal strategy classes.
     */
    public static TraversalStrategyResolver defaultResolver() {
        return DEFAULT_RESOLVER;
    }

    /**
     * Determines whether a traversal strategy class has been allowed by any resolver created in this JVM. This check is
     * intended for code paths such as {@code JavaTranslator} that receive a {@link TraversalStrategyProxy} after
     * deserialization has already completed.
     */
    public static boolean isGloballyAllowed(final Class<?> strategyClass) {
        return GLOBALLY_ALLOWED_STRATEGIES.contains(strategyClass);
    }

    /**
     * Resolves a fully qualified class name to an allowed traversal strategy class.
     *
     * @throws IllegalArgumentException if the class name is not present in this resolver
     */
    public Class<? extends TraversalStrategy> resolve(final String fqcn) {
        final Class<? extends TraversalStrategy> strategyClass = strategiesByName.get(fqcn);
        if (null == strategyClass)
            throw new IllegalArgumentException(String.format("TraversalStrategy class is not allowed: %s", fqcn));

        return strategyClass;
    }

    /**
     * Determines whether the supplied class is allowed by this resolver.
     */
    public boolean isAllowed(final Class<?> strategyClass) {
        return null != strategyClass && strategiesByName.get(strategyClass.getName()) == strategyClass;
    }

    /**
     * Gets the traversal strategy classes allowed by this resolver.
     */
    public Collection<Class<? extends TraversalStrategy>> getAllowedStrategies() {
        return strategiesByName.values();
    }

    private static Optional<Class<? extends TraversalStrategy>> tryLoadSparqlStrategy() {
        try {
            final Class<?> sparqlStrategy = Class.forName(
                    "org.apache.tinkerpop.gremlin.sparql.process.traversal.strategy.SparqlStrategy",
                    false,
                    TraversalStrategyResolver.class.getClassLoader());
            if (TraversalStrategy.class.isAssignableFrom(sparqlStrategy))
                return Optional.of((Class<? extends TraversalStrategy>) sparqlStrategy);
        } catch (Exception ignored) {
            // optional module
        }

        return Optional.empty();
    }

    /**
     * Builds a traversal strategy resolver. Providers can add custom traversal strategy classes here so that remote
     * GraphSON and GraphBinary deserialization can resolve them.
     */
    public static final class Builder {
        private final Map<String, Class<? extends TraversalStrategy>> allowedStrategies = new LinkedHashMap<>();

        private Builder() {
            DEFAULT_STRATEGIES.forEach(this::addAllowedTraversalStrategy);
            tryLoadSparqlStrategy().ifPresent(this::addAllowedTraversalStrategy);
        }

        /**
         * Adds a traversal strategy class that can be resolved during deserialization.
         */
        public Builder addAllowedTraversalStrategy(final Class<? extends TraversalStrategy> strategyClass) {
            if (null == strategyClass)
                throw new IllegalArgumentException("The traversal strategy class cannot be null");

            allowedStrategies.put(strategyClass.getName(), strategyClass);
            return this;
        }

        /**
         * Adds traversal strategy classes that can be resolved during deserialization.
         */
        public Builder addAllowedTraversalStrategies(final Collection<Class<? extends TraversalStrategy>> strategyClasses) {
            strategyClasses.forEach(this::addAllowedTraversalStrategy);
            return this;
        }

        /**
         * Creates the resolver and records the allowed strategy classes for later validation by {@code JavaTranslator}.
         * The resolver-local allow-list is used during deserialization. The recorded global allow-list is used later
         * when bytecode translation reaches a {@link TraversalStrategyProxy} without access to the serializer-local
         * resolver that created it.
         */
        public TraversalStrategyResolver create() {
            GLOBALLY_ALLOWED_STRATEGIES.addAll(allowedStrategies.values());
            return new TraversalStrategyResolver(allowedStrategies.values());
        }
    }
}
