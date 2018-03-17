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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.ProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * A {@link TraversalStrategy} defines a particular atomic operation for mutating a {@link Traversal} prior to its evaluation.
 * There are 6 pre-defined "traversal categories": {@link DecorationStrategy}, {@link OptimizationStrategy}, {@link ProviderOptimizationStrategy}, {@link FinalizationStrategy}, and {@link VerificationStrategy}.
 * Strategies within a category are sorted amongst themselves and then category sorts are applied in the ordered specified previous.
 * That is, decorations are applied, then optimizations, then provider optimizations, then finalizations, and finally, verifications.
 * If a strategy does not fit within the specified categories, then it can simply implement {@link TraversalStrategy} and can have priors/posts that span categories.
 * <p/>
 * A traversal strategy should be a final class as various internal operations on a strategy are based on its ability to be assigned to more general classes.
 * A traversal strategy should typically be stateless with a public static <code>instance()</code> method.
 * However, at limit, a traversal strategy can have a state defining constructor (typically via a "builder"), but that state can not mutate once instantiated.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface TraversalStrategy<S extends TraversalStrategy> extends Serializable, Comparable<TraversalStrategy<?>> {

    public static final String STRATEGY = "strategy";

    public void apply(final Traversal.Admin<?, ?> traversal);

    /**
     * The set of strategies that must be executed before this strategy is executed.
     * If there are no ordering requirements, the default implementation returns an empty set.
     *
     * @return the set of strategies that must be executed prior to this one.
     */
    public default Set<Class<? extends S>> applyPrior() {
        return Collections.emptySet();
    }

    /**
     * The set of strategies that must be executed after this strategy is executed.
     * If there are no ordering requirements, the default implementation returns an empty set.
     *
     * @return the set of strategies that must be executed post this one
     */
    public default Set<Class<? extends S>> applyPost() {
        return Collections.emptySet();
    }

    /**
     * The type of traversal strategy -- i.e. {@link DecorationStrategy}, {@link OptimizationStrategy}, {@link FinalizationStrategy}, or {@link VerificationStrategy}.
     *
     * @return the traversal strategy category class
     */
    public default Class<S> getTraversalCategory() {
        return (Class) TraversalStrategy.class;
    }

    /**
     * Get the configuration representation of this strategy.
     * This is useful for converting a strategy into a serialized form.
     *
     * @return the configuration used to create this strategy
     */
    public default Configuration getConfiguration() {
        return new BaseConfiguration();
    }

    /**
     * Returns the "weight" of the class of this TraversalStrategy, used in {@link #compareTo}. It is recommended that
     * this method returns a constant, i. e. the same value for all instances of some specific TraversalStrategy
     * implementation class. Pre-defined "traversal categories" have specified "weights": {@link
     * DecorationStrategy#COMPARISON_WEIGHT}, {@link OptimizationStrategy#COMPARISON_WEIGHT}, {@link
     * ProviderOptimizationStrategy#COMPARISON_WEIGHT}, {@link FinalizationStrategy#COMPARISON_WEIGHT} and {@link
     * VerificationStrategy#COMPARISON_WEIGHT}.
     *
     * The default implementation of this method returns 0.
     */
    public default int getComparisonWeight() {
        return 0;
    }

    /**
     * TraversalStrategies are compared by their class, i. e. strategy1.compareTo(strategy2) always returns results of
     * the same sign, when strategy1 and strategy2 are any instances of some specific implementation classes of
     * TraversalStrategy. When strategy1 and strategy2 have the same class, strategy1.compareTo(strategy2) returns 0.
     * <p/>
     * This method is implemented as {@code
     * Integer.compare(this.getComparisonWeight(), otherTraversalCategory.getComparisonWeight())}, and it is not
     * recommended to override this implementation, {@link #getComparisonWeight()} should be overridden instead.
     */
    @Override
    public default int compareTo(final TraversalStrategy<?> otherTraversalCategory) {
        return Integer.compare(getComparisonWeight(), otherTraversalCategory.getComparisonWeight());
    }

    /**
     * Implemented by strategies that adds "application logic" to the traversal (e.g. {@link PartitionStrategy}).
     */
    public interface DecorationStrategy extends TraversalStrategy<DecorationStrategy> {

        public static final int COMPARISON_WEIGHT = 100;

        @Override
        public default Class<DecorationStrategy> getTraversalCategory() {
            return DecorationStrategy.class;
        }

        /**
         * Returns {@link #COMPARISON_WEIGHT}. It's not recommended to override this method.
         */
        @Override
        default int getComparisonWeight() {
            return COMPARISON_WEIGHT;
        }
    }

    /**
     * Implemented by strategies that rewrite the traversal to be more efficient, but with the same semantics
     * (e.g. {@link CountStrategy}). During a re-write ONLY TinkerPop steps should be used.
     * For strategies that utilize provider specific steps, use {@link ProviderOptimizationStrategy}.
     */
    public interface OptimizationStrategy extends TraversalStrategy<OptimizationStrategy> {

        public static final int COMPARISON_WEIGHT = 200;

        @Override
        public default Class<OptimizationStrategy> getTraversalCategory() {
            return OptimizationStrategy.class;
        }

        /**
         * Returns {@link #COMPARISON_WEIGHT}. It's not recommended to override this method.
         */
        @Override
        default int getComparisonWeight() {
            return COMPARISON_WEIGHT;
        }
    }

    /**
     * Implemented by strategies that rewrite the traversal to be more efficient, but with the same semantics.
     * This is for graph system/language/driver providers that want to rewrite a traversal using provider specific steps.
     */
    public interface ProviderOptimizationStrategy extends TraversalStrategy<ProviderOptimizationStrategy> {

        public static final int COMPARISON_WEIGHT = 300;

        @Override
        public default Class<ProviderOptimizationStrategy> getTraversalCategory() {
            return ProviderOptimizationStrategy.class;
        }

        /**
         * Returns {@link #COMPARISON_WEIGHT}. It's not recommended to override this method.
         */
        @Override
        default int getComparisonWeight() {
            return COMPARISON_WEIGHT;
        }
    }

    /**
     * Implemented by strategies that do final behaviors that require a fully compiled traversal to work (e.g.
     * {@link ProfileStrategy}).
     */
    public interface FinalizationStrategy extends TraversalStrategy<FinalizationStrategy> {

        public static final int COMPARISON_WEIGHT = 400;

        @Override
        public default Class<FinalizationStrategy> getTraversalCategory() {
            return FinalizationStrategy.class;
        }

        /**
         * Returns {@link #COMPARISON_WEIGHT}. It's not recommended to override this method.
         */
        @Override
        default int getComparisonWeight() {
            return COMPARISON_WEIGHT;
        }
    }

    /**
     * Implemented by strategies where there is no more behavioral tweaking of the traversal required.  Strategies that
     * implement this category will simply analyze the traversal and throw exceptions if the traversal is not correct
     * for the execution context (e.g. {@link LambdaRestrictionStrategy}).
     */
    public interface VerificationStrategy extends TraversalStrategy<VerificationStrategy> {

        public static final int COMPARISON_WEIGHT = 500;

        @Override
        public default Class<VerificationStrategy> getTraversalCategory() {
            return VerificationStrategy.class;
        }

        /**
         * Returns {@link #COMPARISON_WEIGHT}. It's not recommended to override this method.
         */
        @Override
        default int getComparisonWeight() {
            return COMPARISON_WEIGHT;
        }
    }
}
