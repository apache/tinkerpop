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
package org.apache.tinkerpop.machine.strategy;

import org.apache.tinkerpop.machine.bytecode.Bytecode;

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public interface Strategy<S extends Strategy> extends Serializable, Comparable<Class<? extends Strategy>> {

    public <C> void apply(final Bytecode<C> bytecode);

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
    public default Class<S> getStrategyCategory() {
        return (Class) Strategy.class;
    }

    @Override
    public default int compareTo(final Class<? extends Strategy> otherStrategyCategory) {
        return 0;
    }

    /**
     * Implemented by strategies that adds "application logic" to the traversal.
     */
    public interface DecorationStrategy extends Strategy<DecorationStrategy> {

        @Override
        public default Class<DecorationStrategy> getStrategyCategory() {
            return DecorationStrategy.class;
        }

        @Override
        public default int compareTo(final Class<? extends Strategy> otherTraversalCategory) {
            if (otherTraversalCategory.equals(DecorationStrategy.class))
                return 0;
            else if (otherTraversalCategory.equals(OptimizationStrategy.class))
                return -1;
            else if (otherTraversalCategory.equals(ProviderStrategy.class))
                return -1;
            else if (otherTraversalCategory.equals(FinalizationStrategy.class))
                return -1;
            else if (otherTraversalCategory.equals(VerificationStrategy.class))
                return -1;
            else
                return 0;
        }
    }

    /**
     * Implemented by strategies that rewrite the traversal to be more efficient, but with the same semantics.
     * During a re-write ONLY TinkerPop steps should be used.
     * For strategies that utilize provider specific steps, use {@link ProviderStrategy}.
     */
    public interface OptimizationStrategy extends Strategy<OptimizationStrategy> {

        @Override
        public default Class<OptimizationStrategy> getStrategyCategory() {
            return OptimizationStrategy.class;
        }

        @Override
        public default int compareTo(final Class<? extends Strategy> otherTraversalCategory) {
            if (otherTraversalCategory.equals(DecorationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(OptimizationStrategy.class))
                return 0;
            else if (otherTraversalCategory.equals(ProviderStrategy.class))
                return -1;
            else if (otherTraversalCategory.equals(FinalizationStrategy.class))
                return -1;
            else if (otherTraversalCategory.equals(VerificationStrategy.class))
                return -1;
            else
                return 0;
        }
    }

    /**
     * Implemented by strategies that rewrite the traversal to be more efficient, but with the same semantics.
     * This is for graph system/language/driver providers that want to rewrite a traversal using provider specific steps.
     */
    public interface ProviderStrategy extends Strategy<ProviderStrategy> {

        @Override
        public default Class<ProviderStrategy> getStrategyCategory() {
            return ProviderStrategy.class;
        }

        @Override
        public default int compareTo(final Class<? extends Strategy> otherTraversalCategory) {
            if (otherTraversalCategory.equals(DecorationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(OptimizationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(ProviderStrategy.class))
                return 0;
            else if (otherTraversalCategory.equals(FinalizationStrategy.class))
                return -1;
            else if (otherTraversalCategory.equals(VerificationStrategy.class))
                return -1;
            else
                return 0;
        }
    }

    /**
     * Implemented by strategies that do final behaviors that require a fully compiled traversal to work.
     */
    public interface FinalizationStrategy extends Strategy<FinalizationStrategy> {

        @Override
        public default Class<FinalizationStrategy> getStrategyCategory() {
            return FinalizationStrategy.class;
        }

        @Override
        public default int compareTo(final Class<? extends Strategy> otherTraversalCategory) {
            if (otherTraversalCategory.equals(DecorationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(OptimizationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(ProviderStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(FinalizationStrategy.class))
                return 0;
            else if (otherTraversalCategory.equals(VerificationStrategy.class))
                return -1;
            else
                return 0;
        }
    }

    /**
     * Implemented by strategies where there is no more behavioral tweaking of the traversal required.  Strategies that
     * implement this category will simply analyze the traversal and throw exceptions if the traversal is not correct
     * for the execution context.
     */
    public interface VerificationStrategy extends Strategy<VerificationStrategy> {

        @Override
        public default Class<VerificationStrategy> getStrategyCategory() {
            return VerificationStrategy.class;
        }

        @Override
        public default int compareTo(final Class<? extends Strategy> otherTraversalCategory) {
            if (otherTraversalCategory.equals(DecorationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(OptimizationStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(ProviderStrategy.class))
                return 1;
            else if (otherTraversalCategory.equals(FinalizationStrategy.class))
                return 1;
            else
                return 0;
        }
    }

}
