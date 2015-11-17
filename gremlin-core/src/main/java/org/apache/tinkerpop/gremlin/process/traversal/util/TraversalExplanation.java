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

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

/**
 * A TraversalExplanation takes a {@link Traversal} and, for each registered {@link TraversalStrategy}, it creates a mapping reflecting how each strategy alters the traversal.
 * This is useful for understanding how each traversal strategy mutates the traversal.
 * This is useful in debugging and analysis of traversal compilation.
 * The {@link TraversalExplanation#toString()} has a pretty-print representation that is useful in the Gremlin Console.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalExplanation {

    private final Traversal.Admin<?, ?> traversal;
    private final List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> strategyTraversals = new ArrayList<>();

    public TraversalExplanation(final Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal.clone();
        Traversal.Admin<?, ?> mutatingTraversal = this.traversal.clone();
        for (final TraversalStrategy strategy : this.traversal.getStrategies().toList()) {
            strategy.apply(mutatingTraversal);
            this.strategyTraversals.add(Pair.with(strategy, mutatingTraversal));
            mutatingTraversal = mutatingTraversal.clone();
        }
    }

    /**
     * Get the list of {@link TraversalStrategy} applications. For strategy, the resultant mutated {@link Traversal} is provided.
     *
     * @return the list of strategy/traversal pairs
     */
    public List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> getStrategyTraversals() {
        return Collections.unmodifiableList(this.strategyTraversals);
    }

    /**
     * Get the original {@link Traversal} used to create this explanation.
     *
     * @return the original traversal
     */
    public Traversal.Admin<?, ?> getOriginalTraversal() {
        return this.traversal;
    }

    /**
     * A pretty-print representation of the traversal explanation.
     *
     * @return a {@link String} representation of the traversal explanation
     */
    @Override
    public String toString() {
        final String originalTraversal = "Original Traversal";
        final String finalTraversal = "Final Traversal";
        final int maxStrategyColumnLength = this.strategyTraversals.stream().map(Pair::getValue0).map(Object::toString).map(String::length).max(Comparator.<Integer>naturalOrder()).get();
        final int maxTraversalColumnLength = Stream.concat(Stream.of(Pair.with(null, this.traversal)), this.strategyTraversals.stream()).map(Pair::getValue1).map(Object::toString).map(String::length).max(Comparator.<Integer>naturalOrder()).get();

        final StringBuilder builder = new StringBuilder("Traversal Explanation\n");
        for (int i = 0; i < (maxStrategyColumnLength + 7 + maxTraversalColumnLength); i++) {
            builder.append("=");
        }
        builder.append("\n");
        builder.append(originalTraversal);
        for (int i = 0; i < maxStrategyColumnLength - originalTraversal.length() + 7; i++) {
            builder.append(" ");
        }
        builder.append(this.traversal.toString());
        builder.append("\n\n");
        for (final Pair<TraversalStrategy, Traversal.Admin<?, ?>> pairs : this.strategyTraversals) {
            builder.append(pairs.getValue0());
            int spacesToAdd = maxStrategyColumnLength - pairs.getValue0().toString().length() + 1;
            for (int i = 0; i < spacesToAdd; i++) {
                builder.append(" ");
            }
            builder.append("[").append(pairs.getValue0().getTraversalCategory().getSimpleName().substring(0, 1)).append("]");
            for (int i = 0; i < 3; i++) {
                builder.append(" ");
            }
            builder.append(pairs.getValue1().toString()).append("\n");
        }
        builder.append("\n");
        builder.append(finalTraversal);
        for (int i = 0; i < maxStrategyColumnLength - finalTraversal.length() + 7; i++) {
            builder.append(" ");
        }
        builder.append(this.strategyTraversals.get(this.strategyTraversals.size() - 1).getValue1());
        return builder.toString();
    }

}
