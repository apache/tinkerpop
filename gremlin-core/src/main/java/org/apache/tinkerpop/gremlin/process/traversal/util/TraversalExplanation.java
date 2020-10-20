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
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A TraversalExplanation takes a {@link Traversal} and, for each registered {@link TraversalStrategy}, it creates a
 * mapping reflecting how each strategy alters the traversal. This is useful for understanding how each traversal
 * strategy mutates the traversal. This is useful in debugging and analysis of traversal compilation. The
 * {@link TraversalExplanation#toString()} has a pretty-print representation that is useful in the Gremlin Console.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalExplanation extends AbstractExplanation implements Serializable {

    protected Traversal.Admin<?, ?> traversal;
    protected List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> strategyTraversals = new ArrayList<>();

    protected TraversalExplanation() {
        // no arg constructor for serialization
    }

    public TraversalExplanation(final Traversal.Admin<?, ?> traversal) {
        this.traversal = traversal.clone();
        final TraversalStrategies mutatingStrategies = new DefaultTraversalStrategies();
        for (final TraversalStrategy strategy : this.traversal.getStrategies()) {
            final Traversal.Admin<?, ?> mutatingTraversal = this.traversal.clone();
            mutatingStrategies.addStrategies(strategy);
            mutatingTraversal.setStrategies(mutatingStrategies);
            mutatingTraversal.applyStrategies();
            this.strategyTraversals.add(Pair.with(strategy, mutatingTraversal));
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

    public ImmutableExplanation asImmutable() {
        return new ImmutableExplanation(getOriginalTraversalAsString(),
                this.getIntermediates().collect(Collectors.toList()));
    }

    @Override
    protected Stream<String> getStrategyTraversalsAsString() {
        return this.strategyTraversals.stream()
                .map(Pair::getValue0)
                .map(Object::toString);
    }

    @Override
    protected String getOriginalTraversalAsString() {
        return getOriginalTraversal().toString();
    }

    @Override
    protected Stream<Triplet<String, String, String>> getIntermediates() {
        return this.strategyTraversals.stream().map( p -> Triplet.with(p.getValue0().toString(),
                p.getValue0().getTraversalCategory().getSimpleName(), p.getValue1().toString()));
    }
}
