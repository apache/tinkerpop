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
import org.javatuples.Triplet;

import java.util.List;
import java.util.stream.Stream;

/**
 * A data-only representation of a {@link TraversalExplanation} which doesn't re-calculate the "explanation" from
 * the raw traversal data each time the explanation is displayed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ImmutableExplanation extends TraversalExplanation {

    private final String originalTraversal;
    private final List<Triplet<String, String, String>> intermediates;

    public ImmutableExplanation(final String originalTraversal,
                                final List<Triplet<String, String, String>> intermediates) {
        this.originalTraversal = originalTraversal;
        this.intermediates = intermediates;
    }

    @Override
    public List<Pair<TraversalStrategy, Traversal.Admin<?, ?>>> getStrategyTraversals() {
        throw new UnsupportedOperationException("This instance is immutable");
    }

    @Override
    public Traversal.Admin<?, ?> getOriginalTraversal() {
        throw new UnsupportedOperationException("This instance is immutable");
    }

    @Override
    public ImmutableExplanation asImmutable() {
        return this;
    }

    @Override
    protected Stream<String> getStrategyTraversalsAsString() {
        return getIntermediates().map(Triplet::getValue0);
    }

    @Override
    protected Stream<String> getTraversalStepsAsString() {
        return Stream.concat(Stream.of(this.originalTraversal), getIntermediates().map(Triplet::getValue2));
    }

    @Override
    protected String getOriginalTraversalAsString() {
        return this.originalTraversal;
    }

    @Override
    protected Stream<Triplet<String, String, String>> getIntermediates() {
        return intermediates.stream();
    }
}
