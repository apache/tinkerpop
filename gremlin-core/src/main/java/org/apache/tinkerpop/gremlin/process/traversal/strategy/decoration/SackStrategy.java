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

package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

/**
 * The {@code SackStrategy} is used internal to the {@code withSack()} steps of {@link TraversalSource} and is not
 * typically constructed directly.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackStrategy extends AbstractTraversalStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    private final Supplier initialValue;
    private final UnaryOperator splitOperator;
    private final BinaryOperator mergeOperator;

    private SackStrategy(final Supplier initialValue, final UnaryOperator splitOperator, final BinaryOperator mergeOperator) {
        if (null == initialValue)
            throw new IllegalArgumentException("The initial value of a sack can not be null");
        this.initialValue = initialValue;
        this.splitOperator = splitOperator;
        this.mergeOperator = mergeOperator;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.isRoot())
            traversal.getSideEffects().setSack(this.initialValue, this.splitOperator, this.mergeOperator);
    }

    public static <A> Builder<A> build() {
        return new Builder<>();
    }

    public final static class Builder<A> {

        private Supplier<A> initialValue;
        private UnaryOperator<A> splitOperator = null;
        private BinaryOperator<A> mergeOperator = null;

        private Builder() {
        }

        public Builder initialValue(final Supplier<A> initialValue) {
            this.initialValue = initialValue;
            return this;
        }

        public Builder splitOperator(final UnaryOperator<A> splitOperator) {
            this.splitOperator = splitOperator;
            return this;
        }

        public Builder mergeOperator(final BinaryOperator<A> mergeOperator) {
            this.mergeOperator = mergeOperator;
            return this;
        }

        public SackStrategy create() {
            return new SackStrategy(this.initialValue, this.splitOperator, this.mergeOperator);
        }
    }
}