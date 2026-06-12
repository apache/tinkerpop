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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.List;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AndP<V> extends ConnectiveP<V> {

    public AndP(final List<P<V>> predicates) {
        super(predicates);
        for (final P<V> p : predicates) {
            this.and(p);
        }
        this.biPredicate = new AndBiPredicate(this);
    }

    @Override
    public P<V> and(final Predicate<? super V> predicate) {
        if (!(predicate instanceof P))
            throw new IllegalArgumentException("Only P predicates can be and'd together");
        else if (predicate instanceof AndP)
            this.predicates.addAll(((AndP) predicate).getPredicates());
        else
            this.predicates.add((P<V>) predicate);
        return this;
    }

    @Override
    public P<V> negate() {
        super.negate();
        return new OrP<>(this.predicates);
    }

    /**
     * Resolves child predicates with short-circuiting. Because a conjunction fails as soon as any child
     * predicate cannot be satisfied, resolution stops at the first child that resolves empty (i.e. a scalar
     * predicate whose child traversal produced no comparison value). This avoids evaluating the remaining
     * child traversals, which may be expensive. Collection predicates (within/without) never resolve empty
     * (they resolve to an empty collection), so they do not trigger the short-circuit.
     */
    @Override
    public void resolve(final Traverser.Admin<?> traverser) {
        // No super.resolve(): a connective predicate carries no child traversal of its own; only its
        // operands do. Resolving each operand directly is sufficient (and enables the short-circuit below).
        for (final P<V> p : this.predicates) {
            if (p.hasTraversal()) {
                p.resolve(traverser);
                if (p.isResolvedEmpty()) return;
            }
        }
    }

    @Override
    public boolean isResolvedEmpty() {
        // AND short-circuits: if any child resolved empty, the conjunction cannot be satisfied
        for (final P<V> p : this.predicates) {
            if (p.hasTraversal() && p.isResolvedEmpty()) return true;
        }
        return false;
    }

    @Override
    public String toString() {
        return "and(" + StringFactory.removeEndBrackets(this.predicates) + ")";
    }

    @Override
    public AndP<V> clone() {
        final AndP<V> clone = (AndP<V>) super.clone();
        clone.biPredicate = new AndBiPredicate(clone);
        return clone;
    }

    private class AndBiPredicate implements PBiPredicate<V, V>, Serializable {

        private final AndP<V> andP;

        private AndBiPredicate(final AndP<V> andP) {
            this.andP = andP;
        }

        @Override
        public boolean test(final V valueA, final V valueB) {
            for (final P<V> predicate : this.andP.predicates) {
                if (!predicate.test(valueA))
                    return false;
            }
            return true;
        }

        @Override
        public String getPredicateName() {
            return "and";
        }
    }
}
