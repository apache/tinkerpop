/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalP<S, E> extends P<E> {

    private Traversal.Admin<S, E> traversal;
    private final boolean negate;

    public TraversalP(final Traversal.Admin<S, E> traversal, final E end, final boolean negate) {
        super(null, end);
        this.traversal = traversal;
        this.negate = negate;
        this.biPredicate = (BiPredicate) new TraversalBiPredicate(this);
    }

    public TraversalP(final Traversal.Admin<S, E> traversal, final boolean negate) {
        this(traversal, null, negate);
    }

    public List<Traversal.Admin<S, E>> getTraversals() {
        return Collections.singletonList(this.traversal);
    }

    @Override
    public int hashCode() {
        return this.traversal.hashCode() ^ Boolean.hashCode(this.negate);
    }

    @Override
    public boolean equals(final Object other) {
        if (other != null && other.getClass().equals(this.getClass())) {
            final TraversalP otherTraversalP = (TraversalP) other;
            return this.negate == otherTraversalP.negate &&
                    this.traversal.equals(otherTraversalP.traversal);
        }
        return false;
    }

    @Override
    public TraversalP<S, E> negate() {
        return new TraversalP<>(this.traversal.clone(), this.value, !this.negate);
    }

    @Override
    public TraversalP<S, E> clone() {
        final TraversalP<S, E> clone = (TraversalP<S, E>) super.clone();
        clone.traversal = this.traversal.clone();
        clone.biPredicate = (BiPredicate) new TraversalBiPredicate<>(clone);
        return clone;
    }

    public static P<?> orTraversals(final Traversal<?, ?>... orTraversals) {
        P<?> p = traversal(orTraversals[0]);
        for (int i = 1; i < orTraversals.length; i++) {
            p = p.or(orTraversals[i]);
        }
        return p;
    }

    public static P<?> andTraversals(final Traversal<?, ?>... andTraversals) {
        P<?> p = traversal(andTraversals[0]);
        for (int i = 1; i < andTraversals.length; i++) {
            p = p.and(andTraversals[i]);
        }
        return p;
    }

    private static class TraversalBiPredicate<S, E> implements BiPredicate<S, E>, Serializable {

        private final TraversalP<S, E> traversalP;

        public TraversalBiPredicate(final TraversalP<S, E> traversalP) {
            this.traversalP = traversalP;
        }

        @Override
        public boolean test(final S start, final E end) {
            if (null == start)
                throw new IllegalArgumentException("The traversal must be provided a start: " + traversalP.traversal);
            final boolean result;
            if (start instanceof Traverser)
                result = TraversalUtil.test(((Traverser<S>) start).asAdmin(), traversalP.traversal, end);
            else
                result = TraversalUtil.test(start, traversalP.traversal, end);
            return traversalP.negate ? !result : result;
        }

        @Override
        public String toString() {
            return this.traversalP.negate ? "!" + this.traversalP.traversal.toString() : this.traversalP.traversal.toString();
        }
    }
}
