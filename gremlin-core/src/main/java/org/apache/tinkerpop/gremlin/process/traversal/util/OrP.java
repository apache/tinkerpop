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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.Predicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class OrP<V> extends ConnectiveP<V> {

    public OrP(final List<P<V>> predicates) {
        super(predicates);
        this.biPredicate = new OrBiPredicate(this);
    }

    @Deprecated
    /**
     * @deprecated As of release 3.2.0-incubating, replaced by {@link OrP(List)}
     */
    public OrP(final P<V>... predicates) {
        this(Arrays.asList(predicates));
    }

    @Override
    public P<V> or(final Predicate<? super V> predicate) {
        if (!(predicate instanceof P))
            throw new IllegalArgumentException("Only P predicates can be or'd together");
        this.predicates.add((P<V>) predicate);   // TODO: clone and add?
        return this;
    }

    @Override
    public P<V> negate() {
        super.negate();
        return new AndP<>(this.predicates);
    }

    @Override
    public String toString() {
        return "or(" + StringFactory.removeEndBrackets(this.predicates) + ")";
    }

    @Override
    public OrP<V> clone() {
        final OrP<V> clone = (OrP<V>) super.clone();
        clone.biPredicate = new OrBiPredicate(clone);
        return clone;
    }

    private class OrBiPredicate implements BiPredicate<V, V>, Serializable {

        private final OrP<V> orP;

        private OrBiPredicate(final OrP<V> orP) {
            this.orP = orP;
        }

        @Override
        public boolean test(final V valueA, final V valueB) {
            for (final P<V> predicate : this.orP.predicates) {
                if (predicate.test(valueA))
                    return true;
            }
            return false;
        }
    }
}