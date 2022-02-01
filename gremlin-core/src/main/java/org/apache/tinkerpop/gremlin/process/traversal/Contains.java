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

import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collection;
import java.util.Objects;
import java.util.function.BiPredicate;

/**
 * {@link Contains} is a {@link BiPredicate} that evaluates whether the first object is contained within (or not
 * within) the second collection object. If the first object is a number, each element in the second collection
 * will be compared to the first object using {@link Compare}'s {@code eq} predicate. This will ensure, that numbers
 * are matched by their value only, ignoring the number type. For example:
 * <p/>
 * <pre>
 * gremlin Contains.within [gremlin, blueprints, furnace] == true
 * gremlin Contains.without [gremlin, rexster] == false
 * rexster Contains.without [gremlin, blueprints, furnace] == true
 * 123 Contains.within [1, 2, 3] == false
 * 100 Contains.within [1L, 10L, 100L] == true
 * </pre>
 *
 * @author Pierre De Wilde
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Contains implements BiPredicate<Object, Collection> {

    /**
     * The first object is within the {@code Collection} provided in the second object. The second object may not be
     * {@code null}.
     *
     * @since 3.0.0-incubating
     */
    within {
        @Override
        public boolean test(final Object first, final Collection second) {
            GremlinTypeErrorException typeError = null;
            for (final Object o : second) {
                try {
                    if (Compare.eq.test(first, o))
                        return true;
                } catch (GremlinTypeErrorException ex) {
                    // hold onto it until the end in case any other arguments evaluate to TRUE
                    typeError = ex;
                }
            }
            if (typeError != null)
                throw typeError;
            return false;
        }
    },

    /**
     * The first object is not within the {@code Collection} provided in the second object. The second object may not be
     * {@code null}.
     *
     * @since 3.0.0-incubating
     */
    without {
        @Override
        public boolean test(final Object first, final Collection second) {
            return !within.test(first, second);
        }
    };

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract boolean test(final Object first, final Collection second);

    /**
     * Produce the opposite representation of the current {@code Contains} enum.
     */
    @Override
    public Contains negate() {
        return this.equals(within) ? without : within;
    }
}
