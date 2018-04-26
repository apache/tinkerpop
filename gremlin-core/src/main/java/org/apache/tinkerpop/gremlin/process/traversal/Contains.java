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

import java.util.Collection;
import java.util.function.BiPredicate;

/**
 * {@link Contains} is a {@link BiPredicate} that evaluates whether the first object is contained within (or not
 * within) the second collection object. For example:
 * <p/>
 * <pre>
 * gremlin Contains.within [gremlin, blueprints, furnace] == true
 * gremlin Contains.without [gremlin, rexster] == false
 * rexster Contains.without [gremlin, blueprints, furnace] == true
 * </pre>
 *
 * @author Pierre De Wilde
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Contains implements BiPredicate<Object, Collection> {

    /**
     * The first object is within the {@link Collection} provided in the second object.
     *
     * @since 3.0.0-incubating
     */
    within {
        @Override
        public boolean test(final Object first, final Collection second) {
            return second.contains(first);
        }
    },

    /**
     * The first object is not within the {@link Collection} provided in the second object.
     *
     * @since 3.0.0-incubating
     */
    without {
        @Override
        public boolean test(final Object first, final Collection second) {
            return !second.contains(first);
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
