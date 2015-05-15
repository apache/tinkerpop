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

package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalBiPredicate<S, E> implements BiPredicate<S, E> {

    private Traversal.Admin<S, E> traversal;
    private final boolean negate;

    public TraversalBiPredicate(final Traversal.Admin<S, E> traversal, final boolean negate) {
        this.traversal = traversal;
        this.negate = negate;
    }

    @Override
    public boolean test(final S start, final E end) {
        if (null == start)
            throw new IllegalArgumentException("The traversal must be provided a start: " + this.traversal);
        final boolean result = null == end ? TraversalUtil.test(start, this.traversal) : TraversalUtil.test(start, this.traversal, end);
        return this.negate ? !result : result;
    }

    public Traversal.Admin<S, E> getTraversal() {
        return this.traversal;
    }

    @Override
    public String toString() {
        return this.traversal.toString();
    }

    @Override
    public TraversalBiPredicate<S, E> negate() {
        return new TraversalBiPredicate<>(this.traversal.clone(), !this.negate);
    }

    @Override
    public TraversalBiPredicate<S, E> clone() {
        try {
            final TraversalBiPredicate<S, E> clone = (TraversalBiPredicate<S, E>) super.clone();
            clone.traversal = this.traversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e);
        }
    }
}
