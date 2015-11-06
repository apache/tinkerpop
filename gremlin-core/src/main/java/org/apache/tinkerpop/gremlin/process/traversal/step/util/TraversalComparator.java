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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalComparator<S, E> implements Comparator<S>, Serializable, Cloneable {

    private Traversal.Admin<S, E> traversal;
    private final Comparator<E> comparator;

    public TraversalComparator(final Traversal.Admin<S, E> traversal, final Comparator<E> comparator) {
        this.traversal = traversal;
        this.comparator = comparator;
    }

    @Override
    public String toString() {
        return this.comparator.toString() + "(" + this.traversal + ")";
    }

    @Override
    public int compare(final S start1, final S start2) {
        return start1 instanceof Traverser ?
                this.comparator.compare(TraversalUtil.apply((Traverser.Admin<S>) start1, this.traversal), TraversalUtil.apply((Traverser.Admin<S>) start2, this.traversal)) :
                this.comparator.compare(TraversalUtil.apply(start1, this.traversal), TraversalUtil.apply(start2, this.traversal));
    }

    @Override
    public TraversalComparator clone() {
        try {
            final TraversalComparator clone = (TraversalComparator) super.clone();
            clone.traversal = this.traversal.clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public Traversal.Admin<S, E> getTraversal() {
        return this.traversal;
    }

    public Comparator<E> getComparator() {
        return this.comparator;
    }
}

