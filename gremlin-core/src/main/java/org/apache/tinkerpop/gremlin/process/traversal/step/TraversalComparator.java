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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.Traversal;
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
        return this.comparator.compare(TraversalUtil.apply(start1, this.traversal), TraversalUtil.apply(start2, this.traversal));
    }

    @Override
    public TraversalComparator clone() throws CloneNotSupportedException {
        final TraversalComparator clone = (TraversalComparator) super.clone();
        clone.traversal = this.traversal.clone();
        return clone;
    }
}

