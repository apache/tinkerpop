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

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ComparatorTraverser<S> implements Comparator<Traverser<S>>, Serializable {

    private final Comparator<S> comparator;

    public ComparatorTraverser(final Comparator<S> comparator) {
        this.comparator = comparator;
    }

    public Comparator<S> getComparator() {
        return this.comparator;
    }

    @Override
    public int compare(final Traverser<S> traverserA, final Traverser<S> traverserB) {
        return this.comparator.compare(traverserA.get(), traverserB.get());
    }

    public static <S> List<ComparatorTraverser<S>> convertComparator(final List<Comparator<S>> comparators) {
        return comparators.stream().map(comparator -> new ComparatorTraverser<>(comparator)).collect(Collectors.toList());
    }
}
