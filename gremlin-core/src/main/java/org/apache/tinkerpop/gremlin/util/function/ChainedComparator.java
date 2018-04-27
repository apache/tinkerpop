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
package org.apache.tinkerpop.gremlin.util.function;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.javatuples.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ChainedComparator<S, C extends Comparable> implements Comparator<S>, Serializable, Cloneable {

    private List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = new ArrayList<>();
    private final boolean isShuffle;
    private final boolean traversers;

    public ChainedComparator(final boolean traversers, final List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators) {
        this.traversers = traversers;
        if (comparators.isEmpty())
            this.comparators.add(new Pair<>(new IdentityTraversal(), (Comparator) Order.asc));
        else
            this.comparators.addAll(comparators);
        this.isShuffle = (Comparator) (this.comparators.get(this.comparators.size() - 1).getValue1()) == Order.shuffle;
        if (!this.isShuffle)
            this.comparators.removeAll(this.comparators.stream().filter(pair -> (Comparator) pair.getValue1() == Order.shuffle).collect(Collectors.toList()));
    }

    public boolean isShuffle() {
        return this.isShuffle;
    }

    @Override
    public int compare(final S objectA, final S objectB) {
        for (final Pair<Traversal.Admin<S, C>, Comparator<C>> pair : this.comparators) {
            final int comparison = this.traversers ?
                    pair.getValue1().compare(TraversalUtil.apply((Traverser.Admin<S>) objectA, pair.getValue0()), TraversalUtil.apply((Traverser.Admin<S>) objectB, pair.getValue0())) :
                    pair.getValue1().compare(TraversalUtil.apply(objectA, pair.getValue0()), TraversalUtil.apply(objectB, pair.getValue0()));
            if (comparison != 0)
                return comparison;
        }
        return 0;
    }

    @Override
    public ChainedComparator<S, C> clone() {
        try {
            final ChainedComparator<S, C> clone = (ChainedComparator<S, C>) super.clone();
            clone.comparators = new ArrayList<>();
            for (final Pair<Traversal.Admin<S, C>, Comparator<C>> comparator : this.comparators) {
                clone.comparators.add(new Pair<>(comparator.getValue0().clone(), comparator.getValue1()));
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
