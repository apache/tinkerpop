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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class CoalesceStep<S, E> extends FlatMapStep<S, E> implements TraversalParent {

    private List<Traversal.Admin<S, E>> coalesceTraversals;

    @SafeVarargs
    public CoalesceStep(final Traversal.Admin traversal, final Traversal.Admin<S, E>... coalesceTraversals) {
        super(traversal);
        this.coalesceTraversals = Arrays.asList(coalesceTraversals);
        for (final Traversal.Admin<S, ?> conjunctionTraversal : this.coalesceTraversals) {
            this.integrateChild(conjunctionTraversal);
        }
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        final Traverser.Admin<S> innerTraverser = traverser.clone().asAdmin();
        innerTraverser.setBulk(1L);
        for (final Traversal.Admin<S, E> coalesceTraversal : this.coalesceTraversals) {
            coalesceTraversal.reset();
            coalesceTraversal.addStart(innerTraverser.split());
            if (coalesceTraversal.hasNext())
                return coalesceTraversal;
        }
        return EmptyIterator.instance();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.unmodifiableList(this.coalesceTraversals);
    }

    @Override
    public CoalesceStep<S, E> clone() {
        final CoalesceStep<S, E> clone = (CoalesceStep<S, E>) super.clone();
        clone.coalesceTraversals = new ArrayList<>();
        for (final Traversal.Admin<S, E> conjunctionTraversal : this.coalesceTraversals) {
            clone.coalesceTraversals.add(conjunctionTraversal.clone());
        }
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        for (final Traversal.Admin<S, E> conjunctionTraversal : this.coalesceTraversals) {
            this.integrateChild(conjunctionTraversal);
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.coalesceTraversals);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode(), i = 0;
        for (final Traversal.Admin<S, E> traversal : this.coalesceTraversals) {
            result ^= Integer.rotateLeft(traversal.hashCode(), i++);
        }
        return result;
    }
}
