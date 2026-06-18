/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ReadOnlyTraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public final class NoneStep<S, S2> extends FilterStep<S> implements ReadOnlyTraversalParent {

    private P<S2> predicate;

    public NoneStep(final Traversal.Admin traversal, final P<S2> predicate) {
        super(traversal);

        if (null == predicate) {
            throw new IllegalArgumentException("Input predicate to none step can't be null.");
        }

        this.predicate = predicate;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        if (this.predicate.hasTraversal()) {
            this.predicate.resolve(traverser);
        }

        final S item = traverser.get();

        if (item instanceof Iterable || item instanceof Iterator || ((item != null) && item.getClass().isArray())) {
            final Iterator<S2> iterator = IteratorUtils.asIterator(item);
            while (iterator.hasNext()) {
                if (this.predicate.test(iterator.next())) {
                    return false;
                }
            }
            return true;
        }

        return false;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.predicate);
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>();
        P.collectTraversals(this.predicate, traversals);
        return (List) Collections.unmodifiableList(traversals);
    }

    @Override
    public NoneStep<S, S2> clone() {
        final NoneStep<S, S2> clone = (NoneStep<S, S2>) super.clone();
        clone.predicate = this.predicate.clone();
        return clone;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        final List<Traversal.Admin<?, ?>> traversals = new ArrayList<>();
        P.collectTraversals(this.predicate, traversals);
        traversals.forEach(this::integrateChild);
    }
}
