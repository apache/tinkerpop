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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TraversalFlatMapStep<S, E> extends FlatMapStep<S, E> implements TraversalParent {

    private Traversal.Admin<S, E> flatMapTraversal;

    public TraversalFlatMapStep(final Traversal.Admin traversal, final Traversal<S, E> flatMapTraversal) {
        super(traversal);
        this.flatMapTraversal = this.integrateChild(flatMapTraversal.asAdmin());
    }

    @Override
    protected Iterator<E> flatMap(final Traverser.Admin<S> traverser) {
        return TraversalUtil.applyAll(traverser, this.flatMapTraversal);
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.singletonList(this.flatMapTraversal);
    }

    @Override
    public TraversalFlatMapStep<S, E> clone() {
        final TraversalFlatMapStep<S, E> clone = (TraversalFlatMapStep<S, E>) super.clone();
        clone.flatMapTraversal = this.flatMapTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.flatMapTraversal);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.flatMapTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.flatMapTraversal.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements();
    }
}
