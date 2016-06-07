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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackValueStep<S, A, B> extends SideEffectStep<S> implements TraversalParent, ByModulating {

    private Traversal.Admin<S, B> sackTraversal = null;

    private BiFunction<A, B, A> sackFunction;

    public SackValueStep(final Traversal.Admin traversal, final BiFunction<A, B, A> sackFunction) {
        super(traversal);
        this.sackFunction = sackFunction;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> sackTraversal) {
        this.sackTraversal = this.integrateChild(sackTraversal);
    }

    @Override
    public List<Traversal.Admin<S, B>> getLocalChildren() {
        return null == this.sackTraversal ? Collections.emptyList() : Collections.singletonList(this.sackTraversal);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        traverser.sack(this.sackFunction.apply(traverser.sack(), null == this.sackTraversal ? (B) traverser.get() : TraversalUtil.apply(traverser, this.sackTraversal)));
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sackFunction, this.sackTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.sackFunction.hashCode() ^ ((null == this.sackTraversal) ? "null".hashCode() : this.sackTraversal.hashCode());
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return getSelfAndChildRequirements(TraverserRequirement.SACK);
    }

    @Override
    public SackValueStep<S, A, B> clone() {
        final SackValueStep<S, A, B> clone = (SackValueStep<S, A, B>) super.clone();
        if (null != this.sackTraversal)
            clone.sackTraversal = this.sackTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.sackTraversal);
    }
}
