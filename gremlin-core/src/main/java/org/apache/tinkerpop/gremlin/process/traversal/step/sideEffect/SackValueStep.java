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
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SackValueStep<S, A, B> extends AbstractStep<S, S> implements TraversalParent, ByModulating, LambdaHolder {

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
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.sackTraversal && this.sackTraversal.equals(oldTraversal))
            this.sackTraversal = this.integrateChild(newTraversal);
    }

    @Override
    public List<Traversal.Admin<S, B>> getLocalChildren() {
        return null == this.sackTraversal ? Collections.emptyList() : Collections.singletonList(this.sackTraversal);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        final Traverser.Admin<S> traverser = this.starts.next();
        if (null == sackTraversal) {
            traverser.sack(this.sackFunction.apply(traverser.sack(), (B) traverser.get()));
            return traverser;
        } else {
            final TraversalProduct product = TraversalUtil.produce(traverser, this.sackTraversal);
            if (!product.isProductive()) return EmptyTraverser.instance();
            traverser.sack(this.sackFunction.apply(traverser.sack(), (B) product.get()));
            return traverser;
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.sackFunction, this.sackTraversal);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.sackFunction.hashCode() ^ ((null == this.sackTraversal) ? "null".hashCode() : this.sackTraversal.hashCode());
    }

    public BiFunction<A, B, A> getSackFunction() {
        return this.sackFunction;
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
