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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SelectOneStep<S, E> extends MapStep<S, E> implements TraversalParent, Scoping, PathProcessor, ByModulating {

    private final Pop pop;
    private final String selectKey;
    private Traversal.Admin<S, E> selectTraversal = null;
    private Set<String> keepLabels;

    public SelectOneStep(final Traversal.Admin traversal, final Pop pop, final String selectKey) {
        super(traversal);
        this.pop = pop;
        this.selectKey = selectKey;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        final Traverser.Admin<S> traverser = this.starts.next();

        try {
            final S o = getScopeValue(pop, selectKey, traverser);
            if (null == o) return traverser.split(null, this);

            final TraversalProduct product = TraversalUtil.produce(o, this.selectTraversal);
            if (!product.isProductive()) return EmptyTraverser.instance();

            final Traverser.Admin<E> outTraverser = traverser.split((E) product.get(), this);
            if (!(this.getTraversal().getParent() instanceof MatchStep))
                PathProcessor.processTraverserPathLabels(outTraverser, this.keepLabels);
            return outTraverser;
        } catch (KeyNotFoundException nfe) {
            return EmptyTraverser.instance();
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.pop, this.selectKey, this.selectTraversal);
    }

    @Override
    public SelectOneStep<S, E> clone() {
        final SelectOneStep<S, E> clone = (SelectOneStep<S, E>) super.clone();
        if (null != this.selectTraversal)
            clone.selectTraversal = this.selectTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.selectTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.selectKey.hashCode();
        if (null != this.selectTraversal)
            result ^= this.selectTraversal.hashCode();
        if (null != this.pop)
            result ^= this.pop.hashCode();
        return result;
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return null == this.selectTraversal ? Collections.emptyList() : Collections.singletonList(this.selectTraversal);
    }

    @Override
    public void removeLocalChild(final Traversal.Admin<?, ?> traversal) {
        if (this.selectTraversal == traversal)
            this.selectTraversal = null;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.selectTraversal = this.integrateChild(selectTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.selectTraversal && this.selectTraversal.equals(oldTraversal))
            this.selectTraversal = this.integrateChild(newTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.singleton(this.selectKey);
    }

    @Override
    public Set<ScopingInfo> getScopingInfo() {
        final Set<String> labels = this.getScopeKeys();
        final Set<ScopingInfo> scopingInfoSet = new HashSet<>();
        for (String label : labels) {
            final ScopingInfo scopingInfo = new ScopingInfo();
            scopingInfo.label = label;
            scopingInfo.pop = this.getPop();
            scopingInfoSet.add(scopingInfo);
        }
        return scopingInfoSet;
    }

    public Pop getPop() {
        return this.pop;
    }

    @Override
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

}


