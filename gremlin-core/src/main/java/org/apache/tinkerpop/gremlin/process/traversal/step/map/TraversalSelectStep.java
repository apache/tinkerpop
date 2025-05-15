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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class TraversalSelectStep<S, E> extends MapStep<S, E> implements TraversalParent, PathProcessor, ByModulating, Scoping {

    private final Pop pop;
    private Traversal.Admin<S, E> keyTraversal;
    private Traversal.Admin<E, E> selectTraversal = null;
    private Set<String> keepLabels;

    public TraversalSelectStep(final Traversal.Admin traversal, final Pop pop, final Traversal<S, E> keyTraversal) {
        super(traversal);
        this.pop = pop;
        this.keyTraversal = this.integrateChild(keyTraversal.asAdmin());
    }

    @Override
    protected Traverser.Admin<E> processNextStart() {
        final Traverser.Admin<S> traverser = this.starts.next();
        final Iterator<E> keyIterator = TraversalUtil.applyAll(traverser, this.keyTraversal);
        if (keyIterator.hasNext()) {
            final E key = keyIterator.next();
            try {
                final E end = getScopeValue(pop, key, traverser);
                final Traverser.Admin<E> outTraverser = traverser.split(null == end ? null : TraversalUtil.applyNullable(end, this.selectTraversal), this);
                if (!(this.getTraversal().getParent() instanceof MatchStep)) {
                    PathProcessor.processTraverserPathLabels(outTraverser, this.keepLabels);
                }
                return outTraverser;
            } catch (KeyNotFoundException nfe) {
                return EmptyTraverser.instance();
            }
        } else {
            return EmptyTraverser.instance();
        }
    }

    @Override
    public Set<String> getScopeKeys() {
        // can't return scope keys here because they aren't known prior to traversal execution and this method is
        // used at strategy application time. not getting any test failures as a result of returning empty. assuming
        // that strategies don't use Scoping in a way that requires the keys to be known and if they aren't doesn't
        // hose the whole traversal. in the worst case, strategies will hopefully just leave steps alone rather than
        // make their own assumptions that the step is not selecting anything. if that is happening somehow we might
        // need to modify Scoping to better suite this runtime evaluation of the key.
        return Collections.emptySet();
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

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.pop, this.keyTraversal, this.selectTraversal);
    }

    @Override
    public TraversalSelectStep<S, E> clone() {
        final TraversalSelectStep<S, E> clone = (TraversalSelectStep<S, E>) super.clone();
        clone.keyTraversal = this.keyTraversal.clone();
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
        int result = super.hashCode() ^ this.keyTraversal.hashCode();
        if (null != this.selectTraversal)
            result ^= this.selectTraversal.hashCode();
        if (null != this.pop)
            result ^= this.pop.hashCode();
        return result;
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        if (null == this.selectTraversal && null == this.keyTraversal)
            return Collections.emptyList();

        final List<Traversal.Admin<?, ?>> children = new ArrayList<>();
        if (selectTraversal != null)
            children.add(selectTraversal);
        if (keyTraversal != null)
            children.add(keyTraversal);

        return children;
    }

    @Override
    public void removeLocalChild(final Traversal.Admin<?, ?> traversal) {
        if (this.selectTraversal == traversal)
            this.selectTraversal = null;
        if (this.keyTraversal == traversal)
            this.keyTraversal = null;
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.selectTraversal = this.integrateChild(selectTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(
                TraverserRequirement.OBJECT,
                TraverserRequirement.SIDE_EFFECTS,
                TraverserRequirement.PATH);
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


