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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class TraversalSelectStep<S, E> extends MapStep<S, E> implements TraversalParent, PathProcessor, ByModulating {

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
    protected E map(final Traverser.Admin<S> traverser) {
        E end = null;
        final Iterator<E> keyIterator = TraversalUtil.applyAll(traverser, this.keyTraversal);
        if (keyIterator.hasNext()) {
            final E key = keyIterator.next();
            final Object object = traverser.get();
            if (object instanceof Map && ((Map) object).containsKey(key))
                end = (E) ((Map) object).get(key);
            else if (key instanceof String) {
                final String skey = (String) key;
                if (traverser.getSideEffects().exists(skey)) {
                    end = traverser.getSideEffects().get((String) key);
                } else {
                    final Path path = traverser.path();
                    if (path.hasLabel(skey))
                        end = null == pop ? path.get(skey) : path.get(pop, skey);
                }
            }
        }
        return null != end ? TraversalUtil.applyNullable(end, this.selectTraversal) : null;
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
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(
                TraverserRequirement.OBJECT,
                TraverserRequirement.SIDE_EFFECTS,
                TraverserRequirement.PATH);
    }

    //@Override
    //public Set<String> getScopeKeys() {
    //    return Collections.singleton(this.selectKey);
    //}

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

    @Override
    protected Traverser.Admin<E> processNextStart() {
        final Traverser.Admin<E> traverser = super.processNextStart();
        if (!(this.getTraversal().getParent() instanceof MatchStep)) {
            PathProcessor.processTraverserPathLabels(traverser, this.keepLabels);
        }
        return traverser;
    }
}


