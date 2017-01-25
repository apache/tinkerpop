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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Distributing;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Pushing;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.BinaryOperator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class DedupGlobalStep<S> extends FilterStep<S> implements TraversalParent, Scoping, Barrier<Map<Object, Traverser.Admin<S>>>, ByModulating, PathProcessor, Distributing, Pushing, GraphComputing {

    private Traversal.Admin<S, Object> dedupTraversal = null;
    private Set<Object> duplicateSet = new HashSet<>();
    private final Set<String> dedupLabels;
    private Set<String> keepLabels;
    private Map<Object, Traverser.Admin<S>> barrier;
    private Iterator<Map.Entry<Object, Traverser.Admin<S>>> barrierIterator;
    private boolean atWorker = false;
    private boolean pushBased = false;

    public DedupGlobalStep(final Traversal.Admin traversal, final String... dedupLabels) {
        super(traversal);
        this.dedupLabels = dedupLabels.length == 0 ? null : Collections.unmodifiableSet(new HashSet<>(Arrays.asList(dedupLabels)));
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        if (this.pushBased && this.atWorker) return false; // todo: study why this is needed
        traverser.setBulk(1);
        if (null == this.dedupLabels) {
            return this.duplicateSet.add(TraversalUtil.applyNullable(traverser, this.dedupTraversal));
        } else {
            final List<Object> objects = new ArrayList<>(this.dedupLabels.size());
            this.dedupLabels.forEach(label -> objects.add(TraversalUtil.applyNullable((S) this.getScopeValue(Pop.last, label, traverser), this.dedupTraversal)));
            return this.duplicateSet.add(objects);
        }
    }

    @Override
    public ElementRequirement getMaxRequirement() {
        return null == this.dedupLabels ? ElementRequirement.ID : PathProcessor.super.getMaxRequirement();
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        while (null != this.barrier || null != this.barrierIterator) {
            if (null == this.barrierIterator) {
                this.barrierIterator = null == this.barrier ? Collections.emptyIterator() : this.barrier.entrySet().iterator();
                this.barrier = null;
            }
            while (this.barrierIterator.hasNext()) {
                final Map.Entry<Object, Traverser.Admin<S>> entry = this.barrierIterator.next();
                if (this.duplicateSet.add(entry.getKey()))
                    return PathProcessor.processTraverserPathLabels(entry.getValue(), this.keepLabels);
            }
            this.barrierIterator = null;
        }
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    public List<Traversal<S, Object>> getLocalChildren() {
        return null == this.dedupTraversal ? Collections.emptyList() : Collections.singletonList(this.dedupTraversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> dedupTraversal) {
        this.dedupTraversal = this.integrateChild(dedupTraversal);
    }

    @Override
    public DedupGlobalStep<S> clone() {
        final DedupGlobalStep<S> clone = (DedupGlobalStep<S>) super.clone();
        clone.duplicateSet = new HashSet<>();
        if (null != this.dedupTraversal)
            clone.dedupTraversal = this.dedupTraversal.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.integrateChild(this.dedupTraversal);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        if (this.dedupTraversal != null)
            result ^= this.dedupTraversal.hashCode();
        if (this.dedupLabels != null)
            result ^= this.dedupLabels.hashCode();
        return result;
    }

    @Override
    public void reset() {
        super.reset();
        this.duplicateSet.clear();
        this.barrier = null;
        this.barrierIterator = null;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.dedupLabels, this.dedupTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.dedupLabels == null ?
                this.getSelfAndChildRequirements(TraverserRequirement.BULK) :
                this.getSelfAndChildRequirements(TraverserRequirement.LABELED_PATH, TraverserRequirement.BULK);
    }

    @Override
    public Set<String> getScopeKeys() {
        return null == this.dedupLabels ? Collections.emptySet() : this.dedupLabels;
    }

    @Override
    public void processAllStarts() {

    }

    @Override
    public boolean hasNextBarrier() {
        return null != this.barrier || this.starts.hasNext();
    }

    @Override
    public Map<Object, Traverser.Admin<S>> nextBarrier() throws NoSuchElementException {
        final Map<Object, Traverser.Admin<S>> map = null != this.barrier ? this.barrier : new HashMap<>();
        while (this.starts.hasNext()) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final Object object;
            if (null != this.dedupLabels) {
                object = new ArrayList<>(this.dedupLabels.size());
                for (final String label : this.dedupLabels) {
                    ((List) object).add(TraversalUtil.applyNullable((S) this.getScopeValue(Pop.last, label, traverser), this.dedupTraversal));
                }
            } else {
                object = TraversalUtil.applyNullable(traverser, this.dedupTraversal);
            }
            if (this.duplicateSet.add(object) && !map.containsKey(object)) {
                traverser.setBulk(1L);
                // traverser.detach();
                traverser.set(DetachedFactory.detach(traverser.get(), true)); // TODO: detect required detachment accordingly
                map.put(object, traverser);
            }
        }
        this.barrier = null;
        this.barrierIterator = null;
        return map;
    }

    @Override
    public void addBarrier(final Map<Object, Traverser.Admin<S>> barrier) {
        if (null == this.barrier)
            this.barrier = new HashMap<>(barrier);
        else
            this.barrier.putAll(barrier);
    }

    @Override
    public MemoryComputeKey<Map<Object, Traverser.Admin<S>>> getMemoryComputeKey() {
        return MemoryComputeKey.of(this.getId(), (BinaryOperator) Operator.addAll, false, true);
    }

    @Override
    public void setKeepLabels(Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }

    @Override
    public void setAtMaster(final boolean atMaster) {
        this.atWorker = !atMaster;
    }

    @Override
    public void setPushBased(final boolean pushBased) {
        this.pushBased = pushBased;
    }

    @Override
    public void onGraphComputer() {
        this.setPushBased(true);
    }

    @Override
    public void atMaster(final boolean atMaster) {
        this.setAtMaster(atMaster);
    }
}
