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
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.FilteringBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalProduct;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;

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
public final class DedupGlobalStep<S> extends FilterStep<S> implements TraversalParent, Scoping, GraphComputing,
        FilteringBarrier<Map<Object, Traverser.Admin<S>>>, ByModulating, PathProcessor {

    private Traversal.Admin<S, Object> dedupTraversal = null;
    private Set<Object> duplicateSet = new HashSet<>();
    private boolean onGraphComputer = false;
    private final Set<String> dedupLabels;
    private Set<String> keepLabels;
    private boolean executingAtMaster = false;
    private Map<Object, Traverser.Admin<S>> barrier;
    private Iterator<Map.Entry<Object, Traverser.Admin<S>>> barrierIterator;

    public DedupGlobalStep(final Traversal.Admin traversal, final String... dedupLabels) {
        super(traversal);
        this.dedupLabels = dedupLabels.length == 0 ? null : Collections.unmodifiableSet(new HashSet<>(Arrays.asList(dedupLabels)));
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        if (this.onGraphComputer && !this.executingAtMaster) return true;
        traverser.setBulk(1L);
        if (null == this.dedupLabels) {
            final TraversalProduct product = TraversalUtil.produce(traverser, this.dedupTraversal);
            return product.isProductive() && this.duplicateSet.add(product.get());
        } else {
            final List<Object> objects = new ArrayList<>(this.dedupLabels.size());
            for (String label : dedupLabels) {
                final TraversalProduct product = TraversalUtil.produce((S) this.getSafeScopeValue(Pop.last, label, traverser), this.dedupTraversal);
                if (!product.isProductive()) break;
                objects.add(product.get());
            }

            // the object sizes must be equal or else it means a by() wasn't productive and that path will be filtered
            return objects.size() == dedupLabels.size() && duplicateSet.add(objects);
        }
    }

    @Override
    public void atMaster(final boolean atMaster) {
        this.executingAtMaster = atMaster;
    }

    @Override
    public ElementRequirement getMaxRequirement() {
        return null == this.dedupLabels ? ElementRequirement.ID : PathProcessor.super.getMaxRequirement();
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        if (null != this.barrier) {
            this.barrierIterator = this.barrier.entrySet().iterator();
            this.barrier = null;
        }
        while (this.barrierIterator != null && this.barrierIterator.hasNext()) {
            if (null == this.barrierIterator)
                this.barrierIterator = this.barrier.entrySet().iterator();
            final Map.Entry<Object, Traverser.Admin<S>> entry = this.barrierIterator.next();
            if (this.duplicateSet.add(entry.getKey()))
                return PathProcessor.processTraverserPathLabels(entry.getValue(), this.keepLabels);
        }
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    public List<Traversal<S, Object>> getLocalChildren() {
        return null == this.dedupTraversal ? Collections.emptyList() : Collections.singletonList(this.dedupTraversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> dedupTraversal) {
        if (this.dedupTraversal != null) {
            throw new IllegalStateException("Dedup step can only have one by modulator");
        }
        this.dedupTraversal = this.integrateChild(dedupTraversal);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        if (null != this.dedupTraversal && this.dedupTraversal.equals(oldTraversal))
            this.dedupTraversal = this.integrateChild(newTraversal);
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
    public void onGraphComputer() {
        this.onGraphComputer = true;
    }

    @Override
    public Set<String> getScopeKeys() {
        return null == this.dedupLabels ? Collections.emptySet() : this.dedupLabels;
    }

    @Override
    public HashSet<PopInstruction> getPopInstructions() {
        final HashSet<PopInstruction> popInstructions = new HashSet<>();
        popInstructions.addAll(Scoping.super.getPopInstructions());
        popInstructions.addAll(TraversalParent.super.getPopInstructions());
        return popInstructions;
    }

    @Override
    public void processAllStarts() {

    }

    @Override
    public Map<Object, Traverser.Admin<S>> getEmptyBarrier() {
        return new HashMap<>();
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
            boolean productive;
            if (null != this.dedupLabels) {
                object = new ArrayList<>(this.dedupLabels.size());
                for (final String label : this.dedupLabels) {
                    final TraversalProduct product = TraversalUtil.produce((S) this.getSafeScopeValue(Pop.last, label, traverser), this.dedupTraversal);
                    if (!product.isProductive()) break;
                    ((List) object).add(product.get());
                }

                productive = ((List) object).size() == this.dedupLabels.size();
            } else {
                final TraversalProduct product = TraversalUtil.produce(traverser, this.dedupTraversal);
                productive = product.isProductive();
                object = productive ? product.get() : null;
            }

            if (productive) {
                if (!map.containsKey(object)) {
                    traverser.setBulk(1L);

                    // DetachedProperty and DetachedVertexProperty both have a transient for the Host element. that causes
                    // trouble for olap which ends up requiring the Host later. can't change the transient without some
                    // consequences: (1) we break gryo formatting and io tests start failing (2) storing the element with
                    // the property has the potential to bloat detached Element instances as it basically stores that data
                    // twice. Not sure if it's smart to change that at least in 3.4.x and not without some considerable
                    // thought as to what might be major changes. To work around the problem we will detach properties as
                    // references so that the parent element goes with it. Also, given TINKERPOP-2318 property comparisons
                    // have changed in such a way that allows this to work properly
                    if (this.onGraphComputer) {
                        if (traverser.get() instanceof Property)
                            traverser.set(ReferenceFactory.detach(traverser.get()));
                        else
                            traverser.set(DetachedFactory.detach(traverser.get(), true));
                    } else {
                        traverser.set(traverser.get());
                    }
                    map.put(object, traverser);
                }
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
    public void setKeepLabels(final Set<String> keepLabels) {
        this.keepLabels = new HashSet<>(keepLabels);
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }
}
