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
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalRing;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SelectStep<S, E> extends MapStep<S, Map<String, E>> implements Scoping, TraversalParent, PathProcessor, ByModulating {

    private TraversalRing<Object, E> traversalRing = new TraversalRing<>();
    private final Pop pop;
    private final List<String> selectKeys;
    private final Set<String> selectKeysSet;
    private Set<String> keepLabels;

    public SelectStep(final Traversal.Admin traversal, final Pop pop, final String... selectKeys) {
        super(traversal);
        this.pop = pop;
        this.selectKeys = Arrays.asList(selectKeys);
        this.selectKeysSet = Collections.unmodifiableSet(new HashSet<>(this.selectKeys));
        if (this.selectKeys.size() < 2)
            throw new IllegalArgumentException("At least two select keys must be provided: " + this);
    }

    @Override
    protected Traverser.Admin<Map<String, E>> processNextStart() throws NoSuchElementException {
        final Traverser.Admin<S> traverser = this.starts.next();
        final Map<String, E> bindings = new LinkedHashMap<>(this.selectKeys.size(), 1.0f);
        try {
            for (final String selectKey : this.selectKeys) {
                final E end = this.getScopeValue(this.pop, selectKey, traverser);
                final TraversalProduct product = TraversalUtil.produce(end, this.traversalRing.next());

                if (!product.isProductive()) break;

                bindings.put(selectKey, (E) product.get());
            }

            // bindings should be the same size as unique keys or else there was an unproductive by() in which case
            // we filter with an EmptyTraverser.
            if (bindings.size() != selectKeysSet.size()) return EmptyTraverser.instance();

        } catch (KeyNotFoundException nfe) {
            return EmptyTraverser.instance();
        } finally {
            this.traversalRing.reset();
        }

        return PathProcessor.processTraverserPathLabels(traverser.split(bindings, this), this.keepLabels);
    }

    @Override
    public void reset() {
        super.reset();
        this.traversalRing.reset();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.pop, this.selectKeys, this.traversalRing);
    }

    @Override
    public SelectStep<S, E> clone() {
        final SelectStep<S, E> clone = (SelectStep<S, E>) super.clone();
        clone.traversalRing = this.traversalRing.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.traversalRing.getTraversals().forEach(this::integrateChild);
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.traversalRing.hashCode() ^ this.selectKeys.hashCode();
        if (null != this.pop)
            result ^= this.pop.hashCode();
        return result;
    }

    @Override
    public List<Traversal.Admin<Object, E>> getLocalChildren() {
        return this.traversalRing.getTraversals();
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> selectTraversal) {
        this.traversalRing.addTraversal(this.integrateChild(selectTraversal));
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        this.traversalRing.replaceTraversal(
                (Traversal.Admin<Object, E>) oldTraversal,
                this.integrateChild(newTraversal));
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT, TraverserRequirement.SIDE_EFFECTS);
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.selectKeysSet;
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

    /**
     * Get the keys for this SelectStep. Unlike {@link SelectStep#getScopeKeys()}, this returns a list possibly with
     * a duplicate key. This guarantees to return the keys in the same order as passed in.
     * TODO: getScopeKeys should return order-aware data structure instead of HashSet so that graph providers can
     *       get the keys in the order passed in a query, and can associate them with by-traversals in a correct sequence.
     *
     */
    public List<String> getSelectKeys() {
        return this.selectKeys;
    }

    public Map<String, Traversal.Admin<Object, E>> getByTraversals() {
        final Map<String, Traversal.Admin<Object, E>> map = new LinkedHashMap<>();
        this.traversalRing.reset();
        for (final String as : this.selectKeys) {
            map.put(as, this.traversalRing.next());
        }
        return map;
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
