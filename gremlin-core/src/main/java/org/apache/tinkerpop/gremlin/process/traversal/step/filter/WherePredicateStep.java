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

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class WherePredicateStep<S> extends FilterStep<S> implements Scoping, PathProcessor {

    protected String startKey;
    protected List<String> selectKeys;
    protected P<Object> predicate;
    protected final Set<String> scopeKeys = new HashSet<>();
    protected Set<String> keepLabels;

    public WherePredicateStep(final Traversal.Admin traversal, final Optional<String> startKey, final P<String> predicate) {
        super(traversal);
        this.startKey = startKey.orElse(null);
        if (null != this.startKey)
            this.scopeKeys.add(this.startKey);
        this.predicate = (P) predicate;
        this.selectKeys = new ArrayList<>();
        this.configurePredicates(this.predicate);
    }

    private void configurePredicates(final P<Object> predicate) {
        if (predicate instanceof ConnectiveP)
            ((ConnectiveP<Object>) predicate).getPredicates().forEach(this::configurePredicates);
        else {
            final String selectKey = (String) (predicate.getValue() instanceof Collection ? ((Collection) predicate.getValue()).iterator().next() : predicate.getValue()); // hack for within("x"))
            this.selectKeys.add(selectKey);
            this.scopeKeys.add(selectKey);
        }
    }

    private void setPredicateValues(final P<Object> predicate, final Traverser.Admin<S> traverser, final Iterator<String> selectKeysIterator) {
        if (predicate instanceof ConnectiveP)
            ((ConnectiveP<Object>) predicate).getPredicates().forEach(p -> this.setPredicateValues(p, traverser, selectKeysIterator));
        else
            predicate.setValue(this.getScopeValue(Pop.last, selectKeysIterator.next(), traverser));
    }

    public Optional<P<?>> getPredicate() {
        return Optional.ofNullable(this.predicate);
    }

    public Optional<String> getStartKey() {
        return Optional.ofNullable(this.startKey);
    }

    public void removeStartKey() {
        this.selectKeys.remove(this.startKey);
        this.startKey = null;
    }

    @Override
    protected boolean filter(final Traverser.Admin<S> traverser) {
        this.setPredicateValues(this.predicate, traverser, this.selectKeys.iterator());
        return this.predicate.test(null == this.startKey ? traverser.get() : this.getScopeValue(Pop.last, this.startKey, traverser));
    }

    @Override
    public String toString() {
        // TODO: revert the predicates to their string form?
        return StringFactory.stepString(this, this.startKey, this.predicate);
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.unmodifiableSet(this.scopeKeys);
    }

    @Override
    public WherePredicateStep<S> clone() {
        final WherePredicateStep<S> clone = (WherePredicateStep<S>) super.clone();
        clone.predicate = this.predicate.clone();
        return clone;
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ (null == this.startKey ? "null".hashCode() : this.startKey.hashCode()) ^ this.predicate.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return TraversalHelper.getLabels(TraversalHelper.getRootTraversal(this.traversal)).stream().filter(this.scopeKeys::contains).findAny().isPresent() ?
                TYPICAL_GLOBAL_REQUIREMENTS :
                TYPICAL_LOCAL_REQUIREMENTS;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() {
        return PathProcessor.processTraverserPathLabels(super.processNextStart(), this.keepLabels);
    }

    @Override
    public void setKeepLabels(Set<String> labels) {
        this.keepLabels = labels;
    }

    @Override
    public Set<String> getKeepLabels() {
        return this.keepLabels;
    }
}
