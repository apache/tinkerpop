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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.ListFunction;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A map step that returns the merger of the traverser and the provided arguments without duplicates. This is commonly
 * known as a union.
 */
public final class MergeStep<S, E> extends ScalarMapStep<S, E> implements TraversalParent, ListFunction {
    private Traversal.Admin<S, E> valueTraversal;
    private GValue<Object> parameterItems;

    public MergeStep(final Traversal.Admin traversal, final Object values) {
        super(traversal);

        if (values instanceof Traversal) {
            valueTraversal = integrateChild(((Traversal<S, E>) values).asAdmin());
        } else if (values instanceof GValue){
            parameterItems = (GValue<Object>) values;
        } else {
            parameterItems = GValue.of(null, values);
        }
    }

    public Traversal.Admin<S, E> getValueTraversal() {
        return valueTraversal;
    }

    public Object getParameterItems() {
        return parameterItems;
    }

    public GValue<Object> getParameterItemsGValue() {
        return parameterItems;
    }

    @Override
    public String getStepName() { return "merge"; }

    @Override
    protected E map(final Traverser.Admin<S> traverser) {
        final S incoming = traverser.get();

        final Map mapA = (incoming instanceof Map) ? (Map) incoming : null;
        if (mapA != null) {
            final Object mapB = (valueTraversal != null) ? TraversalUtil.apply(traverser, valueTraversal) : parameterItems.get();
            if (!(mapB instanceof Map)) {
                throw new IllegalArgumentException(
                        String.format(
                                "%s step expected provided argument to evaluate to a Map, encountered %s",
                                getStepName(),
                                mapB.getClass()));
            }

            final Map mergedMap = new HashMap(mapA);
            mergedMap.putAll((Map) mapB);
            return (E) mergedMap;
        } else {
            final Collection listA = convertTraverserToCollection(traverser);

            if (parameterItems != null && parameterItems.get() instanceof Map) {
                throw new IllegalArgumentException(getStepName() + " step type mismatch: expected argument to be Iterable but got Map");
            }
            final Collection listB =
                    (null != valueTraversal)
                            ? convertTraversalToCollection(traverser, valueTraversal)
                            : convertArgumentToCollection(parameterItems.get());

            final Set elements = new HashSet();

            elements.addAll(listA);
            elements.addAll(listB);

            return (E) elements;
        }
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        return (null == valueTraversal) ? Collections.emptyList() : Collections.singletonList(valueTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() { return this.getSelfAndChildRequirements(); }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        if (valueTraversal != null) { this.integrateChild(this.valueTraversal); }
    }

    @Override
    public MergeStep<S, E> clone() {
        final MergeStep<S, E> clone = (MergeStep<S, E>) super.clone();
        if (null != this.valueTraversal) {
            clone.valueTraversal = this.valueTraversal.clone();
        } else {
            clone.parameterItems = this.parameterItems;
        }
        return clone;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        return Objects.hash(result, valueTraversal, parameterItems);
    }
}
