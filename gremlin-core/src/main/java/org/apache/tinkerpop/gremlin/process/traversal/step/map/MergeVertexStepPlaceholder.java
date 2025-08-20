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

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

/**
 * Implementation for the {@code mergeV()} step covering both the start step version and the one used mid-traversal.
 */
public class MergeVertexStepPlaceholder<S> extends AbstractStep<S, Vertex> implements MergeStepContract<S, Vertex, Map>, GValueHolder<S, Vertex> {

    private static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));
    private Map<Object, List<Object>> properties = new HashMap<>();

    public static void validateMapInput(final Map map, final boolean ignoreTokens) {
        MergeStep.validate(map, ignoreTokens, allowedTokens, "mergeV");
    }

    private Traversal.Admin<?,Map<Object,Object>> mergeTraversal;
    private Traversal.Admin<?,Map<Object,Object>> onCreateTraversal;
    private Traversal.Admin<?,Map<Object,Object>> onMatchTraversal;
    private final boolean isStart;

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart) {
        this(traversal, isStart, new IdentityTraversal<>());
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final Map<Object, Object> merge) {
        this(traversal, isStart, new ConstantTraversal<>(merge));
        validateMapInput(merge, false);
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final GValue<Map<Object, Object>> merge) {
        this(traversal, isStart, new GValueConstantTraversal<>(merge));
        validateMapInput(merge.get(), false);
    }

    public MergeVertexStepPlaceholder(final Traversal.Admin traversal, final boolean isStart, final Traversal.Admin<?,Map<Object, Object>> mergeTraversal) {
        super(traversal);
        this.isStart = isStart;
        this.mergeTraversal = mergeTraversal;
        if (mergeTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).getGValue());
        }
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() throws NoSuchElementException {
        return null;
    }

    @Override
    public MergeVertexStepPlaceholder<S> clone() {
        return (MergeVertexStepPlaceholder<S>) super.clone();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return super.getRequirements();
    }

    @Override
    public Traversal.Admin getMergeTraversal() {
        if (mergeTraversal != null && mergeTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).getGValue().getName());
            return ((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).getConstantTraversal();
        }
        return mergeTraversal;
    }

    public Traversal.Admin getMergeTraversalGValueSafe() {
        if (mergeTraversal != null && mergeTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).getConstantTraversal();
        }
        return mergeTraversal;
    }

    @Override
    public Traversal.Admin getOnCreateTraversal() {
        if (onCreateTraversal != null && onCreateTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, Map<Object, Object>>) onCreateTraversal).getGValue().getName());
            return ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateTraversal).getConstantTraversal();
        }
        return onCreateTraversal;
    }

    public Traversal.Admin getOnCreateTraversalGValueSafe() {
        if (onCreateTraversal != null && onCreateTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateTraversal).getConstantTraversal();
        }
        return onCreateTraversal;
    }

    @Override
    public Traversal.Admin getOnMatchTraversal() {
        if (onMatchTraversal != null && onMatchTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, Map<Object, Object>>) onMatchTraversal).getGValue().getName());
            return ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchTraversal).getConstantTraversal();
        }
        return onMatchTraversal;
    }

    public Traversal.Admin getOnMatchTraversalGValueSafe() {
        if (onMatchTraversal != null && onMatchTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchTraversal).getConstantTraversal();
        }
        return onMatchTraversal;
    }

    @Override
    public boolean isStart() {
        return isStart;
    }

    @Override
    public boolean isFirst() {
        return true; // Step cannot be iterated so isFirst() is always true
    }

    @Override
    public void addChildOption(Merge token, Traversal.Admin traversalOption) {
        if (token == Merge.onCreate) {
            setOnCreate(traversalOption);
        } else if (token == Merge.onMatch) {
            setOnMatch(traversalOption);
        } else {
            throw new UnsupportedOperationException(String.format("Option %s for Merge is not supported", token.name()));
        }
    }

    @Override
    public boolean isUsingPartitionStrategy() {
        return false;
    }

    @Override
    public void setOnMatch(final Traversal.Admin<?,Map<Object, Object>> onMatchMap) {
        this.onMatchTraversal = onMatchMap;
        if (onMatchMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchMap).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) onMatchMap).getGValue());
        }
    }

    @Override
    public void setOnCreate(final Traversal.Admin<?,Map<Object, Object>> onCreateMap) {
        this.onCreateTraversal = onCreateMap;
        if (onCreateMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateMap).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) onCreateMap).getGValue());
        }
    }

    @Override
    public void setMerge(Traversal.Admin<?,Map<Object, Object>> mergeMap) {
        this.mergeTraversal = mergeMap;
        if (mergeMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) mergeMap).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) mergeMap).getGValue());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        MergeVertexStepPlaceholder<?> that = (MergeVertexStepPlaceholder<?>) o;
        return isStart == that.isStart && Objects.equals(properties, that.properties) && Objects.equals(mergeTraversal, that.mergeTraversal) && Objects.equals(onCreateTraversal, that.onCreateTraversal) && Objects.equals(onMatchTraversal, that.onMatchTraversal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), properties, mergeTraversal, onCreateTraversal, onMatchTraversal, isStart);
    }

    @Override
    public MergeVertexStep<S> asConcreteStep() {
        MergeVertexStep<S> step = new MergeVertexStep<>(traversal, isStart,
                mergeTraversal instanceof GValueConstantTraversal ?
                        ((GValueConstantTraversal) mergeTraversal).getConstantTraversal() : mergeTraversal);
        step.setOnCreate(onCreateTraversal instanceof GValueConstantTraversal ?
                ((GValueConstantTraversal) onCreateTraversal).getConstantTraversal() : onCreateTraversal);
        step.setOnMatch(onMatchTraversal instanceof GValueConstantTraversal ?
                ((GValueConstantTraversal) onMatchTraversal).getConstantTraversal() : onMatchTraversal);
        TraversalHelper.copyLabels(this, step, false);

        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value);
            }
        }

        return step;
    }

    @Override
    public boolean isParameterized() {
        return onMatchTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal) onMatchTraversal).isParameterized() ||
                onCreateTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal) onCreateTraversal).isParameterized() ||
                mergeTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal) mergeTraversal).isParameterized();
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (mergeTraversal != null && mergeTraversal instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).updateVariable(name, value);
        }
        if (onMatchTraversal != null && onMatchTraversal instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchTraversal).updateVariable(name, value);
        }
        if (onCreateTraversal != null && onCreateTraversal instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateTraversal).updateVariable(name, value);
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Set<GValue<?>> gValues = new HashSet<>();
        if (mergeTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, ?>) mergeTraversal).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<?, ?>) mergeTraversal).getGValue());
        }
        if (onMatchTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, ?>) onMatchTraversal).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<?, ?>) onMatchTraversal).getGValue());
        }
        if (onCreateTraversal instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, ?>) onCreateTraversal).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<?, ?>) onCreateTraversal).getGValue());
        }
        return gValues;
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        throw new IllegalStateException("Cannot get mutating CallbackRegistry on GValue placeholder step");
    }

    /**
     * This implementation should only be used as a mechanism for supporting {@link PartitionStrategy}.
     */
    @Override
    public void addProperty(Object key, Object value) {
        if (key instanceof GValue) {
            throw new IllegalArgumentException("GValue cannot be used as a property key");
        }
        if (value instanceof GValue) {
            traversal.getGValueManager().register((GValue<?>) value);
        }
        if (properties.containsKey(key)) {
            throw new IllegalArgumentException("MergeElement.addProperty only support properties with single cardinality");
        }
        properties.put(key, Collections.singletonList(value));
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        return GValueHelper.resolveProperties(properties,
                gValue -> traversal.getGValueManager().pinVariable(gValue.getName()));
    }

    public Map<Object, List<Object>> getPropertiesGValueSafe() {
        return GValueHelper.resolveProperties(properties);
    }

    @Override
    public boolean removeProperty(Object k) {
        if (properties.containsKey(k)) {
            properties.remove(k);
            return true;
        }
        return false;
    }
}
