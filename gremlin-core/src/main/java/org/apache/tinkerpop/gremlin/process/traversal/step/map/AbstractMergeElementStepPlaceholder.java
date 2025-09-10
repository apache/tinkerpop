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
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.T;

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

public abstract class AbstractMergeElementStepPlaceholder<S, E> extends AbstractStep<S, E> implements MergeStepContract<S, E, Map>, GValueHolder<S, E> {
    protected static final Set allowedTokens = new LinkedHashSet(Arrays.asList(T.id, T.label));
    protected final boolean isStart;
    protected Map<Object, List<Object>> properties = new HashMap<>();
    protected Traversal.Admin<?, Map<Object, Object>> mergeTraversal;
    protected Traversal.Admin<?, Map<Object, Object>> onCreateTraversal;
    protected Traversal.Admin<?, Map<Object, Object>> onMatchTraversal;

    public AbstractMergeElementStepPlaceholder(Traversal.Admin traversal, final Traversal.Admin<?, Map<Object, Object>> mergeTraversal, final boolean isStart) {
        super(traversal);
        this.mergeTraversal = mergeTraversal;
        this.isStart = isStart;
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("GValuePlaceholder step is not executable");
    }

    @Override
    public AbstractMergeElementStepPlaceholder<S, E> clone() {
        return (AbstractMergeElementStepPlaceholder<S, E>) super.clone();
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

    @Override
    public Object getMergeMapWithGValue() {
        if (mergeTraversal == null) {
            return null;
        }
        if (mergeTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, Map<Object, Object>>) mergeTraversal).getGValue();
        }
        if (mergeTraversal instanceof ConstantTraversal) {
            return mergeTraversal.next();
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

    @Override
    public Object getOnCreateMapWithGValue() {
        if (onCreateTraversal == null) {
            return null;
        }
        if (onCreateTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateTraversal).getGValue();
        }
        if (onCreateTraversal instanceof ConstantTraversal) {
            return onCreateTraversal.next();
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

    @Override
    public Object getOnMatchMapWithGValue() {
        if (onMatchTraversal == null) {
            return null;
        }
        if (onMatchTraversal instanceof GValueConstantTraversal) {
            return ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchTraversal).getGValue();
        }
        if (onMatchTraversal instanceof ConstantTraversal) {
            return onMatchTraversal.next();
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
    public void setOnMatch(final Traversal.Admin<?, Map<Object, Object>> onMatchMap) {
        this.onMatchTraversal = onMatchMap;
        if (onMatchMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) onMatchMap).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) onMatchMap).getGValue());
        }
    }

    @Override
    public void setOnCreate(final Traversal.Admin<?, Map<Object, Object>> onCreateMap) {
        this.onCreateTraversal = onCreateMap;
        if (onCreateMap instanceof GValueConstantTraversal && ((GValueConstantTraversal<?, Map<Object, Object>>) onCreateMap).isParameterized()) {
            traversal.getGValueManager().register(((GValueConstantTraversal<?, Map<Object, Object>>) onCreateMap).getGValue());
        }
    }

    @Override
    public void setMerge(Traversal.Admin<?, Map<Object, Object>> mergeMap) {
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
        AbstractMergeElementStepPlaceholder<?, ?> that = (AbstractMergeElementStepPlaceholder<?, ?>) o;
        return isStart == that.isStart && Objects.equals(properties, that.properties) && Objects.equals(mergeTraversal, that.mergeTraversal) && Objects.equals(onCreateTraversal, that.onCreateTraversal) && Objects.equals(onMatchTraversal, that.onMatchTraversal);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), properties, mergeTraversal, onCreateTraversal, onMatchTraversal, isStart);
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

    @Override
    public Map<Object, List<Object>> getPropertiesWithGValues() {
        return Collections.unmodifiableMap(properties);
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
