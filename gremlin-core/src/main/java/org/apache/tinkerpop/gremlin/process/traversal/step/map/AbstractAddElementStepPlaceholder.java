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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractAddElementStepPlaceholder<S, E extends Element, X extends Event> extends AbstractStep<S, E>
        implements AddElementStepContract<S, E>, GValueHolder<S, E>, Writing<X> {

    protected Traversal.Admin<S, String> label;
    protected Map<Object, List<Object>> properties = new HashMap<>();
    protected GValue<Object> elementId;
    protected Set<String> scopeKeys = new HashSet<>();

    public AbstractAddElementStepPlaceholder(final Traversal.Admin traversal, final String label) {
        this(traversal, label == null ? null : new ConstantTraversal<>(label));
    }

    public AbstractAddElementStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        this(traversal.asAdmin(), label == null ? null : new GValueConstantTraversal<>(label));
    }

    public AbstractAddElementStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> labelTraversal) {
        super(traversal);
        if (labelTraversal == null) {
            throw new IllegalArgumentException("The label provided must not be null");
        }
        this.label = labelTraversal;
        if (labelTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().register(((GValueConstantTraversal<S, String>) labelTraversal).getGValue());
        }
        addTraversal(labelTraversal);
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.unmodifiableSet(scopeKeys);
    }

    protected void addTraversal(final Traversal.Admin<?, ?> traversal) {
        TraversalHelper.getStepsOfAssignableClassRecursively(Scoping.class, traversal).forEach(s -> scopeKeys.addAll(s.getScopeKeys()));
    }

    @Override
    public List<Traversal.Admin<S, E>> getLocalChildren() {
        List<Traversal.Admin<S, E>> childTraversals = new ArrayList<>();
        for (List<Object> values : properties.values()) {
            for (Object value : values) {
                if (value instanceof Traversal) {
                    childTraversals.add((Traversal.Admin<S, E>) ((Traversal) value).asAdmin());
                }
            }
        }
        return childTraversals;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("GValuePlaceholder step is not executable");
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        if (label != null) {
            hash ^= label.hashCode();
        }
        if (properties != null) {
            for (Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
                hash ^= Objects.hashCode(entry.getKey());
                hash ^= Objects.hashCode(entry.getValue());
            };
        }
        return hash;
    }

    protected void configureConcreteStep(AddElementStepContract<S, E> step) {
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value instanceof GValue ? ((GValue<?>) value).get() : value);
            }
        }
        if (elementId != null) {
            step.setElementId(elementId.get());
        }
        TraversalHelper.copyLabels(this, step, false);
    }

    @Override
    public boolean isParameterized() {
        if (label instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) label).isParameterized() ||
                (elementId != null && elementId.isVariable())) {
            return true;
        }
        for (List<Object> list : properties.values()) {
            if (GValue.containsVariables(list.toArray())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getLabel() {
        if (label instanceof GValueConstantTraversal) {
            traversal.getGValueManager().pinVariable(((GValueConstantTraversal<?, ?>) label).getGValue().getName());
        }
        return label.next();
    }

    public String getLabelGValueSafe() {
        return label.next();
    }

    private void setLabel(Object label) {// TODO should this be public and added to step interface?
        if (getLabelGValueSafe().equals(Vertex.DEFAULT_LABEL)) {
            this.label = label instanceof GValue ? new GValueConstantTraversal<>((GValue) label) : new ConstantTraversal<>((String) label);
            if (label instanceof GValue) {
                traversal.getGValueManager().register((GValue<?>) label);
            }
        }
    }

    @Override
    public void addProperty(Object key, Object value) {
        if (key instanceof GValue) {
            throw new IllegalArgumentException("GValue cannot be used as a property key");
        }
        if (value instanceof GValue) {
            traversal.getGValueManager().register((GValue<?>) value);
        }
        if (key == T.label) {
            setLabel(value);
            return;
        }
        if (key == T.id) {
            setElementId(value);
            return;
        }
        if (!supportsMultiProperties() && properties.containsKey(key)) {
            throw new IllegalArgumentException("Multi-properties are not supported by this step");
        }
        if (value instanceof Traversal) {
            this.integrateChild(((Traversal<?,?>) value).asAdmin());
        }
        List<Object> values = properties.get(key);
        if (values == null) {
            values = new ArrayList<>();
            properties.put(key, values);
        }
        values.add(value);
    }

    protected abstract boolean supportsMultiProperties();

    @Override
    public Map<Object, List<Object>> getProperties() {
        return GValueHelper.resolveProperties(properties,
                gValue -> traversal.getGValueManager().pinVariable(gValue.getName()));
    }

    public Map<Object, List<Object>> getPropertiesGValueSafe() {
        return GValueHelper.resolveProperties(properties);
    }

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

    @Override
    public Object getElementId() {
        if (elementId == null) {
            return null;
        }
        if (elementId.isVariable()) {
            this.traversal.getGValueManager().pinVariable(elementId.getName());
        }
        return elementId.get();
    }

    public Object getElementIdGValueSafe() {
        if (elementId == null) {
            return null;
        }
        return elementId.get();
    }

    @Override
    public boolean removeElementId() {
        if (elementId == null) {
            return false;
        }
        elementId = null;
        return true;
    }

    @Override
    public void setElementId(Object elementId) {
        this.elementId = elementId instanceof GValue ? (GValue<Object>) elementId : GValue.of(elementId);
        this.traversal.getGValueManager().register(this.elementId);
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (label instanceof GValueConstantTraversal) {
            ((GValueConstantTraversal<S, String>) label).updateVariable(name, value);
        }
        if (elementId != null && name.equals(elementId.getName())) {
            elementId = GValue.of(name, value);
        }
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object propertyVal : entry.getValue()) {
                if (propertyVal instanceof GValue && name.equals(((GValue<?>) propertyVal).getName())) {
                    properties.put(entry.getKey(), Collections.singletonList(GValue.of(name, value)));
                }
            }
        }
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        Set<GValue<?>> gValues = GValueHelper.getGValuesFromProperties(properties);
        if (label instanceof GValueConstantTraversal && ((GValueConstantTraversal<S, String>) label).getGValue().isVariable()) {
            gValues.add(((GValueConstantTraversal<S, String>) label).getGValue());
        }
        if (elementId != null && elementId.isVariable()) {
            gValues.add(elementId);
        }
        return gValues;
    }

    @Override
    public CallbackRegistry<X> getMutatingCallbackRegistry() {
        throw new IllegalStateException("Cannot get mutating CallbackRegistry on GValue placeholder step");
    }

    @Override
    public AbstractAddElementStepPlaceholder<S, E, X> clone() {
        final AbstractAddElementStepPlaceholder<S, E, X> clone = (AbstractAddElementStepPlaceholder) super.clone();
        if (label != null) {
            clone.label = label.clone();
        }

        // deep clone properties
        clone.properties = new HashMap<>();
        for (Map.Entry<Object, List<Object>> entry : this.properties.entrySet()) {
            final Object key = entry.getKey();
            final List<Object> oldValues = entry.getValue();
            final List<Object> newValues = new ArrayList<>(oldValues.size());
            for (Object v : oldValues) {
                if (v instanceof Traversal) {
                    newValues.add(((Traversal<?, ?>) v).asAdmin().clone());
                } else if (v instanceof GValue) {
                    try {
                        newValues.add(((GValue) v).clone());
                    } catch (CloneNotSupportedException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    newValues.add(v);
                }
            }
            clone.properties.put(key, newValues);
        }

        clone.elementId = this.elementId;
        clone.scopeKeys = new HashSet<>(this.scopeKeys);
        return clone;
    }

}
