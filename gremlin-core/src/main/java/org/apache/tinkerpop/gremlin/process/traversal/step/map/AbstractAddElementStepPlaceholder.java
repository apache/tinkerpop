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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public abstract class AbstractAddElementStepPlaceholder<S, E extends Element, X extends Event> extends ScalarMapStep<S, E>
        implements AddElementStepContract<S, E>, GValueHolder<S, E>, Writing<X> {

    protected Object label;
    protected Map<Object, List<Object>> properties = new HashMap<>();
    protected Object elementId;
    protected Set<String> scopeKeys = new HashSet<>();
    protected Parameters withConfiguration = new Parameters();

    protected AbstractAddElementStepPlaceholder(final Traversal.Admin traversal, final String label) {
        super(traversal);
        this.label = label == null ? this.getDefaultLabel() : label;
    }

    protected AbstractAddElementStepPlaceholder(final Traversal.Admin traversal, final GValue<String> label) {
        super(traversal);
        this.label = label == null ? this.getDefaultLabel() : label;
        if (label.isVariable()) {
            traversal.getGValueManager().register(label);
        }
    }

    protected AbstractAddElementStepPlaceholder(final Traversal.Admin traversal, final Traversal.Admin<S,String> labelTraversal) {
        super(traversal);
        this.label = labelTraversal == null ? this.getDefaultLabel() : labelTraversal;
        if (labelTraversal instanceof GValueConstantTraversal) {
            traversal.getGValueManager().register(((GValueConstantTraversal<S, String>) labelTraversal).getGValue());
        }
        addTraversal(labelTraversal);
    }

    protected abstract String getDefaultLabel();

    @Override
    public Set<String> getScopeKeys() {
        return Collections.unmodifiableSet(scopeKeys);
    }

    protected void addTraversal(final Traversal.Admin<?, ?> traversal) {
        integrateChild(traversal);
        TraversalHelper.getStepsOfAssignableClassRecursively(Scoping.class, traversal).forEach(s -> scopeKeys.addAll(s.getScopeKeys()));
    }

    @Override
    public List<Traversal.Admin<?, ?>> getLocalChildren() {
        List<Traversal.Admin<?, ?>> childTraversals = new ArrayList<>();
        for (Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            if (entry.getKey() instanceof Traversal) {
                childTraversals.add((Traversal.Admin<?, ?>) ((Traversal) entry.getKey()).asAdmin());
            }
            for (Object value : entry.getValue()) {
                if (value instanceof Traversal) {
                    childTraversals.add((Traversal.Admin<?, ?>) ((Traversal) value).asAdmin());
                }
            }
        }
        if (label != null && label instanceof Traversal) {
            childTraversals.add(((Traversal<?, ?>) label).asAdmin());
        }
        if (elementId != null && elementId instanceof Traversal) {
            childTraversals.add(((Traversal<?, ?>) elementId).asAdmin());
        }
        return childTraversals;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.getLocalChildren().forEach(this::integrateChild);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    protected E map(Traverser.Admin<S> traverser) {
        throw new IllegalStateException("GValuePlaceholder step is not executable");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AbstractAddElementStepPlaceholder<?, ?, ?> that = (AbstractAddElementStepPlaceholder<?, ?, ?>) o;
        return Objects.equals(label, that.label) &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(elementId, that.elementId) &&
                Objects.equals(scopeKeys, that.scopeKeys) &&
                Objects.equals(withConfiguration, that.withConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), label, properties, elementId, scopeKeys, withConfiguration);
    }

    protected void configureConcreteStep(AddElementStepContract<S, E> step) {
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value instanceof GValue ? ((GValue<?>) value).get() : value);
            }
        }
        if (elementId != null) {
            step.setElementId(elementId instanceof GValue ? ((GValue<?>) elementId).get() : elementId);
        }
        for (Map.Entry<Object, List<Object>> entry : withConfiguration.getRaw().entrySet()) {
            for (Object value : entry.getValue()) {
                step.configure(entry.getKey(), value);
            }
        }
        TraversalHelper.copyLabels(this, step, false);
    }

    @Override
    public boolean isParameterized() {
        if (label instanceof GValue && ((GValue<?>) label).isVariable() ||
                (elementId instanceof GValue && ((GValue<?>) elementId).isVariable())) {
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
    public Object getLabel() {
        if (label instanceof GValue) {
            traversal.getGValueManager().pinVariable(((GValue<?>) label).getName());
            return ((GValue<?>) label).get();
        }
        return label;
    }

    @Override
    public Object getLabelWithGValue() {
        return label;
    }

    @Override
    public void setLabel(Object label) {
        if (this.label.equals(this.getDefaultLabel())) {
            if (label instanceof Traversal) {
                this.label = ((Traversal<S, String>) label).asAdmin();
                this.integrateChild((Traversal.Admin<?, ?>) this.label);
            } else if (label instanceof GValue) {
                this.label = label;
                traversal.getGValueManager().register((GValue<?>) label);
            } else {
                this.label = label;
            }
        } else {
            throw new IllegalArgumentException(String.format("Element T.label has already been set to [%s] and cannot be overridden with [%s]",
                    this.label, label));
        }
    }

    @Override
    public void addProperty(Object key, Object value) {
        if (key instanceof GValue) {
            throw new IllegalArgumentException("GValue cannot be used as a property key");
        }
        if (key instanceof Traversal) {
            this.integrateChild(((Traversal<?, ?>) key).asAdmin());
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

    @Override
    public Object getElementId() {
        if (elementId instanceof GValue) {
            this.traversal.getGValueManager().pinVariable(((GValue<?>) elementId).getName());
            return ((GValue<?>) elementId).get();
        }
        return elementId;
    }

    @Override
    public Object getElementIdWithGValue() {
        return elementId;
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
        if (this.elementId != null) {
            throw new IllegalArgumentException(String.format("Element T.id has already been set to [%s] and cannot be overridden with [%s]",
                    this.elementId, elementId));
        }
        if (elementId instanceof Traversal) {
            this.elementId = ((Traversal<S, Object>) elementId).asAdmin();
            this.integrateChild((Traversal.Admin<?, ?>) this.elementId);
        } else if (elementId instanceof GValue) {
            traversal.getGValueManager().register((GValue<?>) elementId);
            this.elementId = elementId;
        } else {
            this.elementId = elementId;
        }
    }

    @Override
    public void updateVariable(String name, Object value) {
        if (label instanceof GValue && name.equals(((GValue<?>) label).getName())) {
            label = GValue.of(name, value);
        }
        if (elementId instanceof GValue && name.equals(((GValue<?>) elementId).getName())) {
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
        if (label instanceof GValue && ((GValue<?>) label).isVariable()) {
            gValues.add((GValue<?>) label);
        }
        if (elementId instanceof GValue && ((GValue<?>) elementId).isVariable()) {
            gValues.add((GValue<?>) elementId);
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
            // Attempt to deep clone label for Traversal and GValue.
            if (this.label instanceof Traversal) {
                clone.label = ((Traversal<?, ?>) this.label).asAdmin().clone();
            } else if (this.label instanceof GValue) {
                clone.label = ((GValue<?>) this.label).clone();
            } else {
                clone.label = this.label;
            }
        }
        if (elementId != null) {
            // Attempt to deep clone elementId for Traversal and GValue.
            if (this.elementId instanceof Traversal) {
                clone.elementId = ((Traversal<?, ?>) this.elementId).asAdmin().clone();
            } else if (this.elementId instanceof GValue) {
                clone.elementId = ((GValue<?>) this.elementId).clone();
            } else {
                clone.elementId = this.elementId;
            }
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
                    newValues.add(((GValue) v).clone());
                } else {
                    newValues.add(v);
                }
            }
            clone.properties.put(key, newValues);
        }

        clone.scopeKeys = new HashSet<>(this.scopeKeys);
        clone.withConfiguration = this.withConfiguration.clone();
        return clone;
    }

    @Override
    public Parameters getParameters() {
        return this.withConfiguration;
    }

    @Override
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }
}
