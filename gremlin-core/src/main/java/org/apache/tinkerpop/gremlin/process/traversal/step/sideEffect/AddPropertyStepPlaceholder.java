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
package org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.GValueHelper;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class AddPropertyStepPlaceholder<S extends Element> extends SideEffectStep<S>
        implements AddPropertyStepContract<S>, GValueHolder<S, S> {

    /**
     * property key
     */
    private Object key;
    /**
     * property value
     */
    private Object value;
    /**
     * cardinality of the property
     */
    private VertexProperty.Cardinality cardinality;
    /**
     * meta-properties of the property
     */
    private Map<Object, List<Object>> properties = new HashMap<>();

    private Parameters withConfiguration = new Parameters();

    public AddPropertyStepPlaceholder(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        super(traversal);
        if (keyObject instanceof GValue) {
            throw new IllegalArgumentException("GValue is not allowed for property keys");
        }
        this.key = keyObject;
        if (this.key instanceof Traversal) {
            this.integrateChild(((Traversal<?, ?>) this.key).asAdmin());
        }
        if (valueObject instanceof GValue) {
            traversal.getGValueManager().register((GValue<?>) valueObject);
        }
        this.value = valueObject;
        if (this.value instanceof Traversal) {
            this.integrateChild(((Traversal<?, ?>) this.value).asAdmin());
        }
        this.cardinality = cardinality;
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.emptySet(); //TODO:: is this right?
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
        if (key != null && key instanceof Traversal) {
            childTraversals.add(((Traversal<?, ?>) key).asAdmin());
        }
        if (value != null && value instanceof Traversal) {
            childTraversals.add(((Traversal<?, ?>) value).asAdmin());
        }
        return childTraversals;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.getLocalChildren().forEach(this::integrateChild);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        AddPropertyStepPlaceholder<?> that = (AddPropertyStepPlaceholder<?>) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                cardinality == that.cardinality &&
                Objects.equals(properties, that.properties) &&
                Objects.equals(withConfiguration, that.withConfiguration);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), key, value, cardinality, properties, withConfiguration);
    }

    @Override
    protected void sideEffect(Traverser.Admin<S> traverser) {
        throw new IllegalStateException("AddPropertyStepPlaceholder is not executable");
    }

    @Override
    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public Object getKey() {
        return key;
    }

    @Override
    public Object getValue() {
        if (value == null) {
            return null;
        }
        if (value instanceof GValue) {
            traversal.getGValueManager().pinVariable(((GValue<?>) value).getName());
            return ((GValue<?>) value).get();
        }
        return value;
    }

    /**
     * Get the value as-is, without unboxing GValues if present, and without pinning the variable
     */
    @Override
    public Object getValueWithGValue() {
        return value;
    }

    @Override
    public AddPropertyStepPlaceholder<S> clone() {
        final AddPropertyStepPlaceholder<S> clone = (AddPropertyStepPlaceholder<S>) super.clone();
        clone.cardinality = cardinality;

        // Attempt to deep clone key for Traversal and GValue. Shallow copy is fine if key is a String or enum
        if (this.key instanceof Traversal) {
            clone.key = ((Traversal<?, ?>) this.key).asAdmin().clone();
        } else if (this.key instanceof GValue) {
            clone.key = ((GValue<?>) this.key).clone();
        } else {
            clone.key = this.key;
        }

        // Attempt to deep clone value for Traversal and GValue.
        if (this.value instanceof Traversal) {
            clone.value = ((Traversal<?, ?>) this.value).asAdmin().clone();
        } else if (this.value instanceof GValue) {
            clone.value = ((GValue<?>) this.value).clone();
        } else {
            clone.value = this.value;
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

        return clone;
    }

    @Override
    public AddPropertyStep<S> asConcreteStep() {
        AddPropertyStep<S> step = new AddPropertyStep<>(traversal, cardinality, key, value instanceof GValue ? ((GValue<?>) value).get() : value);

        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value instanceof GValue ? ((GValue<?>) value).get() : value);
            }
        }

        for (Map.Entry<Object, List<Object>> entry : withConfiguration.getRaw().entrySet()) {
            for (Object value : entry.getValue()) {
                step.configure(entry.getKey(), value);
            }
        }

        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        if (value instanceof GValue && ((GValue<?>) value).isVariable()) {
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
    public void updateVariable(String name, Object value) {
        if (this.value instanceof GValue && name.equals(((GValue<?>) this.value).getName())) {
            this.value = GValue.of(name, value);
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
        if (value instanceof GValue && ((GValue<?>) value).isVariable()) {
            gValues.add((GValue<?>) value);
        }
        return gValues;
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
        if (value instanceof Traversal) {
            this.integrateChild(((Traversal<?, ?>) value).asAdmin());
        }
        if (properties.containsKey(key)) {
            throw new IllegalArgumentException("Only single value meta-properties are supported");
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

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        throw new IllegalStateException("Cannot get mutating CallbackRegistry on GValue placeholder step");
    }

    @Override
    public void configure(final Object... keyValues) {
        this.withConfiguration.set(this, keyValues);
    }

    @Override
    public Parameters getParameters() {
        return withConfiguration;
    }
}
