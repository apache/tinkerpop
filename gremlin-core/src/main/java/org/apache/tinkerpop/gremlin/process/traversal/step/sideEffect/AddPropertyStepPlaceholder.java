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
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddPropertyStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;

public class AddPropertyStepPlaceholder<S extends Element> extends AbstractStep<S, S>
        implements AddPropertyStepInterface<S>, GValueHolder<S, S> {

    private Object key;
    private GValue<?> value;
    private VertexProperty.Cardinality cardinality;
    private Map<Object, List<Object>> properties = new HashMap<>();

    public AddPropertyStepPlaceholder(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        super(traversal);
        if (keyObject instanceof GValue) {
            throw new IllegalArgumentException("GValue is not allowed for property keys");
        }
        this.key = keyObject;
        this.value = GValue.of(valueObject);
        this.cardinality = cardinality;
        if (valueObject instanceof GValue) {
            traversal.getGValueManager().track((GValue<?>) valueObject);
        }
    }

    @Override
    public Set<String> getScopeKeys() {
        return Collections.emptySet(); //TODO:: is this right?
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return Collections.emptyList(); //TODO:: is this right?
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ Objects.hashCode(key) ^ Objects.hashCode(value) ^ Objects.hashCode(cardinality);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("GValueHolder is not executable");
    }

    @Override
    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public AddPropertyStepPlaceholder<S> clone() {
        final AddPropertyStepPlaceholder<S> clone = (AddPropertyStepPlaceholder<S>) super.clone();
        clone.cardinality = cardinality;
        clone.key = key;
        clone.value = value;
        clone.properties.putAll(properties);
        return clone;
    }

    @Override
    public AddPropertyStep<S> asConcreteStep() {
        AddPropertyStep<S> step = new AddPropertyStep<>(traversal, cardinality, key, value.get());

        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (Object value : entry.getValue()) {
                step.addProperty(entry.getKey(), value instanceof GValue ? ((GValue<?>) value).get() : value);
            }
        }

        TraversalHelper.copyLabels(this, step, false);
        return step;
    }

    @Override
    public boolean isParameterized() {
        if (value.isVariable()) {
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
        if (name.equals(this.value.getName())) {
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
        Set<GValue<?>> gValues = new HashSet<>();
        if (value.isVariable()) {
            gValues.add(value);
        }
        for (final Map.Entry<Object, List<Object>> entry : properties.entrySet()) {
            for (final Object propertyVal : entry.getValue()) {
                if (propertyVal instanceof GValue && ((GValue<?>) propertyVal).isVariable()) {
                    gValues.add((GValue<?>) propertyVal);
                }
            }
        }
        return gValues;
    }

    @Override
    public void addProperty(Object key, Object value) {
        if (key instanceof GValue) {
            throw new IllegalArgumentException("GValue cannot be used as a property key");
        }
        if (value instanceof GValue) { //TODO could value come in as a traversal?
            traversal.getGValueManager().track((GValue<?>) value);
        }
        if (properties.containsKey(key)) {
            throw new IllegalArgumentException("Only single value meta-properties are supported");
        }
        properties.put(key, Collections.singletonList(value));
    }

    @Override
    public Map<Object, List<Object>> getProperties() {
        for (List<Object> list : properties.values()) {
            for (Object value : list) {
                if (value instanceof GValue) {
                    traversal.getGValueManager().pinVariable(((GValue<?>) value).getName());
                }
            }
        }
        return properties;
    }

    public Map<Object, List<Object>> getPropertiesGValueSafe() {
        return properties;
    }

    @Override
    public void removeProperty(Object k) {
        properties.remove(k);
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        throw new IllegalStateException("Cannot get mutating CallbackRegistry on GValue placeholder step");
    }
}
