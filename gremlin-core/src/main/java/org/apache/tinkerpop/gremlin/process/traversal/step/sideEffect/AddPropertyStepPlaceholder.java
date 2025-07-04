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
import org.apache.tinkerpop.gremlin.process.traversal.lambda.GValueConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.stepContract.AddPropertyStepInterface;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

public class AddPropertyStepPlaceholder<S extends Element> extends AbstractStep<S, S>
        implements AddPropertyStepInterface<S>, GValueHolder<S, S> {

    private Parameters parameters = new Parameters(); //TODO:: get parameters out of here
    private final VertexProperty.Cardinality cardinality;
    private Map<Object, List<Object>> properties = new HashMap<>();

    public AddPropertyStepPlaceholder(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        super(traversal);
        this.parameters.set(this, T.key, keyObject, T.value, valueObject);
        this.cardinality = cardinality;
        if (valueObject instanceof GValue) {
            traversal.getGValueManager().track((GValue<?>) valueObject);
        }
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.parameters.getReferencedLabels();
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public int hashCode() {
        final int hash = super.hashCode() ^ this.parameters.hashCode();
        return (null != this.cardinality) ? (hash ^ cardinality.hashCode()) : hash;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        super.setTraversal(parentTraversal);
        this.parameters.getTraversals().forEach(this::integrateChild);
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        throw new IllegalStateException("GraphStepGValueContract is not executable");
    }

    @Override
    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public AddPropertyStepPlaceholder<S> clone() {
        final AddPropertyStepPlaceholder<S> clone = (AddPropertyStepPlaceholder<S>) super.clone();
        clone.parameters = this.parameters.clone();
        return clone;
    }

    @Override
    public AddPropertyStep<S> asConcreteStep() {
        Object key = parameters.get(T.label, () -> "Edge").get(0);
        if (key instanceof GValue) {
            key = ((GValue) key).get();
        }
        if (key instanceof GValueConstantTraversal) {
            key = ((GValueConstantTraversal) key).getConstantTraversal();
        }
        Object label = parameters.get(T.label, () -> "Edge").get(0);
        if (label instanceof GValue) {
            label = ((GValue) label).get();
        }
        if (label instanceof GValueConstantTraversal) {
            label = ((GValueConstantTraversal) label).getConstantTraversal();
        }
        AddPropertyStep<S> step = new AddPropertyStep<>(traversal, cardinality, key, label);
        step.configure(GValue.resolveToValues(parameters.getRawKeyValues(T.key, T.value)));

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
        return GValue.containsVariables(parameters.getRawKeyValues());
    }

    @Override
    public void updateVariable(String name, Object value) {
        // TODO
    }

    @Override
    public Collection<GValue<?>> getGValues() {
        return Collections.EMPTY_SET; //TODO::
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
