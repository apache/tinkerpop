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
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.keyed.KeyedProperty;
import org.apache.tinkerpop.gremlin.structure.util.keyed.KeyedVertexProperty;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddPropertyStep<S extends Element> extends SideEffectStep<S>
        implements Writing<Event.ElementPropertyChangedEvent>, Deleting<Event.ElementPropertyChangedEvent>, TraversalParent, Scoping {

    private Parameters parameters = new Parameters();
    private final VertexProperty.Cardinality cardinality;
    private CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry;

    public AddPropertyStep(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        super(traversal);
        this.parameters.set(this, T.key, keyObject, T.value, valueObject);
        this.cardinality = cardinality;
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public Set<String> getScopeKeys() {
        return this.parameters.getReferencedLabels();
    }

    @Override
    public HashSet<PopInstruction> getPopInstructions() {
        final HashSet<PopInstruction> popInstructions = new HashSet<>();
        popInstructions.addAll(Scoping.super.getPopInstructions());
        popInstructions.addAll(TraversalParent.super.getPopInstructions());
        return popInstructions;
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final Object k = this.parameters.get(traverser, T.key, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided key: " + this);
        }).get(0);

        // T identifies immutable components of elements. only property keys can be modified.
        if (k instanceof T)
            throw new IllegalStateException(String.format("T.%s is immutable on existing elements", ((T) k).name()));

        final String key = (String) k;
        final Object value = this.parameters.get(traverser, T.value, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided value: " + this);
        }).get(0);
        final Object[] vertexPropertyKeyValues = this.parameters.getKeyValues(traverser, T.key, T.value);

        final Element element = traverser.get();

        // can't set cardinality if the element is something other than a vertex as only vertices can have
        // a cardinality of properties. if we don't throw an error here we end up with a confusing cast exception
        // which doesn't explain what went wrong
        if (this.cardinality != null && !(element instanceof Vertex))
            throw new IllegalStateException(String.format(
                    "Property cardinality can only be set for a Vertex but the traversal encountered %s for key: %s",
                    element.getClass().getSimpleName(), key));

        final Optional<EventStrategy> optEventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class);
        final boolean eventingIsConfigured = EventUtil.hasAnyCallbacks(callbackRegistry)
                && optEventStrategy.isPresent();
        final EventStrategy es = optEventStrategy.orElse(null);

        // find property to remove. only need to capture the removedProperty if eventing is configured
        final Property removedProperty = eventingIsConfigured ?
                captureRemovedProperty(element, key, value, es) :
                VertexProperty.empty();

        final VertexProperty.Cardinality card = this.cardinality != null
                ? this.cardinality
                : element.graph().features().vertex().getCardinality(key);

        // update property
        if (element instanceof Vertex) {
            if (null != card) {
                ((Vertex) element).property(card, key, value, vertexPropertyKeyValues);
            } else if (vertexPropertyKeyValues.length > 0) {
                ((Vertex) element).property(key, value, vertexPropertyKeyValues);
            } else {
                ((Vertex) element).property(key, value);
            }
        } else if (element instanceof Edge) {
            element.property(key, value);
        } else if (element instanceof VertexProperty) {
            element.property(key, value);
        }

        // trigger event callbacks
        if (eventingIsConfigured) {
            EventUtil.registerPropertyChange(callbackRegistry, es, element, removedProperty, value, vertexPropertyKeyValues);
        }
    }

    private Property captureRemovedProperty(final Element element, final String key, final Object value,
                                            final EventStrategy es){
        Property removedProperty = VertexProperty.empty();

        if (element instanceof Vertex) {
            if (cardinality == VertexProperty.Cardinality.set) {
                final Iterator<? extends Property> properties = element.properties(key);
                while (properties.hasNext()) {
                    final Property property = properties.next();
                    if (Objects.equals(property.value(), value)) {
                        removedProperty = property;
                        break;
                    }
                }
            } else if (cardinality == VertexProperty.Cardinality.single) {
                removedProperty = element.property(key);
            }
        } else {
            removedProperty = element.property(key);
        }

        // detach removed property
        if (removedProperty.isPresent()) {
            removedProperty = es.detach(removedProperty);
        } else {
            removedProperty = element instanceof Vertex ? new KeyedVertexProperty(key) : new KeyedProperty(key);
        }

        return removedProperty;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        if (null == this.callbackRegistry) this.callbackRegistry = new ListCallbackRegistry<>();
        return this.callbackRegistry;
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

    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.parameters);
    }

    @Override
    public AddPropertyStep<S> clone() {
        final AddPropertyStep<S> clone = (AddPropertyStep<S>) super.clone();
        clone.parameters = this.parameters.clone();
        return clone;
    }
}
