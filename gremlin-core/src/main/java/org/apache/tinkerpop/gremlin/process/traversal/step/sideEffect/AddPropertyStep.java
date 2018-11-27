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
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
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

import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class AddPropertyStep<S extends Element> extends SideEffectStep<S>
        implements Mutating<Event.ElementPropertyChangedEvent>, TraversalParent, Scoping {

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
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public void configure(final Object... keyValues) {
        this.parameters.set(this, keyValues);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final String key = (String) this.parameters.get(traverser, T.key, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided key: " + this);
        }).get(0);
        final Object value = this.parameters.get(traverser, T.value, () -> {
            throw new IllegalStateException("The AddPropertyStep does not have a provided value: " + this);
        }).get(0);
        final Object[] vertexPropertyKeyValues = this.parameters.getKeyValues(traverser, T.key, T.value);

        final Element element = traverser.get();

        if (this.callbackRegistry != null) {
            final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
            final Property currentProperty = traverser.get().property(key);
            final boolean newProperty = element instanceof Vertex ? currentProperty == VertexProperty.empty() : currentProperty == Property.empty();
            final Event.ElementPropertyChangedEvent evt;
            if (element instanceof Vertex)
                evt = new Event.VertexPropertyChangedEvent(eventStrategy.detach((Vertex) element),
                        newProperty ?
                                new KeyedVertexProperty(key) :
                                eventStrategy.detach((VertexProperty) currentProperty), value, vertexPropertyKeyValues);
            else if (element instanceof Edge)
                evt = new Event.EdgePropertyChangedEvent(eventStrategy.detach((Edge) element),
                        newProperty ?
                                new KeyedProperty(key) :
                                eventStrategy.detach(currentProperty), value);
            else if (element instanceof VertexProperty)
                evt = new Event.VertexPropertyPropertyChangedEvent(eventStrategy.detach((VertexProperty) element),
                        newProperty ?
                                new KeyedProperty(key) :
                                eventStrategy.detach(currentProperty), value);
            else
                throw new IllegalStateException(String.format("The incoming object cannot be processed by change eventing in %s:  %s", AddPropertyStep.class.getName(), element));

            this.callbackRegistry.getCallbacks().forEach(c -> c.accept(evt));
        }

        if (null != this.cardinality)
            ((Vertex) element).property(this.cardinality, key, value, vertexPropertyKeyValues);
        else if (vertexPropertyKeyValues.length > 0)
            ((Vertex) element).property(key, value, vertexPropertyKeyValues);
        else
            element.property(key, value);
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
