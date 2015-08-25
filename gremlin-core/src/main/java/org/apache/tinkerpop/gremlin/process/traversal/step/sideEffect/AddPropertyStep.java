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

import org.apache.tinkerpop.gremlin.process.traversal.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AddPropertyStep<S extends Element> extends SideEffectStep<S> implements Mutating<Event.ElementPropertyChangedEvent>, TraversalParent, Parameterizing {

    private final Parameters parameters = new Parameters();

    private final VertexProperty.Cardinality cardinality;
    private CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry;

    public AddPropertyStep(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final Object keyObject, final Object valueObject) {
        super(traversal);
        this.parameters.set(T.key, keyObject);
        this.parameters.set(T.value, valueObject);
        this.cardinality = cardinality;
        this.parameters.integrateTraversals(this);
    }

    @Override
    public Parameters getParameters() {
        return this.parameters;
    }

    @Override
    public <S, E> List<Traversal.Admin<S, E>> getLocalChildren() {
        return this.parameters.getTraversals();
    }

    @Override
    public void addPropertyMutations(final Object... keyValues) {
        for (int i = 0; i < keyValues.length; i = i + 2) {
            this.parameters.set(keyValues[i], keyValues[i + 1]);
        }
        this.parameters.integrateTraversals(this);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        final String key = this.parameters.get(traverser, T.key, null);
        final Object value = this.parameters.get(traverser, T.value, null);
        final Object[] vertexPropertyKeyValues = this.parameters.getKeyValues(traverser, T.key, T.value);

        if (this.callbackRegistry != null) {
            final Element currentElement = traverser.get();
            final Property currentProperty = traverser.get().property(key);

            // todo: have to do a runtime check until TINKERPOP3-783 is done - asVertex is not reliable
            final boolean runtimeAsVertex = currentElement instanceof Vertex;
            final boolean newProperty = runtimeAsVertex ? currentProperty == VertexProperty.empty() : currentProperty == Property.empty();

            Event.ElementPropertyChangedEvent evt;
            if (currentElement instanceof Vertex)
                evt = new Event.VertexPropertyChangedEvent(DetachedFactory.detach((Vertex) currentElement, true), newProperty ? null : DetachedFactory.detach((VertexProperty) currentProperty, true), value, vertexPropertyKeyValues);
            else if (currentElement instanceof Edge)
                evt = new Event.EdgePropertyChangedEvent(DetachedFactory.detach((Edge) currentElement, true), newProperty ? null : DetachedFactory.detach(currentProperty), value);
            else if (currentElement instanceof VertexProperty)
                evt = new Event.VertexPropertyPropertyChangedEvent(DetachedFactory.detach((VertexProperty) currentElement, true), newProperty ? null : DetachedFactory.detach(currentProperty), value);
            else
                throw new IllegalStateException(String.format("The incoming object cannot be processed by change eventing in %s:  %s", AddPropertyStep.class.getName(), currentElement));

            callbackRegistry.getCallbacks().forEach(c -> c.accept(evt));
        }

        if (null != this.cardinality)
            ((Vertex) traverser.get()).property(this.cardinality, key, value, vertexPropertyKeyValues);
        else
            traverser.get().property(key, value);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return this.getSelfAndChildRequirements(TraverserRequirement.OBJECT);
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }


    @Override
    public int hashCode() {
        return super.hashCode() ^ this.parameters.hashCode() ^ ((null == this.cardinality) ? "null".hashCode() : this.cardinality.hashCode());
    }

    // TODO clone()
}
