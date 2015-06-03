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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.EnumSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class AddPropertyStep<S extends Element> extends SideEffectStep<S> implements Mutating<Event.ElementPropertyChangedEvent> {

    private static final Set<TraverserRequirement> REQUIREMENTS = EnumSet.of(TraverserRequirement.OBJECT);

    private final VertexProperty.Cardinality cardinality;
    private final String key;
    private final Object value;
    private final Object[] vertexPropertyKeyValues;
    private final boolean asVertex;
    private CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry;

    public AddPropertyStep(final Traversal.Admin traversal, final VertexProperty.Cardinality cardinality, final String key, final Object value, final Object... vertexPropertyKeyValues) {
        super(traversal);
        this.key = key;
        this.value = value;
        this.vertexPropertyKeyValues = vertexPropertyKeyValues;
        this.asVertex = null != cardinality || this.vertexPropertyKeyValues.length > 0;
        this.cardinality = null == cardinality ? VertexProperty.Cardinality.list : cardinality;
    }

    public AddPropertyStep(final Traversal.Admin traversal, final String key, final Object value, final Object... vertexPropertyKeyValues) {
        this(traversal, null, key, value, vertexPropertyKeyValues);
    }

    public VertexProperty.Cardinality getCardinality() {
        return cardinality;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public Object[] getVertexPropertyKeyValues() {
        return vertexPropertyKeyValues;
    }

    public boolean isAsVertex() {
        return asVertex;
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
        if (callbackRegistry != null) {
            final Element currentElement = traverser.get();
            final Property currentProperty = traverser.get().property(key);
            final boolean newProperty = asVertex ? currentProperty == VertexProperty.empty() : currentProperty == Property.empty();

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

        if (asVertex)
            ((Vertex) traverser.get()).property(cardinality, key, value, vertexPropertyKeyValues);
        else
            traverser.get().property(key, value);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return REQUIREMENTS;
    }

    @Override
    public CallbackRegistry<Event.ElementPropertyChangedEvent> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode() ^ this.cardinality.hashCode() ^ this.key.hashCode() ^ Integer.rotateLeft(this.value.hashCode(), 16);
        for (final Object item : this.vertexPropertyKeyValues) {
            result ^= item.hashCode();
        }
        return result;
    }
}
