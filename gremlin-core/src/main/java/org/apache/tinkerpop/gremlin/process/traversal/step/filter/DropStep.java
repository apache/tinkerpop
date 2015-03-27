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
package org.apache.tinkerpop.gremlin.process.traversal.step.filter;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.EventCallback;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class DropStep<S> extends FilterStep<S> implements Mutating<EventCallback<Event>> {

    private List<EventCallback<Event>> callbacks = null;

    public DropStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected boolean filter(Traverser.Admin<S> traverser) {
        final S s = traverser.get();
        if (s instanceof Element) {
            final Element toRemove = ((Element) s);
            if (callbacks != null) {
                final Event removeEvent;
                if (s instanceof Vertex)
                    removeEvent = new Event.VertexRemovedEvent(DetachedFactory.detach((Vertex) s, true));
                else if (s instanceof Edge)
                    removeEvent = new Event.EdgeRemovedEvent(DetachedFactory.detach((Edge) s, true));
                else if (s instanceof VertexProperty)
                    removeEvent = new Event.VertexPropertyRemovedEvent(DetachedFactory.detach((VertexProperty) s, true));
                else
                    throw new IllegalStateException("The incoming object is not removable: " + s);

                callbacks.forEach(c -> c.accept(removeEvent));
            }

            toRemove.remove();
        } else if (s instanceof Property) {
            final Property toRemove = ((Property) s);
            if (callbacks != null) {
                final Event.ElementPropertyEvent removeEvent;
                if (toRemove.element() instanceof Edge)
                    removeEvent = new Event.EdgePropertyRemovedEvent((Edge) toRemove.element(), DetachedFactory.detach(toRemove));
                else if (toRemove.element() instanceof VertexProperty)
                    removeEvent = new Event.VertexPropertyPropertyRemovedEvent((VertexProperty) toRemove.element(), DetachedFactory.detach(toRemove));
                else
                    throw new IllegalStateException("The incoming object is not removable: " + s);

                callbacks.forEach(c -> c.accept(removeEvent));
            }
            toRemove.remove();
        } else
            throw new IllegalStateException("The incoming object is not removable: " + s);
        return false;
    }

    @Override
    public void addCallback(final EventCallback<Event> elementPropertyRemovedEventEventCallback) {
        if (callbacks == null) callbacks = new ArrayList<>();
        callbacks.add(elementPropertyRemovedEventEventCallback);
    }

    @Override
    public void removeCallback(final EventCallback<Event> elementPropertyRemovedEventEventCallback) {
        if (callbacks != null) callbacks.remove(elementPropertyRemovedEventEventCallback);
    }

    @Override
    public void clearCallbacks() {
        if (callbacks != null) callbacks.clear();
    }

    @Override
    public List<EventCallback<Event>> getCallbacks() {
        return (callbacks != null) ? Collections.unmodifiableList(callbacks) : Collections.emptyList();
    }
}
