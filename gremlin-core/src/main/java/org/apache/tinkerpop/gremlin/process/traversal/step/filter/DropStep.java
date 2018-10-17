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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DropStep<S> extends FilterStep<S> implements Mutating<Event> {

    private CallbackRegistry<Event> callbackRegistry;

    public DropStep(final Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected boolean filter(Traverser.Admin<S> traverser) {
        final S s = traverser.get();
        if (s instanceof Element) {
            final Element toRemove = (Element) s;
            if (callbackRegistry != null) {
                final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                final Event removeEvent;
                if (s instanceof Vertex)
                    removeEvent = new Event.VertexRemovedEvent(eventStrategy.detach((Vertex) s));
                else if (s instanceof Edge)
                    removeEvent = new Event.EdgeRemovedEvent(eventStrategy.detach((Edge) s));
                else if (s instanceof VertexProperty)
                    removeEvent = new Event.VertexPropertyRemovedEvent(eventStrategy.detach((VertexProperty) s));
                else
                    throw new IllegalStateException("The incoming object is not removable: " + s);

                callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
            }

            toRemove.remove();
        } else if (s instanceof Property) {
            final Property toRemove = (Property) s;
            if (callbackRegistry != null) {
                final EventStrategy eventStrategy = getTraversal().getStrategies().getStrategy(EventStrategy.class).get();
                final Event.ElementPropertyEvent removeEvent;
                if (toRemove.element() instanceof Edge)
                    removeEvent = new Event.EdgePropertyRemovedEvent(eventStrategy.detach((Edge) toRemove.element()), eventStrategy.detach(toRemove));
                else if (toRemove.element() instanceof VertexProperty)
                    removeEvent = new Event.VertexPropertyPropertyRemovedEvent(eventStrategy.detach((VertexProperty) toRemove.element()), eventStrategy.detach(toRemove));
                else
                    throw new IllegalStateException("The incoming object is not removable: " + s);

                callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
            }
            toRemove.remove();
        } else
            throw new IllegalStateException("The incoming object is not removable: " + s);
        return false;
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    /**
     * This method doesn't do anything as {@code drop()} doesn't take property mutation arguments.
     */
    @Override
    public void configure(final Object... keyValues) {
        // do nothing
    }

    @Override
    public Parameters getParameters() {
        return Parameters.EMPTY;
    }
}
