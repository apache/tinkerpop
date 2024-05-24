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
package org.apache.tinkerpop.gremlin.process.traversal.step.util.event;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AddPropertyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Logic for registering events with the {@link EventStrategy} and the callback registry. Extracting this logic allows
 * providers to reuse these utilities more readily.
 */
public final class EventUtil {

    private EventUtil() {}

    /**
     * Register a vertex property change event with the callback registry.
     */
    public static void registerVertexPropertyChange(final CallbackRegistry<Event> callbackRegistry,
                                                    final Traversal.Admin<Object, Object> traversal,
                                                    final Vertex vertex,
                                                    final String key,
                                                    final Object value){
        if (hasAnyCallbacks(callbackRegistry)) {
            final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
            final Property<?> p = vertex.property(key);
            final Property<Object> oldValue = p.isPresent() ? eventStrategy.detach(vertex.property(key)) : null;
            final Event.VertexPropertyChangedEvent vpce = new Event.VertexPropertyChangedEvent(eventStrategy.detach(vertex), oldValue, value);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
        }
    }

    /**
     * Register an edge property change event with the callback registry.
     */
    public static void registerEdgePropertyChange(final CallbackRegistry<Event> callbackRegistry,
                                                  final Traversal.Admin<Object, Object> traversal,
                                                  final Edge edge,
                                                  final String key,
                                                  final Object value){
        if (hasAnyCallbacks(callbackRegistry)) {
            final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
            final Property<?> p = edge.property(key);
            final Property<Object> oldValue =
                    p.isPresent() ? eventStrategy.detach(edge.property(key)) : null;
            final Event.EdgePropertyChangedEvent vpce = new Event.EdgePropertyChangedEvent(eventStrategy.detach(edge), oldValue, value);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerEdgeCreation(final CallbackRegistry<Event.EdgeAddedEvent> callbackRegistry,
                                            final Traversal.Admin<Object, Object> traversal,
                                            final Edge addedEdge){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.EdgeAddedEvent vae = createEdgeAddedEvent(traversal, addedEdge);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerEdgeCreationWithGenericEventRegistry(final CallbackRegistry<Event> callbackRegistry,
                                                                    final Traversal.Admin<Object, Object> traversal,
                                                                    final Edge addedEdge){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.EdgeAddedEvent vae = createEdgeAddedEvent(traversal, addedEdge);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    private static Event.EdgeAddedEvent createEdgeAddedEvent(final Traversal.Admin<Object, Object> traversal,
                                                             final Edge addedEdge){
        final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
        return new Event.EdgeAddedEvent(eventStrategy.detach(addedEdge));
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerVertexCreation(final CallbackRegistry<Event.VertexAddedEvent> callbackRegistry,
                                              final Traversal.Admin<Object, Object> traversal,
                                              final Vertex addedVertex){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.VertexAddedEvent vae = createVertexAddedEvent(traversal, addedVertex);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerVertexCreationWithGenericEventRegistry(final CallbackRegistry<Event> callbackRegistry,
                                                                      final Traversal.Admin<Object, Object> traversal,
                                                                      final Vertex addedVertex){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.VertexAddedEvent vae = createVertexAddedEvent(traversal, addedVertex);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    private static Event.VertexAddedEvent createVertexAddedEvent(final Traversal.Admin<Object, Object> traversal,
                                                                 final Vertex addedVertex){
        final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
        return new Event.VertexAddedEvent(eventStrategy.detach(addedVertex));
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerElementRemoval(final CallbackRegistry<Event> callbackRegistry,
                                              final Traversal.Admin<Object, Object> traversal,
                                              final Element elementForRemoval){
        if (hasAnyCallbacks(callbackRegistry)) {
            final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
            final Event removeEvent;
            if (elementForRemoval instanceof Vertex)
                removeEvent = new Event.VertexRemovedEvent(eventStrategy.detach((Vertex) elementForRemoval));
            else if (elementForRemoval instanceof Edge)
                removeEvent = new Event.EdgeRemovedEvent(eventStrategy.detach((Edge) elementForRemoval));
            else if (elementForRemoval instanceof VertexProperty)
                removeEvent = new Event.VertexPropertyRemovedEvent(eventStrategy.detach((VertexProperty) elementForRemoval));
            else
                throw new IllegalStateException("The incoming object is not removable: " + elementForRemoval);

            callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerPropertyRemoval(final CallbackRegistry<Event> callbackRegistry,
                                               final Traversal.Admin<Object, Object> traversal,
                                               final Property elementForRemoval){
        if (hasAnyCallbacks(callbackRegistry)) {
            final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
            final Event.ElementPropertyEvent removeEvent;
            if (elementForRemoval.element() instanceof Edge)
                removeEvent = new Event.EdgePropertyRemovedEvent(eventStrategy.detach((Edge) elementForRemoval.element()), eventStrategy.detach(elementForRemoval));
            else if (elementForRemoval.element() instanceof VertexProperty)
                removeEvent = new Event.VertexPropertyPropertyRemovedEvent(eventStrategy.detach((VertexProperty) elementForRemoval.element()), eventStrategy.detach(elementForRemoval));
            else
                throw new IllegalStateException("The incoming object is not removable: " + elementForRemoval);

            callbackRegistry.getCallbacks().forEach(c -> c.accept(removeEvent));
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static void registerPropertyChange(final CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry,
                                              final EventStrategy es,
                                              final Element affectedElement,
                                              final Property removedProperty,
                                              final Object value,
                                              final Object[] vertexPropertyKeyValues){
        final Event.ElementPropertyChangedEvent event;
        if (affectedElement instanceof Vertex) {
            event = new Event.VertexPropertyChangedEvent(es.detach((Vertex) affectedElement), removedProperty, value, vertexPropertyKeyValues);
        } else if (affectedElement instanceof Edge) {
            event = new Event.EdgePropertyChangedEvent(es.detach((Edge) affectedElement), removedProperty, value);
        } else if (affectedElement instanceof VertexProperty) {
            event = new Event.VertexPropertyPropertyChangedEvent(es.detach((VertexProperty) affectedElement), removedProperty, value);
        } else {
            throw new IllegalStateException(String.format("The incoming object cannot be processed by change eventing in %s:  %s", AddPropertyStep.class.getName(), affectedElement));
        }
        for (EventCallback<Event.ElementPropertyChangedEvent> c : callbackRegistry.getCallbacks()) {
            c.accept(event);
        }
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static boolean hasAnyCallbacks(final CallbackRegistry<? extends Event> callbackRegistry){
        return callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty();
    }

    /**
     * Register a vertex property addition event with the callback registry.
     */
    public static EventStrategy forceGetEventStrategy(final Traversal.Admin<Object, Object> traversal){
        return traversal.getStrategies().getStrategy(EventStrategy.class).get();
    }
}
