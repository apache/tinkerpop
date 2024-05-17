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

public class EventUtil {

    public static void registerVertexPropertyChange(CallbackRegistry<Event> callbackRegistry,
                                                    Traversal.Admin<Object, Object> traversal,
                                                    Vertex vertex,
                                                    String key,
                                                    Object value){
        if (hasAnyCallbacks(callbackRegistry)) {
            final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
            final Property<?> p = vertex.property(key);
            final Property<Object> oldValue = p.isPresent() ? eventStrategy.detach(vertex.property(key)) : null;
            final Event.VertexPropertyChangedEvent vpce = new Event.VertexPropertyChangedEvent(eventStrategy.detach(vertex), oldValue, value);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
        }
    }

    public static void registerEdgePropertyChange(CallbackRegistry<Event> callbackRegistry,
                                                  Traversal.Admin<Object, Object> traversal,
                                                  Edge edge,
                                                  String key,
                                                  Object value){
        if (hasAnyCallbacks(callbackRegistry)) {
            final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
            final Property<?> p = edge.property(key);
            final Property<Object> oldValue =
                    p.isPresent() ? eventStrategy.detach(edge.property(key)) : null;
            final Event.EdgePropertyChangedEvent vpce = new Event.EdgePropertyChangedEvent(eventStrategy.detach(edge), oldValue, value);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vpce));
        }
    }

    public static void registerEdgeCreation(CallbackRegistry<Event.EdgeAddedEvent> callbackRegistry,
                                            Traversal.Admin<Object, Object> traversal,
                                            Edge addedEdge){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.EdgeAddedEvent vae = createEdgeAddedEvent(traversal, addedEdge);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    public static void registerEdgeCreationWithGenericEventRegistry(CallbackRegistry<Event> callbackRegistry,
                                                                    Traversal.Admin<Object, Object> traversal,
                                                                    Edge addedEdge){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.EdgeAddedEvent vae = createEdgeAddedEvent(traversal, addedEdge);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    private static Event.EdgeAddedEvent createEdgeAddedEvent(Traversal.Admin<Object, Object> traversal, Edge addedEdge){
        final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
        return new Event.EdgeAddedEvent(eventStrategy.detach(addedEdge));
    }

    public static void registerVertexCreation(CallbackRegistry<Event.VertexAddedEvent> callbackRegistry,
                                              Traversal.Admin<Object, Object> traversal,
                                              Vertex addedVertex){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.VertexAddedEvent vae = createVertexAddedEvent(traversal, addedVertex);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    public static void registerVertexCreationWithGenericEventRegistry(CallbackRegistry<Event> callbackRegistry,
                                                                      Traversal.Admin<Object, Object> traversal,
                                                                      Vertex addedVertex){
        if (hasAnyCallbacks(callbackRegistry)) {
            final Event.VertexAddedEvent vae = createVertexAddedEvent(traversal, addedVertex);
            callbackRegistry.getCallbacks().forEach(c -> c.accept(vae));
        }
    }

    private static Event.VertexAddedEvent createVertexAddedEvent(Traversal.Admin<Object, Object> traversal, Vertex addedVertex){
        final EventStrategy eventStrategy = forceGetEventStrategy(traversal);
        return new Event.VertexAddedEvent(eventStrategy.detach(addedVertex));
    }

    public static void registerElementRemoval(CallbackRegistry<Event> callbackRegistry,
                                              Traversal.Admin<Object, Object> traversal,
                                              Element elementForRemoval){
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

    public static void registerPropertyRemoval(CallbackRegistry<Event> callbackRegistry,
                                               Traversal.Admin<Object, Object> traversal,
                                               Property elementForRemoval){
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

    public static void registerPropertyChange(CallbackRegistry<Event.ElementPropertyChangedEvent> callbackRegistry,
                                              EventStrategy es,
                                              Element affectedElement,
                                              Property removedProperty,
                                              Object value,
                                              Object[] vertexPropertyKeyValues){
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

    public static boolean hasAnyCallbacks(CallbackRegistry<? extends Event> callbackRegistry){
        return callbackRegistry != null && !callbackRegistry.getCallbacks().isEmpty();
    }

    public static EventStrategy forceGetEventStrategy(Traversal.Admin<Object, Object> traversal){
        return traversal.getStrategies().getStrategy(EventStrategy.class).get();
    }
}
