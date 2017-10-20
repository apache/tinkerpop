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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

/**
 * A representation of some action that occurs on a {@link Graph} for a {@link Traversal}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Event {

    /**
     * An {@code Event} publishes its action to all the event {@link MutationListener} objects.
     */
    void fireEvent(final Iterator<MutationListener> eventListeners);

    /**
     * Represents an action where an {@link Edge} is added to the {@link Graph}.
     */
    class EdgeAddedEvent implements Event {

        private final Edge edge;

        public EdgeAddedEvent(final Edge edge) {
            this.edge = edge;
        }

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            while (eventListeners.hasNext()) {
                eventListeners.next().edgeAdded(edge);
            }
        }
    }

    /**
     * Represents an action where an {@link Edge} {@link Property} is added/modified.  If the {@link Property} is
     * new then the {@code oldValue} will be {@code null}.
     */
    class EdgePropertyChangedEvent extends ElementPropertyChangedEvent {

        public EdgePropertyChangedEvent(final Edge edge, final Property oldValue, final Object newValue) {
            super(edge, oldValue, newValue);
        }

        @Override
        void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            listener.edgePropertyChanged((Edge) element, oldValue, newValue);
        }
    }

    /**
     * Represents an action where an {@link Edge} {@link Property} is removed.
     */
    class EdgePropertyRemovedEvent extends ElementPropertyEvent {

        public EdgePropertyRemovedEvent(final Edge element, final Property removed) {
            super(element, removed, null);
        }

        @Override
        void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            listener.edgePropertyRemoved((Edge) element, oldValue);
        }
    }

    /**
     * Represents an action where an {@link Edge} is removed from the {@link Graph}.
     */
    class EdgeRemovedEvent implements Event {

        private final Edge edge;

        public EdgeRemovedEvent(final Edge edge) {
            this.edge = edge;
        }

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            while (eventListeners.hasNext()) {
                eventListeners.next().edgeRemoved(edge);
            }
        }
    }

    /**
     * Represents an action where a {@link Vertex} is removed from the {@link Graph}.
     */
    class VertexAddedEvent implements Event {

        private final Vertex vertex;

        public VertexAddedEvent(final Vertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            while (eventListeners.hasNext()) {
                eventListeners.next().vertexAdded(vertex);
            }
        }
    }

    /**
     * Represents an action where a {@link VertexProperty} is modified on a {@link Vertex}.
     */
    class VertexPropertyChangedEvent extends ElementPropertyChangedEvent {

        public VertexPropertyChangedEvent(final Vertex element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            super(element, oldValue, newValue, vertexPropertyKeyValues);
        }

        @Override
        void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            listener.vertexPropertyChanged((Vertex) element, (VertexProperty) oldValue, newValue, vertexPropertyKeyValues);
        }
    }

    /**
     * Represents an action where a {@link Property} is modified on a {@link VertexProperty}.
     */
    class VertexPropertyPropertyChangedEvent extends ElementPropertyChangedEvent {

        public VertexPropertyPropertyChangedEvent(final VertexProperty element, final Property oldValue, final Object newValue) {
            super(element, oldValue, newValue);
        }

        @Override
        void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            listener.vertexPropertyPropertyChanged((VertexProperty) element, oldValue, newValue);
        }
    }

    /**
     * Represents an action where a {@link Property} is removed from a {@link VertexProperty}.
     */
    class VertexPropertyPropertyRemovedEvent extends ElementPropertyEvent {

        public VertexPropertyPropertyRemovedEvent(final VertexProperty element, final Property removed) {
            super(element, removed, null);
        }

        @Override
        void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            listener.vertexPropertyPropertyRemoved((VertexProperty) element, oldValue);
        }
    }

    /**
     * Represents an action where a {@link Property} is removed from a {@link Vertex}.
     */
    class VertexPropertyRemovedEvent implements Event {

        private final VertexProperty vertexProperty;

        public VertexPropertyRemovedEvent(final VertexProperty vertexProperty) {
            this.vertexProperty = vertexProperty;
        }

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            while (eventListeners.hasNext()) {
                eventListeners.next().vertexPropertyRemoved(vertexProperty);
            }
        }
    }

    /**
     * Represents an action where a {@link Vertex} is removed from the {@link Graph}.
     */
    class VertexRemovedEvent implements Event {

        private final Vertex vertex;

        public VertexRemovedEvent(final Vertex vertex) {
            this.vertex = vertex;
        }

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            while (eventListeners.hasNext()) {
                eventListeners.next().vertexRemoved(vertex);
            }
        }
    }

    /**
     * A base class for {@link Property} mutation events.
     */
    abstract class ElementPropertyChangedEvent extends ElementPropertyEvent {

        public ElementPropertyChangedEvent(final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            super(element, oldValue, newValue, vertexPropertyKeyValues);
        }
    }

    /**
     * A base class for {@link Property} mutation events.
     */
    abstract class ElementPropertyEvent implements Event {

        private final Element element;
        private final Property oldValue;
        private final Object newValue;
        private final Object[] vertexPropertyKeyValues;

        public ElementPropertyEvent(final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues) {
            this.element = element;
            this.oldValue = oldValue;
            this.newValue = newValue;
            this.vertexPropertyKeyValues = vertexPropertyKeyValues;
        }

        abstract void fire(final MutationListener listener, final Element element, final Property oldValue, final Object newValue, final Object... vertexPropertyKeyValues);

        @Override
        public void fireEvent(final Iterator<MutationListener> eventListeners) {
            while (eventListeners.hasNext()) {
                fire(eventListeners.next(), element, oldValue, newValue, vertexPropertyKeyValues);
            }
        }
    }
}
