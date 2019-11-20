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

import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * Interface for a listener to {@link EventStrategy} change events. Implementations of this interface should be added
 * to the list of listeners on the addListener method on the {@link EventStrategy}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface MutationListener {

    /**
     * Raised when a new {@link Vertex} is added.
     *
     * @param vertex the {@link Vertex} that was added
     */
    public void vertexAdded(final Vertex vertex);

    /**
     * Raised after a {@link Vertex} was removed from the graph.
     *
     * @param vertex the {@link Vertex} that was removed
     */
    public void vertexRemoved(final Vertex vertex);

    /**
     * Raised after the property of a {@link Vertex} changed.
     *
     * @param element  the {@link Vertex} that changed
     * @param setValue the new value of the property
     */
    public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues);

    /**
     * Raised after a {@link VertexProperty} was removed from the graph.
     *
     * @param vertexProperty the {@link VertexProperty} that was removed
     */
    public void vertexPropertyRemoved(final VertexProperty vertexProperty);

    /**
     * Raised after a new {@link Edge} is added.
     *
     * @param edge the {@link Edge} that was added
     */
    public void edgeAdded(final Edge edge);

    /**
     * Raised after an {@link Edge} was removed from the graph.
     *
     * @param edge  the {@link Edge} that was removed.
     */
    public void edgeRemoved(final Edge edge);

    /**
     * Raised after the property of a {@link Edge} changed.
     *
     * @param element  the {@link Edge} that changed
     * @param setValue the new value of the property
     */
    public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue);

    /**
     * Raised after an {@link Property} property was removed from an {@link Edge}.
     *
     * @param property  the {@link Property} that was removed
     */
    public void edgePropertyRemoved(final Edge element, final Property property);

    /**
     * Raised after the property of a {@link VertexProperty} changed.
     *
     * @param element  the {@link VertexProperty} that changed
     * @param setValue the new value of the property
     */
    public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue);

    /**
     * Raised after an {@link Property} property was removed from a {@link VertexProperty}.
     *
     * @param property  the {@link Property} that removed
     */
    public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property property);
}

