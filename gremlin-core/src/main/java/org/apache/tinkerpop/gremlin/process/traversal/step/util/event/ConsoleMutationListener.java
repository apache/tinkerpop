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

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

/**
 * An example listener that writes a message to the console for each event that fires from the graph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConsoleMutationListener implements MutationListener {

    private final Graph graph;

    public ConsoleMutationListener(final Graph graph) {
        this.graph = graph;
    }

    @Override
    public void vertexAdded(final Vertex vertex) {
        System.out.println("Vertex [" + vertex.toString() + "] added to graph [" + graph.toString() + "]");
    }

    @Override
    public void vertexRemoved(final Vertex vertex) {
        System.out.println("Vertex [" + vertex.toString() + "] removed from graph [" + graph.toString() + "]");
    }

    @Override
    public void vertexPropertyRemoved(final VertexProperty vertexProperty) {
        System.out.println("Vertex Property [" + vertexProperty.toString() + "] removed from graph [" + graph.toString() + "]");
    }

    @Override
    public void edgeAdded(final Edge edge) {
        System.out.println("Edge [" + edge.toString() + "] added to graph [" + graph.toString() + "]");
    }

    @Override
    public void edgeRemoved(final Edge edge) {
        System.out.println("Edge [" + edge.toString() + "] removed from graph [" + graph.toString() + "]");
    }

    @Override
    public void edgePropertyRemoved(final Edge element, final Property removedValue) {
        System.out.println("Edge [" + element.toString() + "] property with value of [" + removedValue + "] removed in graph [" + graph.toString() + "]");
    }

    @Override
    public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
        System.out.println("Edge [" + element.toString() + "] property change value from [" + oldValue + "] to [" + setValue + "] in graph [" + graph.toString() + "]");
    }

    @Override
    public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
        System.out.println("VertexProperty [" + element.toString() + "] property change value from [" + oldValue + "] to [" + setValue + "] in graph [" + graph.toString() + "]");
    }

    @Override
    public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property oldValue) {
        System.out.println("VertexProperty [" + element.toString() + "] property with value of [" + oldValue + "] removed in graph [" + graph.toString() + "]");
    }

    @Override
    public void vertexPropertyChanged(final Vertex element, final Property oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
        // do nothing - deprecated
    }

    @Override
    public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
        System.out.println("Vertex [" + element.toString() + "] property [" + oldValue + "] change to [" + setValue + "] in graph [" + graph.toString() + "]");
    }

    @Override
    public String toString() {
        return MutationListener.class.getSimpleName() + "[" + graph + "]";
    }
}
