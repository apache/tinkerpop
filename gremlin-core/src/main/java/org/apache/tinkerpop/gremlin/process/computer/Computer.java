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

package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Computer implements Function<Graph, GraphComputer>, Serializable {

    private Class<? extends GraphComputer> graphComputerClass = null;
    private Map<String, Object> configuration = new HashMap<>();
    private int workers = -1;
    private GraphComputer.Persist persist = null;
    private GraphComputer.ResultGraph resultGraph = null;
    private Traversal<Vertex, Vertex> vertices = null;
    private Traversal<Vertex, Edge> edges = null;

    private Computer(final Class<? extends GraphComputer> graphComputerClass) {
        this.graphComputerClass = graphComputerClass;
    }

    public static Computer compute() {
        return new Computer(null);
    }

    public static Computer compute(final Class<? extends GraphComputer> graphComputerClass) {
        return new Computer(graphComputerClass);
    }

    public Computer configure(final String key, final Object value) {
        this.configuration.put(key, value);
        return this;
    }

    public Computer workers(final int workers) {
        this.workers = workers;
        return this;
    }

    public Computer persist(final GraphComputer.Persist persist) {
        this.persist = persist;
        return this;
    }


    public Computer result(final GraphComputer.ResultGraph resultGraph) {
        this.resultGraph = resultGraph;
        return this;
    }

    public Computer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        this.vertices = vertexFilter;
        return this;
    }

    public Computer edges(final Traversal<Vertex, Edge> edgeFilter) {
        this.edges = edgeFilter;
        return this;
    }

    @Override
    public GraphComputer apply(final Graph graph) {
        GraphComputer computer = null == this.graphComputerClass ? graph.compute() : graph.compute(this.graphComputerClass);
        for (final Map.Entry<String, Object> entry : this.configuration.entrySet()) {
            computer = computer.configure(entry.getKey(), entry.getValue());
        }
        if (-1 != this.workers)
            computer = computer.workers(this.workers);
        if (null != this.persist)
            computer = computer.persist(this.persist);
        if (null != this.resultGraph)
            computer = computer.result(this.resultGraph);
        if (null != this.vertices)
            computer = computer.vertices(this.vertices);
        if (null != this.edges)
            computer.edges(this.edges);
        return computer;
    }

    @Override
    public String toString() {
        return null == this.graphComputerClass ? GraphComputer.class.getSimpleName().toLowerCase() : this.graphComputerClass.getSimpleName().toLowerCase();
    }
}
