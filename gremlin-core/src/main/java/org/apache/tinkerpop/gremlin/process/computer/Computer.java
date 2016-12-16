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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.tinkerpop.gremlin.process.computer.GraphComputer.EDGES;
import static org.apache.tinkerpop.gremlin.process.computer.GraphComputer.GRAPH_COMPUTER;
import static org.apache.tinkerpop.gremlin.process.computer.GraphComputer.PERSIST;
import static org.apache.tinkerpop.gremlin.process.computer.GraphComputer.RESULT;
import static org.apache.tinkerpop.gremlin.process.computer.GraphComputer.VERTICES;
import static org.apache.tinkerpop.gremlin.process.computer.GraphComputer.WORKERS;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.3.0, replaced by use of {@link GraphComputer#open(Configuration)}.
 */
@Deprecated
public final class Computer implements Function<Graph, GraphComputer>, Serializable, Cloneable {

    private Class<? extends GraphComputer> graphComputerClass = GraphComputer.class;
    private Map<String, Object> configuration = new HashMap<>();
    private int workers = -1;
    private GraphComputer.Persist persist = null;
    private GraphComputer.ResultGraph resultGraph = null;
    private Traversal<Vertex, Vertex> vertices = null;
    private Traversal<Vertex, Edge> edges = null;

    private Computer() {

    }

    private Computer(final Class<? extends GraphComputer> graphComputerClass) {
        this.graphComputerClass = graphComputerClass;
    }

    public static Computer compute() {
        return new Computer(GraphComputer.class);
    }

    public static Computer compute(final Class<? extends GraphComputer> graphComputerClass) {
        return new Computer(graphComputerClass);
    }

    public Computer graphComputer(final Class<? extends GraphComputer> graphComputerClass) {
        final Computer clone = this.clone();
        clone.graphComputerClass = graphComputerClass;
        return clone;
    }

    public Computer configure(final String key, final Object value) {
        final Computer clone = this.clone();
        clone.configuration.put(key, value);
        return clone;
    }

    public Computer configure(final Map<String, Object> configurations) {
        final Computer clone = this.clone();
        clone.configuration.putAll(configurations);
        return clone;
    }

    public Computer workers(final int workers) {
        final Computer clone = this.clone();
        clone.workers = workers;
        return clone;
    }

    public Computer persist(final GraphComputer.Persist persist) {
        final Computer clone = this.clone();
        clone.persist = persist;
        return clone;
    }


    public Computer result(final GraphComputer.ResultGraph resultGraph) {
        final Computer clone = this.clone();
        clone.resultGraph = resultGraph;
        return clone;
    }

    public Computer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
        final Computer clone = this.clone();
        clone.vertices = vertexFilter;
        return clone;
    }

    public Computer edges(final Traversal<Vertex, Edge> edgeFilter) {
        final Computer clone = this.clone();
        clone.edges = edgeFilter;
        return clone;
    }

    public GraphComputer apply(final Graph graph) {
        GraphComputer computer = this.graphComputerClass.equals(GraphComputer.class) ? graph.compute() : graph.compute(this.graphComputerClass);
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
            computer = computer.edges(this.edges);
        return computer;
    }

    @Override
    public String toString() {
        return this.graphComputerClass.getSimpleName().toLowerCase();
    }

    @Override
    public Computer clone() {
        try {
            final Computer clone = (Computer) super.clone();
            clone.configuration = new HashMap<>(this.configuration);
            if (null != this.vertices)
                clone.vertices = this.vertices.asAdmin().clone();
            if (null != this.edges)
                clone.edges = this.edges.asAdmin().clone();
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage());
        }
    }

    public Configuration configuration() {
        final Map<String, Object> map = new HashMap<>();
        map.put(GRAPH_COMPUTER, this.graphComputerClass.getCanonicalName());
        if (-1 != this.workers)
            map.put(WORKERS, this.workers);
        if (null != this.persist)
            map.put(PERSIST, this.persist.name());
        if (null != this.resultGraph)
            map.put(RESULT, this.resultGraph.name());
        if (null != this.vertices)
            map.put(VERTICES, this.vertices);
        if (null != this.edges)
            map.put(EDGES, this.edges);
        map.putAll(this.getConfiguration());
        return new MapConfiguration(map);
    }

    /////////////////
    /////////////////

    public Class<? extends GraphComputer> getGraphComputerClass() {
        return this.graphComputerClass;
    }

    public Map<String, Object> getConfiguration() {
        return this.configuration;
    }

    public Traversal<Vertex, Vertex> getVertices() {
        return this.vertices;
    }

    public Traversal<Vertex, Edge> getEdges() {
        return this.edges;
    }

    public GraphComputer.Persist getPersist() {
        return this.persist;
    }

    public GraphComputer.ResultGraph getResultGraph() {
        return this.resultGraph;
    }

    public int getWorkers() {
        return this.workers;
    }

}
