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
package org.apache.tinkerpop.gremlin.process.traversal.engine;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
 */
@Deprecated
public final class ComputerTraversalEngine implements TraversalEngine {

    private final transient GraphComputer graphComputer;

    private ComputerTraversalEngine(final GraphComputer graphComputer) {
        this.graphComputer = graphComputer;
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    @Override
    public Type getType() {
        return Type.COMPUTER;
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    @Override
    public String toString() {
        return this.getClass().getSimpleName().toLowerCase();
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    @Override
    public Optional<GraphComputer> getGraphComputer() {
        return Optional.ofNullable(this.graphComputer);
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public static Builder build() {
        return new Builder();
    }

    /**
     * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
     */
    @Deprecated
    public final static class Builder implements TraversalEngine.Builder {

        private Class<? extends GraphComputer> graphComputerClass;
        private int workers = -1;
        private Traversal<Vertex, Vertex> vertexFilter = null;
        private Traversal<Vertex, Edge> edgeFilter = null;

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        @Override
        public List<TraversalStrategy> getWithStrategies() {
            return Collections.emptyList();
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        public Builder workers(final int workers) {
            this.workers = workers;
            return this;
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        public Builder computer(final Class<? extends GraphComputer> graphComputerClass) {
            this.graphComputerClass = graphComputerClass;
            return this;
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        public Builder vertices(final Traversal<Vertex, Vertex> vertexFilter) {
            this.vertexFilter = vertexFilter;
            return this;
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        public Builder edges(final Traversal<Vertex, Edge> edgeFilter) {
            this.edgeFilter = edgeFilter;
            return this;
        }


        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        public ComputerTraversalEngine create(final Graph graph) {
            GraphComputer graphComputer = null == this.graphComputerClass ? graph.compute() : graph.compute(this.graphComputerClass);
            if (-1 != this.workers)
                graphComputer = graphComputer.workers(this.workers);
            if (null != this.vertexFilter)
                graphComputer = graphComputer.vertices(this.vertexFilter);
            if (null != this.edgeFilter)
                graphComputer = graphComputer.edges(this.edgeFilter);
            return new ComputerTraversalEngine(graphComputer);
        }

        /**
         * @deprecated As of release 3.2.0. Please use {@link Graph#traversal(Class)}.
         */
        @Deprecated
        public TraversalSource create(final GraphTraversalSource traversalSource) {
            Computer computer = null == this.graphComputerClass ? Computer.compute() : Computer.compute(this.graphComputerClass);
            if (-1 != this.workers)
                computer = computer.workers(this.workers);
            if (null != this.vertexFilter)
                computer = computer.vertices(this.vertexFilter);
            if (null != this.edgeFilter)
                computer = computer.edges(this.edgeFilter);
            return traversalSource.withComputer(computer);
        }
    }
}