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
package org.apache.tinkerpop.gremlin.structure.util.empty;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Collections;
import java.util.Iterator;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@GraphFactoryClass(EmptyGraph.EmptyGraphFactory.class)
public final class EmptyGraph implements Graph {

    private static final String MESSAGE = "The graph is immutable and empty";
    private static final EmptyGraph INSTANCE = new EmptyGraph();
    private final EmptyGraphFeatures features = new EmptyGraphFeatures();

    private EmptyGraph() {
    }

    public static Graph instance() {
        return INSTANCE;
    }

    @Override
    public Features features() {
        return features;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public GraphComputer compute() {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public void close() throws Exception {
        throw new IllegalStateException(MESSAGE);
    }

    @Override
    public Iterator<Vertex> vertices(final Object... vertexIds) {
        return Collections.emptyIterator();
    }

    @Override
    public Iterator<Edge> edges(final Object... edgeIds) {
        return Collections.emptyIterator();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, "empty");
    }

    /**
     * Features defined such that they support immutability but allow all other possibilities.
     */
    public static final class EmptyGraphFeatures implements Graph.Features {

        private GraphFeatures graphFeatures = new EmptyGraphGraphFeatures();
        private VertexFeatures vertexFeatures = new EmptyGraphVertexFeatures();
        private EdgeFeatures edgeFeatures = new EmptyGraphEdgeFeatures();
        private EdgePropertyFeatures edgePropertyFeatures = new EmptyGraphEdgePropertyFeatures();
        private VertexPropertyFeatures vertexPropertyFeatures = new EmptyGraphVertexPropertyFeatures();

        private EmptyGraphFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return graphFeatures;
        }

        @Override
        public VertexFeatures vertex() {
            return vertexFeatures;
        }

        @Override
        public EdgeFeatures edge() {
            return edgeFeatures;
        }

        /**
         * Graph features defined such that they support immutability but allow all other possibilities.
         */
        public final class EmptyGraphGraphFeatures implements GraphFeatures {
            @Override
            public boolean supportsPersistence() {
                return false;
            }

            @Override
            public boolean supportsTransactions() {
                return false;
            }

            @Override
            public boolean supportsThreadedTransactions() {
                return false;
            }

            @Override
            public VariableFeatures variables() {
                return null;
            }

            @Override
            public boolean supportsComputer() {
                return false;
            }
        }

        /**
         * Vertex features defined such that they support immutability but allow all other possibilities.
         */
        public final class EmptyGraphVertexFeatures extends EmptyGraphElementFeatures implements VertexFeatures {
            @Override
            public VertexProperty.Cardinality getCardinality(final String key) {
                // probably not much hurt here in returning list...it's an "empty graph"
                return VertexProperty.Cardinality.list;
            }

            @Override
            public boolean supportsAddVertices() {
                return false;
            }

            @Override
            public boolean supportsRemoveVertices() {
                return false;
            }

            @Override
            public VertexPropertyFeatures properties() {
                return vertexPropertyFeatures;
            }
        }

        /**
         * Edge features defined such that they support immutability but allow all other possibilities.
         */
        public final class EmptyGraphEdgeFeatures extends EmptyGraphElementFeatures implements EdgeFeatures {
            @Override
            public boolean supportsAddEdges() {
                return false;
            }

            @Override
            public boolean supportsRemoveEdges() {
                return false;
            }

            @Override
            public EdgePropertyFeatures properties() {
                return edgePropertyFeatures;
            }
        }

        /**
         * Vertex Property features defined such that they support immutability but allow all other possibilities.
         */
        public final class EmptyGraphVertexPropertyFeatures implements VertexPropertyFeatures {
            @Override
            public boolean supportsRemoveProperty() {
                return false;
            }
        }

        /**
         * Edge property features defined such that they support immutability but allow all other possibilities.
         */
        public final class EmptyGraphEdgePropertyFeatures implements EdgePropertyFeatures {}

        /**
         * Vertex features defined such that they support immutability but allow all other possibilities.
         */
        public abstract class EmptyGraphElementFeatures implements ElementFeatures {
            @Override
            public boolean supportsAddProperty() {
                return false;
            }

            @Override
            public boolean supportsRemoveProperty() {
                return false;
            }
        }
    }

    /**
     * {@link EmptyGraph} doesn't have a standard {@code open()} method because it is a singleton. Use this factory
     * to provide as a {@link GraphFactoryClass} for {@link EmptyGraph} so that {@link GraphFactory} can instantiate
     * it in a generalized way. End users should not generally use this method of instantiation.
     */
    public static final class EmptyGraphFactory {
        public static Graph open(final Configuration conf) {
            return EmptyGraph.instance();
        }
    }
}