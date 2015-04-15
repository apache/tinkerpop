/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  * http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.tinkerpop.gremlin.process.computer.util;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.Future;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class ShellGraph implements Graph, Serializable {

    private final Class<? extends Graph> graphClass;
    public static final String SHELL_GRAPH_CLASS = "shell.graphClass";

    private ShellGraph(final Class<? extends Graph> graphClass) {
        this.graphClass = graphClass;
    }

    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw Exceptions.vertexAdditionsNotSupported();
    }

    @Override
    public <C extends GraphComputer> C compute(final Class<C> graphComputerClass) throws IllegalArgumentException {
        return (C) ShellGraphComputer.instance();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return ShellGraphComputer.instance();
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
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    @Override
    public Variables variables() {
        throw Exceptions.variablesNotSupported();
    }

    @Override
    public Configuration configuration() {
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty(Graph.GRAPH, ShellGraph.class.getName());
        configuration.setProperty(SHELL_GRAPH_CLASS, this.graphClass);
        return configuration;
    }

    @Override
    public void close() throws Exception {

    }

    public Class<? extends Graph> getGraphClass() {
        return this.graphClass;
    }

    public static ShellGraph open(final Configuration configuration) {
        return new ShellGraph((Class<? extends Graph>) configuration.getProperty(SHELL_GRAPH_CLASS));
    }

    public static ShellGraph of(final Class<? extends Graph> graphClass) {
        return new ShellGraph(graphClass);
    }

    private static class ShellGraphComputer implements GraphComputer {

        private static final ShellGraphComputer INSTANCE = new ShellGraphComputer();

        @Override
        public GraphComputer isolation(final Isolation isolation) {
            return this;
        }

        @Override
        public GraphComputer result(final ResultGraph resultGraph) {
            return this;
        }

        @Override
        public GraphComputer persist(final Persist persist) {
            return this;
        }

        @Override
        public GraphComputer program(final VertexProgram vertexProgram) {
            return this;
        }

        @Override
        public GraphComputer mapReduce(final MapReduce mapReduce) {
            return this;
        }

        @Override
        public Future<ComputerResult> submit() {
            throw new UnsupportedOperationException(ShellGraphComputer.class.getCanonicalName() + " can not be executed as it is simply a placeholder");
        }

        public static ShellGraphComputer instance() {
            return INSTANCE;
        }
    }

    public class Features implements Graph.Features {

        @Override
        public GraphFeatures graph() {
            return new GraphFeatures() {
                @Override
                public boolean supportsComputer() {
                    return true;
                }

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
                    return new VariableFeatures() {
                        @Override
                        public boolean supportsVariables() {
                            return false;
                        }
                    };
                }
            };
        }

        @Override
        public VertexFeatures vertex() {
            return new VertexFeatures() {
                @Override
                public boolean supportsAddVertices() {
                    return false;
                }

                @Override
                public boolean supportsRemoveVertices() {
                    return false;
                }

                @Override
                public boolean supportsMultiProperties() {
                    return false;
                }

                @Override
                public boolean supportsMetaProperties() {
                    return false;
                }

                @Override
                public VertexPropertyFeatures properties() {
                    return new VertexPropertyFeatures() {
                        @Override
                        public boolean supportsAddProperty() {
                            return false;
                        }

                        @Override
                        public boolean supportsRemoveProperty() {
                            return false;
                        }

                        @Override
                        public boolean supportsUserSuppliedIds() {
                            return false;
                        }

                        @Override
                        public boolean supportsNumericIds() {
                            return false;
                        }

                        @Override
                        public boolean supportsStringIds() {
                            return false;
                        }

                        @Override
                        public boolean supportsUuidIds() {
                            return false;
                        }

                        @Override
                        public boolean supportsCustomIds() {
                            return false;
                        }

                        @Override
                        public boolean supportsAnyIds() {
                            return false;
                        }
                    };
                }
            };
        }

        @Override
        public EdgeFeatures edge() {
            return new EdgeFeatures() {
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
                    return new EdgePropertyFeatures() {
                        @Override
                        public boolean supportsProperties() {
                            return false;
                        }
                    };
                }
            };
        }
    }
}
