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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Constructs GraphSON IO implementations given a {@link Graph} and {@link IoRegistry}. Implementers of the {@link Graph}
 * interfaces should see the {@link GraphSONMapper} for information on the expectations for the {@link IoRegistry}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphSONIo implements Io<GraphSONReader.Builder, GraphSONWriter.Builder, GraphSONMapper.Builder> {
    private final IoRegistry registry;
    private final Graph graph;

    private GraphSONIo(final IoRegistry registry, final Graph graph) {
        this.registry = registry;
        this.graph = graph;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphSONReader.Builder reader() {
        return GraphSONReader.build().mapper(mapper().create());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphSONWriter.Builder writer() {
        return GraphSONWriter.build().mapper(mapper().create());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphSONMapper.Builder mapper() {
        return (null == this.registry) ? GraphSONMapper.build() : GraphSONMapper.build().addRegistry(registry);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void writeGraph(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            writer().create().writeGraph(out, graph);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readGraph(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            reader().create().readGraph(in, graph);
        }
    }

    public static Io.Builder<GraphSONIo> build() {
        return new Builder();
    }

    public final static class Builder implements Io.Builder<GraphSONIo> {

        private IoRegistry registry = null;
        private Graph graph;

        @Override
        public Io.Builder<GraphSONIo> registry(final IoRegistry registry) {
            this.registry = registry;
            return this;
        }

        @Override
        public Io.Builder<GraphSONIo> graph(final Graph g) {
            this.graph = g;
            return this;
        }

        @Override
        public GraphSONIo create() {
            if (null == graph) throw new IllegalArgumentException("The graph argument was not specified");
            return new GraphSONIo(registry, graph);
        }
    }
}
