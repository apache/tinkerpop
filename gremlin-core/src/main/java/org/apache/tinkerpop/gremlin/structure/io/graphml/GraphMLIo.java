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
package org.apache.tinkerpop.gremlin.structure.io.graphml;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Constructs GraphML IO implementations given a {@link Graph} and {@link IoRegistry}. Implementers of the {@link Graph}
 * interfaces do not have to register any special serializers to the {@link IoRegistry} as GraphML does not support
 * such things.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GraphMLIo implements Io<GraphMLReader.Builder, GraphMLWriter.Builder, GraphMLMapper.Builder> {
    private final Graph graph;
    private Optional<Consumer<Mapper.Builder>> onMapper;

    private GraphMLIo(final Builder builder) {
        this.graph = builder.graph;
        this.onMapper = Optional.ofNullable(builder.onMapper);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphMLReader.Builder reader() {
        return GraphMLReader.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphMLWriter.Builder writer() {
        return GraphMLWriter.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphMLMapper.Builder mapper() {
        final GraphMLMapper.Builder builder = GraphMLMapper.build();
        onMapper.ifPresent(c -> c.accept(builder));
        return builder;
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

    public static Io.Builder<GraphMLIo> build() {
        return new Builder();
    }

    public final static class Builder implements Io.Builder<GraphMLIo> {

        private Graph graph;
        private Consumer<Mapper.Builder> onMapper = null;

        /**
         * @deprecated As of release 3.2.2, replaced by {@link #onMapper(Consumer)}.
         */
        @Deprecated
        @Override
        public Io.Builder<GraphMLIo> registry(final IoRegistry registry) {
            // GraphML doesn't make use of a registry but the contract should simply exist
            return this;
        }

        @Override
        public Io.Builder<? extends Io> onMapper(final Consumer<Mapper.Builder> onMapper) {
            this.onMapper = onMapper;
            return this;
        }

        @Override
        public Io.Builder<GraphMLIo> graph(final Graph g) {
            this.graph = g;
            return this;
        }

        @Override
        public GraphMLIo create() {
            if (null == graph) throw new IllegalArgumentException("The graph argument was not specified");
            return new GraphMLIo(this);
        }
    }
}
