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
package org.apache.tinkerpop.gremlin.structure.io.gryo;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
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
 * Constructs Gryo IO implementations given a {@link Graph} and {@link IoRegistry}. Implementers of the {@link Graph}
 * interfaces should see the {@link GryoMapper} for information on the expectations for the {@link IoRegistry}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.4.0, replaced by {@link GraphTraversalSource#io(String)}.
 */
@Deprecated
public final class GryoIo implements Io<GryoReader.Builder, GryoWriter.Builder, GryoMapper.Builder> {

    private final Graph graph;
    private Optional<Consumer<Mapper.Builder>> onMapper;
    private final GryoVersion version;

    private GryoIo(final Builder builder) {
        this.graph = builder.graph;
        this.onMapper = Optional.ofNullable(builder.onMapper);
        this.version = builder.version;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GryoReader.Builder reader() {
        return GryoReader.build().mapper(mapper().create());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GryoWriter.Builder writer() {
        return GryoWriter.build().mapper(mapper().create());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GryoMapper.Builder mapper() {
        final GryoMapper.Builder builder = GryoMapper.build().version(version);
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

    /**
     * Create a new builder using the default version of Gryo - v3.
     */
    public static Io.Builder<GryoIo> build() {
        return build(GryoVersion.V3_0);
    }

    /**
     * Create a new builder using the specified version of Gryo.
     */
    public static Io.Builder<GryoIo> build(final GryoVersion version) {
        return new Builder(version);
    }

    public final static class Builder implements Io.Builder<GryoIo> {
        private Graph graph;
        private Consumer<Mapper.Builder> onMapper = null;
        private final GryoVersion version;

        Builder(final GryoVersion version) {
            this.version = version;
        }

        @Override
        public Io.Builder<? extends Io> onMapper(final Consumer<Mapper.Builder> onMapper) {
            this.onMapper = onMapper;
            return this;
        }

        @Override
        public Io.Builder<GryoIo> graph(final Graph g) {
            this.graph = g;
            return this;
        }

        @Override
        public <V> boolean requiresVersion(final V version) {
            return this.version == version;
        }

        @Override
        public GryoIo create() {
            if (null == graph) throw new IllegalArgumentException("The graph argument was not specified");
            return new GryoIo(this);
        }
    }
}
