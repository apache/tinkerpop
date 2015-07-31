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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Constructs Gryo IO implementations given a {@link Graph} and {@link IoRegistry}. Implementers of the {@link Graph}
 * interfaces should see the {@link GryoMapper} for information on the expectations for the {@link IoRegistry}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class GryoIo implements Io<GryoReader.Builder, GryoWriter.Builder, GryoMapper.Builder> {

    private final IoRegistry registry;
    private final Graph graph;

    private GryoIo(final IoRegistry registry, final Graph graph) {
        this.registry = registry;
        this.graph = graph;
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
        return (null == this.registry) ? GryoMapper.build() : GryoMapper.build().addRegistry(this.registry);
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

    public static Io.Builder<GryoIo> build() {
        return new Builder();
    }

    public final static class Builder implements Io.Builder<GryoIo> {

        private IoRegistry registry = null;
        private Graph graph;

        @Override
        public Io.Builder<GryoIo> registry(final IoRegistry registry) {
            this.registry = registry;
            return this;
        }

        @Override
        public Io.Builder<GryoIo> graph(final Graph g) {
            this.graph = g;
            return this;
        }

        @Override
        public GryoIo create() {
            if (null == graph) throw new IllegalArgumentException("The graph argument was not specified");
            return new GryoIo(registry, graph);
        }
    }
}
