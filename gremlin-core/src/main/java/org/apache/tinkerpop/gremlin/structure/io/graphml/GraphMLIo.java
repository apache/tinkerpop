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

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphMLIo implements Io<GraphMLReader.Builder, GraphMLWriter.Builder, GraphMLMapper.Builder> {
    private final Graph graph;

    public GraphMLIo(final Graph graph) {
        this.graph = graph;
    }

    @Override
    public GraphMLReader.Builder reader() {
        return GraphMLReader.build();
    }

    @Override
    public GraphMLWriter.Builder writer() {
        return GraphMLWriter.build();
    }

    @Override
    public GraphMLMapper.Builder mapper() {
        return GraphMLMapper.build();
    }

    @Override
    public void writeGraph(final String file) throws IOException {
        try (final OutputStream out = new FileOutputStream(file)) {
            writer().create().writeGraph(out, graph);
        }
    }

    @Override
    public void readGraph(final String file) throws IOException {
        try (final InputStream in = new FileInputStream(file)) {
            reader().create().readGraph(in, graph);
        }
    }

    public static Io.Builder<GraphMLIo> build() {
        return new Builder();
    }

    public static class Builder implements Io.Builder<GraphMLIo> {

        private Graph graph;

        @Override
        public Io.Builder<GraphMLIo> registry(final IoRegistry registry) {
            // GraphML doesn't make use of a registry but the contract should simply exist
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
            return new GraphMLIo(graph);
        }
    }
}
