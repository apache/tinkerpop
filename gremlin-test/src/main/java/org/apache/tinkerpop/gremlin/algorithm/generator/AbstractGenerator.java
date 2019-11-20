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
package org.apache.tinkerpop.gremlin.algorithm.generator;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Base class for all synthetic network generators.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractGenerator implements Generator {
    protected final Graph g;
    private final String label;
    private final Optional<Consumer<Edge>> edgeProcessor;
    private final Optional<BiConsumer<Vertex, Map<String, Object>>> vertexProcessor;
    protected final Supplier<Long> seedSupplier;

    /**
     * Constructs a new network generator for edges with the given label and annotator. If a {@code seedGenerator} is
     * not supplied then the system clock is used to generate a seed.
     *
     * @param label           Label for the generated edges
     * @param edgeProcessor   {@code Consumer} to use for annotating newly generated edges.
     * @param vertexProcessor {@code Consumer} to use for annotating process vertices.
     * @param seedGenerator   A {@code Supplier} function to provide seeds to {@link java.util.Random}
     */
    AbstractGenerator(final Graph g, final String label, final Optional<Consumer<Edge>> edgeProcessor,
                      final Optional<BiConsumer<Vertex, Map<String, Object>>> vertexProcessor,
                      final Supplier<Long> seedGenerator) {
        this.g = g;
        this.label = label;
        this.edgeProcessor = edgeProcessor;
        this.vertexProcessor = vertexProcessor;
        this.seedSupplier = seedGenerator;
    }

    protected final Edge addEdge(final Vertex out, final Vertex in) {
        final Edge e = out.addEdge(label, in);
        edgeProcessor.ifPresent(c -> c.accept(e));
        return e;
    }

    protected final Vertex processVertex(final Vertex vertex, final Map<String, Object> context) {
        vertexProcessor.ifPresent(c -> c.accept(vertex, context));
        return vertex;
    }

    public abstract static class AbstractGeneratorBuilder<T extends AbstractGeneratorBuilder> {
        protected String label;
        protected Optional<Consumer<Edge>> edgeProcessor = Optional.empty();
        protected Optional<BiConsumer<Vertex, Map<String, Object>>> vertexProcessor = Optional.empty();
        protected Supplier<Long> seedSupplier = System::currentTimeMillis;
        private final Class<T> extendingClass;

        AbstractGeneratorBuilder(final Class<T> extendingClass) {
            this.extendingClass = extendingClass;
        }

        public T label(final String label) {
            if (null == label || label.isEmpty()) throw new IllegalArgumentException("Label cannot be empty");
            this.label = label;
            return extendingClass.cast(this);
        }

        public T edgeProcessor(final Consumer<Edge> edgeProcessor) {
            this.edgeProcessor = Optional.ofNullable(edgeProcessor);
            return extendingClass.cast(this);
        }

        /**
         * The function supplied here may be called more than once per vertex depending on the implementation.
         */
        public T vertexProcessor(final BiConsumer<Vertex, Map<String, Object>> vertexProcessor) {
            this.vertexProcessor = Optional.ofNullable(vertexProcessor);
            return extendingClass.cast(this);
        }

        public T seedGenerator(final Supplier<Long> seedGenerator) {
            this.seedSupplier = Optional.ofNullable(seedGenerator).orElse(System::currentTimeMillis);
            return extendingClass.cast(this);
        }
    }
}
