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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * Generates a synthetic network for a given out- and (optionally) in-degree distribution.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DistributionGenerator extends AbstractGenerator {

    private final Distribution outDistribution;
    private final Distribution inDistribution;
    private final Iterable<Vertex> out;
    private final Iterable<Vertex> in;
    private final int expectedNumEdges;
    private final boolean allowLoops;

    private DistributionGenerator(final Graph g, final String label, final Optional<Consumer<Edge>> edgeProcessor,
                                  final Optional<BiConsumer<Vertex, Map<String, Object>>> vertexProcessor,
                                  final Supplier<Long> seedGenerator, final Iterable<Vertex> out,
                                  final Iterable<Vertex> in, final int expectedNumEdges,
                                  final Distribution outDistribution, final Distribution inDistribution,
                                  final boolean allowLoops) {
        super(g, label, edgeProcessor, vertexProcessor, seedGenerator);
        this.out = out;
        this.in = in;
        this.outDistribution = outDistribution;
        this.inDistribution = inDistribution;
        this.expectedNumEdges = expectedNumEdges;
        this.allowLoops = allowLoops;
    }

    @Override
    public int generate() {
        final long seed = this.seedSupplier.get();
        Random outRandom = new Random(seed);
        final ArrayList<Vertex> outStubs = new ArrayList<>(expectedNumEdges);

        for (Vertex v : out) {
            processVertex(v, Collections.EMPTY_MAP);
            final int degree = this.outDistribution.nextValue(outRandom);
            IntStream.range(0, degree).forEach(i -> outStubs.add(v));
        }

        outRandom = new Random(seed);
        Collections.shuffle(outStubs, outRandom);

        outRandom = new Random(seed);
        final Random inRandom = new Random(this.seedSupplier.get());
        int addedEdges = 0;
        int position = 0;
        for (Vertex v : in) {
            processVertex(v, Collections.EMPTY_MAP);
            final int degree = this.inDistribution.nextConditionalValue(inRandom, this.outDistribution.nextValue(outRandom));
            for (int i = 0; i < degree; i++) {
                Vertex other = null;
                while (null == other) {
                    if (position >= outStubs.size()) return addedEdges; //No more edges to connect
                    other = outStubs.get(position);
                    position++;
                    if (!allowLoops && v.equals(other)) other = null;
                }
                //Connect edge
                addEdge(other, v);
                addedEdges++;
            }
        }
        return addedEdges;
    }

    public static Builder build(final Graph g) {
        return new Builder(g);
    }

    public final static class Builder extends AbstractGeneratorBuilder<Builder> {
        private final Graph g;
        private Distribution outDistribution;
        private Distribution inDistribution;
        private Iterable<Vertex> out;
        private Iterable<Vertex> in;
        private int expectedNumEdges;
        private boolean allowLoops = true;

        private Builder(final Graph g) {
            super(Builder.class);
            this.g = g;
            final List<Vertex> allVertices = IteratorUtils.list(g.vertices());
            this.out = allVertices;
            this.in = allVertices;
            this.expectedNumEdges = allVertices.size() * 2;
        }

        public Builder inVertices(final Iterable<Vertex> vertices) {
            this.in = vertices;
            return this;
        }

        public Builder outVertices(final Iterable<Vertex> vertices) {
            this.out = vertices;
            return this;
        }

        public Builder expectedNumEdges(final int expectedNumEdges) {
            this.expectedNumEdges = expectedNumEdges;
            return this;
        }

        /**
         * Sets whether loops, i.e. edges with the same start and end vertex, are allowed to be generated.
         */
        public void allowLoops(final boolean allowLoops) {
            this.allowLoops = allowLoops;
        }

        /**
         * Sets the distribution to be used to generate the sizes of communities.
         */
        public Builder outDistribution(final Distribution distribution) {
            this.outDistribution = distribution;
            return this;
        }

        /**
         * Sets the distribution to be used to generate the out-degrees of vertices.
         */
        public Builder inDistribution(final Distribution distribution) {
            this.inDistribution = distribution;
            return this;
        }

        public DistributionGenerator create() {
            if (null == outDistribution)
                throw new IllegalStateException("Must set out-distribution before generating edges");
            final Distribution outDist = outDistribution.initialize(SizableIterable.sizeOf(out), expectedNumEdges);
            Distribution inDist;
            if (null == inDistribution) {
                if (out != in) throw new IllegalArgumentException("Need to specify in-distribution");
                inDist = new CopyDistribution();
            } else {
                inDist = inDistribution.initialize(SizableIterable.sizeOf(in), expectedNumEdges);
            }

            return new DistributionGenerator(this.g, this.label, this.edgeProcessor, this.vertexProcessor, this.seedSupplier,
                    this.out, this.in, this.expectedNumEdges, outDist, inDist, allowLoops);
        }
    }
}
