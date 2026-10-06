/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Locale;
import java.util.SplittableRandom;

/**
 * Generates deterministic uniform or R-MAT benchmark data in a {@link TinkerGraph}.
 *
 * This is a manually invoked spike utility, not a test. Its output file extension
 * selects the writer used by the {@code io()} step, so use {@code .kryo} for Gryo.
 */
public final class CsrBenchmarkDataGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CsrBenchmarkDataGenerator.class);

    private static final String[] VERTEX_LABELS = {"person", "software", "device", "location"};
    private static final String[] EDGE_LABELS = {"knows", "created", "uses", "locatedIn"};
    private static final String[] LOW_CARDINALITY_STRINGS = new String[64];
    private static final int NUMERIC_TYPE_COUNT = 8;
    private static final int EDGE_MIXED_PROPERTY_STRIDE = 16;
    private static final long DEFAULT_SEED = 0x5eed_c5a5_2026L;

    private static final double RMAT_A = 0.57d;
    private static final double RMAT_AB = RMAT_A + 0.19d;
    private static final double RMAT_ABC = RMAT_AB + 0.19d;

    static {
        for (int i = 0; i < LOW_CARDINALITY_STRINGS.length; i++) {
            LOW_CARDINALITY_STRINGS[i] = "category-" + i;
        }
    }

    private CsrBenchmarkDataGenerator() {
    }

    public static void main(final String[] args) throws Exception {
        if (args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException(
                    "Usage: CsrBenchmarkDataGenerator <uniform|power-law> <scale> <edge-factor> <output> [seed]");
        }

        final Distribution distribution = Distribution.parse(args[0]);
        final int scale = Integer.parseInt(args[1]);
        final int edgeFactor = Integer.parseInt(args[2]);
        final Path output = Paths.get(args[3]).toAbsolutePath();
        final long seed = args.length == 5 ? Long.decode(args[4]) : DEFAULT_SEED;

        validateArguments(scale, edgeFactor, output);

        final int vertexCount = 1 << scale;
        final long edgeCount = Math.multiplyExact((long) vertexCount, edgeFactor);
        LOGGER.info("Generating {} graph with {} vertices, {} edges, edge factor {}, and seed {}",
                distribution.externalName, vertexCount, edgeCount, edgeFactor, seed);

        final long startedAt = System.nanoTime();
        final TinkerGraph graph = TinkerGraph.open();
        try {
            final Vertex[] vertices = addVertices(graph, vertexCount, seed);
            if (distribution == Distribution.UNIFORM) {
                addUniformEdges(vertices, edgeFactor, seed);
            } else {
                addPowerLawEdges(vertices, scale, edgeCount, seed);
            }

            final long generatedAt = System.nanoTime();
            LOGGER.info("Generated graph in {} seconds; writing {}", elapsedSeconds(startedAt, generatedAt), output);
            try (GraphTraversalSource g = graph.traversal()) {
                g.io(output.toString()).write().iterate();
            }
            LOGGER.info("Wrote {} bytes in {} seconds; total elapsed time {} seconds",
                    Files.size(output), elapsedSeconds(generatedAt, System.nanoTime()),
                    elapsedSeconds(startedAt, System.nanoTime()));
        } finally {
            graph.close();
        }
    }

    private static Vertex[] addVertices(final TinkerGraph graph, final int vertexCount, final long seed) {
        final Vertex[] vertices = new Vertex[vertexCount];
        final int progressInterval = progressInterval(vertexCount);

        for (int ordinal = 0; ordinal < vertexCount; ordinal++) {
            final long mixed = mix64(seed + ordinal);
            final Vertex vertex = graph.addVertex(
                    T.id, (long) ordinal,
                    T.label, VERTEX_LABELS[(int) mixed & (VERTEX_LABELS.length - 1)]);

            addNumericProperty(vertex, ordinal, mixed);
            vertex.property("mixedNumber", numericValue(ordinal, mixed));
            vertex.property("stringLowCardinality",
                    LOW_CARDINALITY_STRINGS[(int) mixed & (LOW_CARDINALITY_STRINGS.length - 1)]);
            vertex.property("stringHighCardinality", "vertex-" + ordinal + '-' + Long.toUnsignedString(mixed, 36));
            vertex.property("duration", Duration.ofSeconds(
                    Math.floorMod(mixed, 7L * 24L * 60L * 60L),
                    Math.floorMod(mix64(mixed), 1_000_000_000L)));
            vertices[ordinal] = vertex;

            if ((ordinal + 1) % progressInterval == 0) {
                LOGGER.info("Added {} of {} vertices", ordinal + 1, vertexCount);
            }
        }

        return vertices;
    }

    private static void addNumericProperty(final Vertex vertex, final int ordinal, final long mixed) {
        switch (ordinal & (NUMERIC_TYPE_COUNT - 1)) {
            case 0:
                vertex.property("byteValue", (byte) mixed);
                break;
            case 1:
                vertex.property("shortValue", (short) mixed);
                break;
            case 2:
                vertex.property("intValue", (int) mixed);
                break;
            case 3:
                vertex.property("longValue", mixed);
                break;
            case 4:
                vertex.property("floatValue", floatingValue(ordinal, mixed));
                break;
            case 5:
                vertex.property("doubleValue", doubleValue(ordinal, mixed));
                break;
            case 6:
                vertex.property("bigIntegerValue",
                        BigInteger.valueOf(mixed).multiply(BigInteger.valueOf(1_000_000_007L)));
                break;
            case 7:
                vertex.property("bigDecimalValue", BigDecimal.valueOf(mixed, ordinal & 15));
                break;
            default:
                throw new IllegalStateException("Unexpected numeric type");
        }
    }

    private static Number numericValue(final long ordinal, final long mixed) {
        switch ((int) ordinal & (NUMERIC_TYPE_COUNT - 1)) {
            case 0:
                return (byte) mixed;
            case 1:
                return (short) mixed;
            case 2:
                return (int) mixed;
            case 3:
                return mixed;
            case 4:
                return floatingValue(ordinal, mixed);
            case 5:
                return doubleValue(ordinal, mixed);
            case 6:
                return BigInteger.valueOf(mixed).multiply(BigInteger.valueOf(1_000_000_007L));
            case 7:
                return BigDecimal.valueOf(mixed, (int) ordinal & 15);
            default:
                throw new IllegalStateException("Unexpected numeric type");
        }
    }

    private static float floatingValue(final long ordinal, final long mixed) {
        switch ((int) ordinal & 1023) {
            case 0:
                return Float.NaN;
            case 1:
                return Float.POSITIVE_INFINITY;
            case 2:
                return Float.NEGATIVE_INFINITY;
            case 3:
                return -0.0f;
            default:
                return (float) (mixed % 1_000_000L) / 100.0f;
        }
    }

    private static double doubleValue(final long ordinal, final long mixed) {
        switch ((int) ordinal & 1023) {
            case 0:
                return Double.NaN;
            case 1:
                return Double.POSITIVE_INFINITY;
            case 2:
                return Double.NEGATIVE_INFINITY;
            case 3:
                return -0.0d;
            default:
                return (double) mixed / 1000.0d;
        }
    }

    private static void addUniformEdges(final Vertex[] vertices, final int edgeFactor, final long seed) {
        final int vertexMask = vertices.length - 1;
        final long edgeCount = Math.multiplyExact((long) vertices.length, edgeFactor);
        final long progressInterval = progressInterval(edgeCount);
        long edgeOrdinal = 0;

        for (int source = 0; source < vertices.length; source++) {
            for (int lane = 0; lane < edgeFactor; lane++) {
                final long laneSeed = mix64(seed + lane);
                final long multiplier = laneSeed | 1L;
                final long increment = mix64(laneSeed);
                final int destination = (int) ((multiplier * source + increment) & vertexMask);
                addEdge(vertices[source], vertices[destination], edgeOrdinal, lane, seed);
                edgeOrdinal++;

                if (edgeOrdinal % progressInterval == 0) {
                    LOGGER.info("Added {} of {} edges", edgeOrdinal, edgeCount);
                }
            }
        }
    }

    private static void addPowerLawEdges(final Vertex[] vertices, final int scale, final long edgeCount,
                                         final long seed) {
        final int vertexMask = vertices.length - 1;
        final SplittableRandom random = new SplittableRandom(seed);
        final long progressInterval = progressInterval(edgeCount);

        for (long edgeOrdinal = 0; edgeOrdinal < edgeCount; edgeOrdinal++) {
            int source = 0;
            int destination = 0;
            for (int bit = 0; bit < scale; bit++) {
                final double quadrant = random.nextDouble();
                source <<= 1;
                destination <<= 1;
                if (quadrant >= RMAT_A) {
                    if (quadrant < RMAT_AB) {
                        destination |= 1;
                    } else if (quadrant < RMAT_ABC) {
                        source |= 1;
                    } else {
                        source |= 1;
                        destination |= 1;
                    }
                }
            }

            source = (int) mix64(source ^ seed) & vertexMask;
            destination = (int) mix64(destination ^ Long.rotateLeft(seed, 17)) & vertexMask;
            addEdge(vertices[source], vertices[destination], edgeOrdinal, (int) mix64(edgeOrdinal), seed);

            if ((edgeOrdinal + 1) % progressInterval == 0) {
                LOGGER.info("Added {} of {} edges", edgeOrdinal + 1, edgeCount);
            }
        }
    }

    private static void addEdge(final Vertex source, final Vertex destination, final long edgeOrdinal,
                                final int labelSelector, final long seed) {
        final long mixed = mix64(seed + edgeOrdinal);
        final Edge edge = source.addEdge(
                EDGE_LABELS[labelSelector & (EDGE_LABELS.length - 1)],
                destination,
                T.id, edgeOrdinal,
                "weight", (float) Math.floorMod(mixed, 1_000_000L) / 1_000_000.0f);

        if ((edgeOrdinal & (EDGE_MIXED_PROPERTY_STRIDE - 1)) == 0) {
            edge.property("mixedNumber", numericValue(edgeOrdinal, mixed));
        }
    }

    private static int progressInterval(final int count) {
        return Math.max(1, count / 10);
    }

    private static long progressInterval(final long count) {
        return Math.max(1L, count / 10L);
    }

    private static long elapsedSeconds(final long start, final long end) {
        return (end - start) / 1_000_000_000L;
    }

    private static long mix64(long value) {
        value = (value ^ (value >>> 30)) * 0xbf58476d1ce4e5b9L;
        value = (value ^ (value >>> 27)) * 0x94d049bb133111ebL;
        return value ^ (value >>> 31);
    }

    private static void validateArguments(final int scale, final int edgeFactor, final Path output) {
        if (scale < 1 || scale > 29) {
            throw new IllegalArgumentException("Scale must be between 1 and 29");
        }
        if (edgeFactor < 1) {
            throw new IllegalArgumentException("Edge factor must be positive");
        }
        Math.multiplyExact(1L << scale, edgeFactor);
        if (Files.exists(output)) {
            throw new IllegalArgumentException("Refusing to overwrite existing file: " + output);
        }
        final Path parent = output.getParent();
        if (parent == null || !Files.isDirectory(parent)) {
            throw new IllegalArgumentException("Output directory does not exist: " + parent);
        }
    }

    private enum Distribution {
        UNIFORM("uniform"),
        POWER_LAW("power-law");

        private final String externalName;

        Distribution(final String externalName) {
            this.externalName = externalName;
        }

        private static Distribution parse(final String value) {
            final String normalized = value.toLowerCase(Locale.ROOT).replace('_', '-');
            for (Distribution distribution : values()) {
                if (distribution.externalName.equals(normalized)) {
                    return distribution;
                }
            }
            throw new IllegalArgumentException("Unknown distribution: " + value);
        }
    }
}
