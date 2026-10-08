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

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.UUID;

/**
 * Generates deterministic uniform, R-MAT or rich benchmark data in a {@link TinkerGraph}.
 * <p/>
 * Usage: {@code CsrBenchmarkDataGenerator <uniform|power-law|rich> <scale> <edge-factor> <output> [seed] [--string-ids]}.
 * {@code rich} has the power-law topology but data that is unfavorable to a columnar snapshot: mixed-type values under
 * one key, multi-properties, meta-properties, rich value types, long strings, sparse and null properties. See
 * {@link #addRichVertices} for the schema. {@code --string-ids} (rich only) uses the String identifiers {@code v<n>}
 * and {@code e<n>} instead of longs. Every vertex keeps exactly one label in every mode.
 * <p/>
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

    public static void main(final String[] argv) throws Exception {
        final List<String> positional = new ArrayList<>();
        boolean flag = false;
        for (final String arg : argv) {
            if ("--string-ids".equals(arg)) {
                flag = true;
            } else {
                positional.add(arg);
            }
        }
        final boolean stringIds = flag;
        final String[] args = positional.toArray(new String[0]);
        if (args.length < 4 || args.length > 5) {
            throw new IllegalArgumentException(
                    "Usage: CsrBenchmarkDataGenerator <uniform|power-law|rich> <scale> <edge-factor> <output> [seed]"
                            + " [--string-ids]");
        }

        final Distribution distribution = Distribution.parse(args[0]);
        final int scale = Integer.parseInt(args[1]);
        final int edgeFactor = Integer.parseInt(args[2]);
        final Path output = Paths.get(args[3]).toAbsolutePath();
        final long seed = args.length == 5 ? Long.decode(args[4]) : DEFAULT_SEED;
        if (stringIds && distribution != Distribution.RICH) {
            throw new IllegalArgumentException("--string-ids is only supported by the rich mode");
        }

        validateArguments(scale, edgeFactor, output);

        final int vertexCount = 1 << scale;
        final long edgeCount = Math.multiplyExact((long) vertexCount, edgeFactor);
        LOGGER.info("Generating {} graph with {} vertices, {} edges, edge factor {}, seed {}{}",
                distribution.externalName, vertexCount, edgeCount, edgeFactor, seed, stringIds ? " and string ids" : "");

        final long startedAt = System.nanoTime();
        final TinkerGraph graph;
        if (distribution == Distribution.RICH) {
            // list cardinality for the multi-properties, and null values for the nullable key
            final Configuration configuration = new BaseConfiguration();
            configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "list");
            configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_ALLOW_NULL_PROPERTY_VALUES, true);
            graph = TinkerGraph.open(configuration);
        } else {
            graph = TinkerGraph.open();
        }
        try {
            final Vertex[] vertices;
            final EdgeWriter writer;
            if (distribution == Distribution.RICH) {
                vertices = addRichVertices(graph, vertexCount, seed, stringIds);
                writer = (source, destination, ordinal, labelSelector) ->
                        addRichEdge(source, destination, ordinal, labelSelector, seed, stringIds);
            } else {
                vertices = addVertices(graph, vertexCount, seed);
                writer = (source, destination, ordinal, labelSelector) ->
                        addEdge(source, destination, ordinal, labelSelector, seed);
            }
            if (distribution == Distribution.UNIFORM) {
                addUniformEdges(vertices, edgeFactor, seed, writer);
            } else {
                addPowerLawEdges(vertices, scale, edgeCount, seed, writer);
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

    @FunctionalInterface
    private interface EdgeWriter {
        void add(Vertex source, Vertex destination, long edgeOrdinal, int labelSelector);
    }

    // ---------------------------------------------------------------- rich mode

    private static final String[] WORDS = {
            "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
            "india", "juliet", "kilo", "lima", "mike", "november", "oscar", "papa"};
    private static final String BIO_PREFIX =
            "this biography starts with a long prefix shared by every vertex so that string comparison has to scan it: ";
    private static final long EPOCH_2020 = 1_577_836_800L;
    private static final int TAG_VOCABULARY = 16;
    private static final int LOCATION_VOCABULARY = 50;

    /**
     * Adds the vertices of the rich schema. Every vertex has exactly one label from the usual pool and these
     * properties (the graph must use list cardinality and allow null values):
     * <ul>
     * <li>{@code name}: unique string.</li>
     * <li>{@code mixed}: int, long, double, String or boolean by ordinal modulo 5; the numbers lie in 0..999.</li>
     * <li>{@code tag}: 0 to 5 distinct values of {@code tag-0}..{@code tag-15}, list cardinality.</li>
     * <li>{@code location}: 1 to 3 values {@code city-0}..{@code city-49}, each with a long meta-property
     * {@code since} and, for about half of them, {@code until}.</li>
     * <li>{@code uuid}, {@code created} (OffsetDateTime with an offset of -2 to +2 hours), {@code elapsed}
     * (Duration), {@code amount} (BigDecimal), {@code bignum} (BigInteger), {@code blob} (byte[8]), {@code listval}
     * (List with a nested List and Map), {@code setval} (Set), {@code mapval} (Map with a nested Map),
     * {@code charval}, {@code shortval}, {@code floatval}, {@code byteval}: one value each.</li>
     * <li>{@code bio}: a string of several hundred characters with a common prefix and a unique suffix, for sorting.</li>
     * <li>{@code sparse}: an int on about 2% of the vertices.</li>
     * <li>{@code nullable}: null on a quarter of the vertices, a string on a quarter, absent on the rest.</li>
     * <li>{@code age} (int, 0..99) and {@code score} (long, 0..9999): plain typed columns.</li>
     * </ul>
     */
    private static Vertex[] addRichVertices(final TinkerGraph graph, final int vertexCount, final long seed,
                                            final boolean stringIds) {
        final Vertex[] vertices = new Vertex[vertexCount];
        final int progressInterval = progressInterval(vertexCount);

        for (int ordinal = 0; ordinal < vertexCount; ordinal++) {
            final long h = mix64(seed + ordinal);
            final long h2 = mix64(h);
            final long h3 = mix64(h2);
            final Vertex vertex = graph.addVertex(
                    T.id, stringIds ? (Object) ("v" + ordinal) : (Object) (long) ordinal,
                    T.label, VERTEX_LABELS[(int) h & (VERTEX_LABELS.length - 1)]);

            vertex.property("name", "name-" + ordinal);
            vertex.property("mixed", mixedValue(ordinal, h2));
            vertex.property("age", (int) Math.floorMod(h3, 100L));
            vertex.property("score", Math.floorMod(h2, 10_000L));

            final int tags = (int) Math.floorMod(h >>> 8, 6L);
            final int tagStart = (int) Math.floorMod(h2 >>> 8, (long) TAG_VOCABULARY);
            for (int i = 0; i < tags; i++) {
                vertex.property("tag", "tag-" + ((tagStart + 3 * i) % TAG_VOCABULARY));
            }

            final int locations = 1 + (int) Math.floorMod(h >>> 16, 3L);
            for (int i = 0; i < locations; i++) {
                final long l = mix64(h3 + i);
                final VertexProperty<String> location = vertex.property("location",
                        "city-" + Math.floorMod(l, (long) LOCATION_VOCABULARY));
                final long since = EPOCH_2020 + Math.floorMod(l >>> 8, 200_000_000L);
                location.property("since", since);
                if (((l >>> 3) & 1) == 0) {
                    location.property("until", since + Math.floorMod(l >>> 20, 100_000_000L));
                }
            }

            vertex.property("uuid", new UUID(h, h2));
            vertex.property("created", OffsetDateTime.ofInstant(
                    Instant.ofEpochSecond(EPOCH_2020 + Math.floorMod(h3, 200_000_000L), 0),
                    ZoneOffset.ofHours((int) Math.floorMod(h >>> 24, 5L) - 2)));
            vertex.property("elapsed", Duration.ofSeconds(
                    Math.floorMod(h, 7L * 24L * 60L * 60L), Math.floorMod(h2, 1_000_000_000L)));
            vertex.property("amount", BigDecimal.valueOf(Math.floorMod(h2, 1_000_000_000L), 4));
            vertex.property("bignum", BigInteger.valueOf(h3).multiply(BigInteger.valueOf(1_000_000_007L)));
            vertex.property("blob", new byte[]{(byte) h, (byte) (h >>> 8), (byte) (h >>> 16), (byte) (h >>> 24),
                    (byte) (h >>> 32), (byte) (h >>> 40), (byte) (h >>> 48), (byte) (h >>> 56)});
            vertex.property("listval", listValue(h, h2));
            vertex.property("setval", setValue(h2));
            vertex.property("mapval", mapValue(ordinal, h, h3));
            vertex.property("charval", (char) ('a' + Math.floorMod(h, 26L)));
            vertex.property("shortval", (short) h2);
            vertex.property("floatval", (float) Math.floorMod(h3, 100_000L) / 100.0f);
            vertex.property("byteval", (byte) h3);

            vertex.property("bio", bio(ordinal, h, h2));

            if (Math.floorMod(h2 >>> 5, 50L) == 0) {
                vertex.property("sparse", (int) Math.floorMod(h3, 1000L));
            }
            switch ((int) Math.floorMod(h >>> 40, 4L)) {
                case 0:
                    vertex.property("nullable", (Object) null);
                    break;
                case 1:
                    vertex.property("nullable", "present-" + Math.floorMod(h3, 100L));
                    break;
                default:
                    break;
            }
            vertices[ordinal] = vertex;

            if ((ordinal + 1) % progressInterval == 0) {
                LOGGER.info("Added {} of {} vertices", ordinal + 1, vertexCount);
            }
        }

        return vertices;
    }

    private static Object mixedValue(final int ordinal, final long h) {
        final int value = (int) Math.floorMod(h, 1000L);
        switch (ordinal % 5) {
            case 0:
                return value;
            case 1:
                return (long) value;
            case 2:
                return value + 0.5d;
            case 3:
                return "s" + value;
            default:
                return (value & 1) == 0;
        }
    }

    private static List<Object> listValue(final long h, final long h2) {
        final List<Object> nested = new ArrayList<>();
        nested.add(Math.floorMod(h, 10L));
        nested.add(Math.floorMod(h2, 10L));
        final Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("k", Math.floorMod(h2, 100L));
        final List<Object> list = new ArrayList<>();
        list.add((int) Math.floorMod(h, 1000L));
        list.add("e" + Math.floorMod(h2, 1000L));
        list.add(Math.floorMod(h, 1000L) / 10.0d);
        list.add(nested);
        list.add(inner);
        return list;
    }

    private static Set<Object> setValue(final long h) {
        final Set<Object> set = new LinkedHashSet<>();
        for (int i = 0; i < 3; i++) {
            set.add("m" + Math.floorMod(mix64(h + i), 8L));
        }
        return set;
    }

    private static Map<String, Object> mapValue(final int ordinal, final long h, final long h3) {
        final Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("x", (int) Math.floorMod(h3, 100L));
        inner.put("y", "y" + Math.floorMod(h, 10L));
        final Map<String, Object> map = new LinkedHashMap<>();
        map.put("id", (long) ordinal);
        map.put("kind", WORDS[(int) Math.floorMod(h3, (long) WORDS.length)]);
        map.put("inner", inner);
        return map;
    }

    private static String bio(final int ordinal, final long h, final long h2) {
        final StringBuilder sb = new StringBuilder(BIO_PREFIX);
        final int words = 40 + (int) Math.floorMod(h2, 60L);
        long w = h;
        for (int i = 0; i < words; i++) {
            w = mix64(w);
            sb.append(WORDS[(int) Math.floorMod(w, (long) WORDS.length)]).append(' ');
        }
        return sb.append('#').append(ordinal).toString();
    }

    private static void addRichEdge(final Vertex source, final Vertex destination, final long edgeOrdinal,
                                    final int labelSelector, final long seed, final boolean stringIds) {
        final long mixed = mix64(seed + edgeOrdinal);
        final int value = (int) Math.floorMod(mixed, 1000L);
        final Object mixedEdge;
        switch ((int) (edgeOrdinal % 5)) {
            case 0:
                mixedEdge = value;
                break;
            case 1:
                mixedEdge = (long) value;
                break;
            case 2:
                mixedEdge = value + 0.5d;
                break;
            case 3:
                mixedEdge = "s" + value;
                break;
            default:
                mixedEdge = (value & 1) == 0;
                break;
        }
        source.addEdge(
                EDGE_LABELS[labelSelector & (EDGE_LABELS.length - 1)],
                destination,
                T.id, stringIds ? (Object) ("e" + edgeOrdinal) : (Object) edgeOrdinal,
                "weight", (double) Math.floorMod(mixed >>> 8, 1_000_000L) / 1_000_000.0d,
                "mixedEdge", mixedEdge,
                "since", OffsetDateTime.ofInstant(
                        Instant.ofEpochSecond(EPOCH_2020 + Math.floorMod(mixed >>> 16, 200_000_000L), 0),
                        ZoneOffset.UTC));
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

    private static void addUniformEdges(final Vertex[] vertices, final int edgeFactor, final long seed,
                                        final EdgeWriter writer) {
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
                writer.add(vertices[source], vertices[destination], edgeOrdinal, lane);
                edgeOrdinal++;

                if (edgeOrdinal % progressInterval == 0) {
                    LOGGER.info("Added {} of {} edges", edgeOrdinal, edgeCount);
                }
            }
        }
    }

    private static void addPowerLawEdges(final Vertex[] vertices, final int scale, final long edgeCount,
                                         final long seed, final EdgeWriter writer) {
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
            writer.add(vertices[source], vertices[destination], edgeOrdinal, (int) mix64(edgeOrdinal));

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
        POWER_LAW("power-law"),
        RICH("rich");

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
