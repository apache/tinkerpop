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
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.snapshot.CsrSnapshot;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.BuildOptions;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.BuildStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.HeapSnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.SnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.build.StreamingSnapshotBuilder;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentPaths;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SnapshotLayout;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SnapshotSource;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 * Builds, verifies and measures CSR snapshots from a {@link CsrBenchmarkDataGenerator} output file.
 * <p/>
 * This is a manually invoked spike utility, not a test. It loads the Gryo file into a {@link TinkerGraph} (or, with
 * {@code --source gryo}, streams it through {@link GryoSnapshotSource} and loads the graph only for {@code --verify}), builds a
 * snapshot with the materializing ({@code heap}) builder, the {@code streaming} builder or both, and reports phase
 * times, peak on-disk bytes, peak heap and published bytes. {@code --verify} checks each snapshot against the graph and
 * {@code --compare} checks that the two builders produce identical bundles. Run it with no arguments for usage.
 */
public final class CsrSnapshotBuildBenchmark {

    private static final String USAGE = String.join(System.lineSeparator(),
            "Usage: CsrSnapshotBuildBenchmark --input <file.kryo> [options]",
            "",
            "  --input <file>              generator output (Gryo) to load into TinkerGraph (required)",
            "  --source <tinkergraph|gryo> build from the TinkerGraph loaded from the input or stream the Gryo",
            "                              file directly; gryo loads the TinkerGraph only for --verify",
            "                              (default tinkergraph)",
            "  --builder <heap|streaming|both>",
            "                              builder(s) to run (default heap; --compare implies both)",
            "  --layout <topology|identity|full>",
            "                              layout to publish (default full)",
            "  --memory-budget <bytes>     streaming builder memory budget; k, m or g suffix allowed (default 256m)",
            "  --grouped-fast-path <true|false>",
            "                              let builders use the grouped-by-out-vertex path (default true)",
            "  --edge-id-index <true|false>",
            "                              publish the edge identifier index (default true)",
            "  --output-dir <dir>          directory receiving one sub-directory per builder; must not already",
            "                              contain them (default: a new directory under java.io.tmpdir)",
            "  --scratch-dir <dir>         scratch directory for the builders (default: the build directory)",
            "  --verify                    check each snapshot against the TinkerGraph",
            "  --compare                   build with both builders and report the first differing file");

    private static final int MAX_FAILURES = 20;
    private static final long SAMPLE_INTERVAL_MILLIS = 10;

    private CsrSnapshotBuildBenchmark() {
    }

    public static void main(final String[] args) {
        final int status;
        try {
            status = run(args);
        } catch (UsageException e) {
            System.err.println(e.getMessage());
            System.err.println();
            System.err.println(USAGE);
            System.exit(2);
            return;
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
            return;
        }
        System.exit(status);
    }

    private static int run(final String[] args) throws Exception {
        final Config config = Config.parse(args);

        final AbstractTinkerGraph graph;
        if (config.source == Source.TINKERGRAPH || config.verify) {
            System.out.println("Loading " + config.input);
            final long loadStart = System.nanoTime();
            // list cardinality, so that multi-properties in the file survive the load and match what the Gryo source emits
            final Configuration configuration = new BaseConfiguration();
            configuration.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, "list");
            graph = (AbstractTinkerGraph) TinkerGraph.open(configuration);
            try (GraphTraversalSource g = graph.traversal()) {
                g.io(config.input.toString()).read().iterate();
            }
            System.out.printf(Locale.ROOT, "Loaded %,d vertices and %,d edges in %s%n",
                    graph.getVerticesCount(), graph.getEdgesCount(), millis(System.nanoTime() - loadStart));
        } else {
            graph = null;
            System.out.println("Streaming " + config.input + " without loading it into TinkerGraph");
        }

        final Path outputDir = config.outputDir != null ? config.outputDir : Files.createTempDirectory("csr-snapshot-bench-");
        Files.createDirectories(outputDir);
        System.out.println("Output directory " + outputDir);

        final BuildOptions.Builder optionsBuilder = BuildOptions.builder()
                .layout(config.layout)
                .memoryBudgetBytes(config.memoryBudget)
                .groupedFastPath(config.groupedFastPath)
                .edgeIdIndex(config.edgeIdIndex);
        if (config.scratchDir != null) optionsBuilder.scratchDirectory(config.scratchDir);
        final BuildOptions options = optionsBuilder.build();

        boolean ok = true;
        final Map<String, Path> built = new HashMap<>();
        // one source for every build, so that both bundles record the same source identifier
        try (SnapshotSource source = config.source == Source.GRYO
                ? new GryoSnapshotSource(config.input) : new TinkerGraphSnapshotSource(graph)) {
            for (final String name : config.builders) {
                final SnapshotBuilder builder = "heap".equals(name) ? new HeapSnapshotBuilder() : new StreamingSnapshotBuilder();
                final Path target = outputDir.resolve(name);
                ok &= buildAndReport(graph, source, name, builder, target, options, config);
                built.put(name, target);
            }

            if (config.compare) {
                ok &= compare(built.get("heap"), built.get("streaming"));
            }
        } finally {
            if (graph != null) graph.close();
        }
        System.out.println(ok ? "RESULT: OK" : "RESULT: FAILED");
        return ok ? 0 : 1;
    }

    // ---------------------------------------------------------------- build and report

    private static boolean buildAndReport(final AbstractTinkerGraph graph, final SnapshotSource source, final String name,
                                          final SnapshotBuilder builder, final Path target, final BuildOptions options, final Config config) {
        System.out.printf(Locale.ROOT, "%n== %s builder: layout=%s, groupedFastPath=%s, edgeIdIndex=%s, memoryBudget=%s%n",
                name, options.layout(), options.groupedFastPath(), options.edgeIdIndex(), bytes(options.memoryBudgetBytes()));

        System.gc();
        final HeapSampler sampler = new HeapSampler();
        final long baseline = sampler.baseline;
        sampler.start();
        final BuildStats stats;
        final long elapsed;
        try {
            final long start = System.nanoTime();
            stats = builder.build(source, target, options);
            elapsed = System.nanoTime() - start;
        } finally {
            sampler.stop();
        }

        System.out.printf(Locale.ROOT, "  %-24s %12s %18s%n", "phase", "elapsed", "disk bytes");
        for (final BuildStats.Phase phase : stats.phases()) {
            System.out.printf(Locale.ROOT, "  %-24s %12s %18s%n", phase.name(), millis(phase.elapsedNanos()), bytes(phase.diskBytes()));
        }
        System.out.printf(Locale.ROOT, "  total build time:    %s%n", millis(elapsed));
        System.out.printf(Locale.ROOT, "  peak on-disk bytes:  %s%n", bytes(stats.peakDiskBytes()));
        System.out.printf(Locale.ROOT, "  peak heap:           %s (used heap after GC before build: %s, sampled every %d ms)%n",
                bytes(sampler.peak.get()), bytes(baseline), SAMPLE_INTERVAL_MILLIS);

        long topology = 0, identity = 0, properties = 0;
        for (final Map.Entry<String, Long> segment : stats.segmentBytes().entrySet()) {
            switch (classify(segment.getKey())) {
                case TOPOLOGY:
                    topology += segment.getValue();
                    break;
                case IDENTITY:
                    identity += segment.getValue();
                    break;
                default:
                    properties += segment.getValue();
                    break;
            }
        }
        System.out.printf(Locale.ROOT, "  published bytes:     topology %s, identity %s, properties %s, total %s (%d segments)%n",
                bytes(topology), bytes(identity), bytes(properties), bytes(stats.totalSegmentBytes()), stats.segmentBytes().size());

        if (!config.verify) return true;
        return verify(graph, source, target, options.layout());
    }

    private enum Source {TINKERGRAPH, GRYO}

    private enum Group {TOPOLOGY, IDENTITY, PROPERTIES}

    // topology is the adjacency offsets and neighbors; identity is everything else outside properties/, including the
    // adjacency edge ordinals
    private static Group classify(final String path) {
        if (path.startsWith(SegmentPaths.PROPERTIES_DIR + SegmentPaths.SEPARATOR)) return Group.PROPERTIES;
        if (path.equals(SegmentPaths.OUT_OFFSETS) || path.equals(SegmentPaths.OUT_NEIGHBORS)
                || path.equals(SegmentPaths.IN_OFFSETS) || path.equals(SegmentPaths.IN_NEIGHBORS)) return Group.TOPOLOGY;
        return Group.IDENTITY;
    }

    /**
     * Samples used heap on a daemon thread. The peak is the highest sample, so it can miss short spikes.
     */
    private static final class HeapSampler implements Runnable {
        private final Runtime runtime = Runtime.getRuntime();
        private final AtomicLong peak = new AtomicLong();
        private final long baseline = used();
        private volatile boolean running = true;
        private Thread thread;

        private long used() {
            return runtime.totalMemory() - runtime.freeMemory();
        }

        void start() {
            peak.set(used());
            thread = new Thread(this, "heap-sampler");
            thread.setDaemon(true);
            thread.start();
        }

        void stop() {
            running = false;
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            peak.accumulateAndGet(used(), Math::max);
        }

        @Override
        public void run() {
            while (running) {
                peak.accumulateAndGet(used(), Math::max);
                try {
                    Thread.sleep(SAMPLE_INTERVAL_MILLIS);
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    // ---------------------------------------------------------------- compare

    /**
     * Reports the first file, in ascending relative path order, that exists in only one bundle or differs in content,
     * along with the byte offset of the difference.
     */
    private static boolean compare(final Path heap, final Path streaming) throws IOException {
        System.out.printf("%n== compare heap vs streaming%n");
        final TreeSet<String> paths = new TreeSet<>();
        paths.addAll(listFiles(heap));
        paths.addAll(listFiles(streaming));

        for (final String path : paths) {
            final Path a = heap.resolve(path);
            final Path b = streaming.resolve(path);
            if (!Files.exists(a)) {
                System.out.println("  DIFFERENT: " + path + " exists only in the streaming bundle");
                return false;
            }
            if (!Files.exists(b)) {
                System.out.println("  DIFFERENT: " + path + " exists only in the heap bundle");
                return false;
            }
            final long offset = firstDifference(a, b);
            if (offset >= 0) {
                System.out.printf(Locale.ROOT, "  DIFFERENT: %s at byte offset %,d (heap size %,d, streaming size %,d)%n",
                        path, offset, Files.size(a), Files.size(b));
                return false;
            }
        }
        System.out.printf(Locale.ROOT, "  identical (%d files)%n", paths.size());
        return true;
    }

    private static TreeSet<String> listFiles(final Path root) throws IOException {
        final TreeSet<String> files = new TreeSet<>();
        try (Stream<Path> walk = Files.walk(root)) {
            final Iterator<Path> it = walk.filter(Files::isRegularFile).iterator();
            while (it.hasNext()) {
                files.add(root.relativize(it.next()).toString().replace('\\', '/'));
            }
        }
        return files;
    }

    /**
     * The offset of the first differing byte, the length of the shorter file if one is a prefix of the other, or -1
     * when the files are identical.
     */
    private static long firstDifference(final Path a, final Path b) throws IOException {
        try (InputStream in1 = new BufferedInputStream(Files.newInputStream(a), 1 << 20);
             InputStream in2 = new BufferedInputStream(Files.newInputStream(b), 1 << 20)) {
            final byte[] buf1 = new byte[1 << 16];
            final byte[] buf2 = new byte[1 << 16];
            long offset = 0;
            while (true) {
                final int n1 = in1.readNBytes(buf1, 0, buf1.length);
                final int n2 = in2.readNBytes(buf2, 0, buf2.length);
                final int common = Math.min(n1, n2);
                for (int i = 0; i < common; i++) {
                    if (buf1[i] != buf2[i]) return offset + i;
                }
                if (n1 != n2) return offset + common;
                if (n1 < buf1.length) return -1;
                offset += n1;
            }
        }
    }

    // ---------------------------------------------------------------- verify

    private static final class Failures {
        private final List<String> messages = new ArrayList<>();
        private long count;

        void add(final String message) {
            count++;
            if (messages.size() < MAX_FAILURES) messages.add(message);
        }

        boolean tooMany() {
            return messages.size() >= MAX_FAILURES;
        }
    }

    private static boolean verify(final AbstractTinkerGraph graph, final SnapshotSource source, final Path target, final SnapshotLayout layout) {
        System.out.println("  verify: opening snapshot with checksum verification");
        final long start = System.nanoTime();
        final Failures failures = new Failures();
        try (CsrSnapshot snapshot = CsrSnapshot.open(target, true)) {
            if (layout == SnapshotLayout.TOPOLOGY) {
                System.out.println("  verify: layout TOPOLOGY publishes no identifiers, labels, endpoints or properties; "
                        + "checking counts and adjacency by ordinal only (ordinals assumed to follow vertex scan order), "
                        + "skipping identity and property checks");
            } else if (layout == SnapshotLayout.IDENTITY) {
                System.out.println("  verify: layout IDENTITY publishes no properties; skipping property checks");
            }
            // ordinals follow the scan order of the source, which for a Gryo file is the file order rather than the
            // iteration order of the graph
            Map<Object, Integer> fileOrder = null;
            if (source instanceof GryoSnapshotSource) {
                final Map<Object, Integer> order = new HashMap<>();
                source.scanVertices((id, labels, properties) -> order.put(id, order.size()));
                fileOrder = order;
            }
            verifySnapshot(graph, snapshot, layout, fileOrder, failures);
        } catch (RuntimeException e) {
            failures.add("exception during verify: " + e);
            e.printStackTrace(System.out);
        }

        if (failures.count == 0) {
            System.out.printf(Locale.ROOT, "  verify: OK in %s%n", millis(System.nanoTime() - start));
            return true;
        }
        System.out.printf(Locale.ROOT, "  verify: FAILED with %d problem(s); first %d:%n", failures.count, failures.messages.size());
        for (final String message : failures.messages) System.out.println("    " + message);
        return false;
    }

    private static void verifySnapshot(final AbstractTinkerGraph graph, final CsrSnapshot snapshot, final SnapshotLayout layout,
                                       final Map<Object, Integer> fileOrder, final Failures failures) {
        final boolean identity = layout != SnapshotLayout.TOPOLOGY;
        final boolean properties = layout == SnapshotLayout.FULL;

        if (snapshot.vertexCount() != graph.getVerticesCount())
            failures.add("vertex count " + snapshot.vertexCount() + " != " + graph.getVerticesCount());
        if (snapshot.edgeCount() != graph.getEdgesCount())
            failures.add("edge count " + snapshot.edgeCount() + " != " + graph.getEdgesCount());

        // only needed when the snapshot cannot translate identifiers to ordinals: the graph side of adjacency is then
        // expressed as ordinals, assuming they follow the vertex scan order
        Map<Object, Integer> ordinalOf = fileOrder;
        if (!identity && null == ordinalOf) {
            ordinalOf = new HashMap<>();
            final Iterator<Vertex> vertices = graph.vertices();
            int ordinal = 0;
            while (vertices.hasNext()) ordinalOf.put(vertices.next().id(), ordinal++);
        }

        long vertexPropertyCount = 0;
        long edgePropertyCount = 0;
        long edgesSeen = 0;
        int ordinal = 0;
        final Iterator<Vertex> vertices = graph.vertices();
        while (vertices.hasNext() && !failures.tooMany()) {
            final Vertex vertex = vertices.next();
            final int v = null == fileOrder ? ordinal++ : fileOrder.getOrDefault(vertex.id(), -1);
            if (null != fileOrder) ordinal++;
            if (ordinal > snapshot.vertexCount()) {
                failures.add("snapshot has fewer vertices than the graph");
                break;
            }
            if (v < 0) {
                failures.add("vertex [" + vertex.id() + "] is not in the Gryo file");
                continue;
            }

            if (identity) {
                final int found = snapshot.vertexOrdinal(vertex.id());
                if (found != v) {
                    failures.add("vertex [" + vertex.id() + "] has ordinal " + found + ", expected " + v);
                    continue;
                }
                if (!Objects.equals(vertex.id(), snapshot.vertexId(v)))
                    failures.add("vertex ordinal " + v + " has id " + snapshot.vertexId(v) + ", expected " + vertex.id());
                if (!Objects.equals(vertex.label(), snapshot.vertexLabel(v)))
                    failures.add("vertex [" + vertex.id() + "] has label " + snapshot.vertexLabel(v) + ", expected " + vertex.label());
            }

            if (properties) {
                // the vertex properties of each key in graph order, compared with the range the snapshot holds for the key
                final Map<String, List<VertexProperty<Object>>> byKey = new LinkedHashMap<>();
                final Iterator<VertexProperty<Object>> it = vertex.properties();
                while (it.hasNext()) {
                    final VertexProperty<Object> vp = it.next();
                    vertexPropertyCount++;
                    byKey.computeIfAbsent(vp.key(), k -> new ArrayList<>()).add(vp);
                }
                for (final Map.Entry<String, List<VertexProperty<Object>>> entry : byKey.entrySet()) {
                    final int keyCode = snapshot.vertexPropertyKeys().indexOf(entry.getKey());
                    if (keyCode < 0) {
                        failures.add("vertex [" + vertex.id() + "] property '" + entry.getKey() + "' has no column");
                        continue;
                    }
                    final List<VertexProperty<Object>> expected = entry.getValue();
                    final long start = snapshot.vertexPropertyStart(keyCode, v);
                    final long end = snapshot.vertexPropertyEnd(keyCode, v);
                    if (end - start != expected.size()) {
                        failures.add("vertex [" + vertex.id() + "] property '" + entry.getKey() + "' has " + (end - start)
                                + " values, expected " + expected.size());
                        continue;
                    }
                    for (int i = 0; i < expected.size(); i++) {
                        final VertexProperty<Object> vp = expected.get(i);
                        final Object value = snapshot.vertexPropertyValue(keyCode, start + i);
                        if (!Objects.equals(vp.value(), value))
                            failures.add("vertex [" + vertex.id() + "] property '" + vp.key() + "' is " + describe(value)
                                    + ", expected " + describe(vp.value()));
                        final Object id = snapshot.vertexPropertyIdentifier(keyCode, start + i);
                        if (!Objects.equals(vp.id(), id))
                            failures.add("vertex [" + vertex.id() + "] property '" + vp.key() + "' has property id " + describe(id)
                                    + ", expected " + describe(vp.id()));
                    }
                }
            }

            verifyAdjacency(snapshot, vertex, v, Direction.OUT, identity, ordinalOf, failures);
            verifyAdjacency(snapshot, vertex, v, Direction.IN, identity, ordinalOf, failures);

            if (identity) {
                final Iterator<Edge> edges = vertex.edges(Direction.OUT);
                while (edges.hasNext()) {
                    final Edge edge = edges.next();
                    edgesSeen++;
                    edgePropertyCount += verifyEdge(snapshot, edge, v, properties, failures);
                }
            }
        }

        if (!failures.tooMany() && identity) {
            if (edgesSeen != snapshot.edgeCount())
                failures.add("walked " + edgesSeen + " edges from the graph but the snapshot has " + snapshot.edgeCount());
        }
        if (properties && !failures.tooMany()) {
            long snapshotVertexProperties = 0;
            for (final String key : snapshot.vertexPropertyKeys()) snapshotVertexProperties += snapshot.vertexProperty(key).presentCount();
            if (snapshotVertexProperties != vertexPropertyCount)
                failures.add("snapshot holds " + snapshotVertexProperties + " vertex properties but the graph has " + vertexPropertyCount);
            long snapshotEdgeProperties = 0;
            for (final String key : snapshot.edgePropertyKeys()) snapshotEdgeProperties += snapshot.edgeProperty(key).presentCount();
            if (snapshotEdgeProperties != edgePropertyCount)
                failures.add("snapshot holds " + snapshotEdgeProperties + " edge properties but the graph has " + edgePropertyCount);
        }
    }

    // compares the multiset of (neighbor id, edge id, edge label) of one vertex and direction; without identity the
    // multiset is of neighbor ordinals only
    private static void verifyAdjacency(final CsrSnapshot snapshot, final Vertex vertex, final int v, final Direction direction,
                                        final boolean identity, final Map<Object, Integer> ordinalOf, final Failures failures) {
        final Map<List<Object>, Integer> expected = new HashMap<>();
        final Iterator<Edge> edges = vertex.edges(direction);
        while (edges.hasNext()) {
            final Edge edge = edges.next();
            final Vertex neighbor = direction == Direction.OUT ? edge.inVertex() : edge.outVertex();
            expected.merge(identity
                    ? List.of(neighbor.id(), edge.id(), edge.label())
                    : List.of(ordinalOf.get(neighbor.id())), 1, Integer::sum);
        }

        final boolean out = direction == Direction.OUT;
        final long begin = out ? snapshot.outStart(v) : snapshot.inStart(v);
        final long end = out ? snapshot.outEnd(v) : snapshot.inEnd(v);
        final Map<List<Object>, Integer> actual = new HashMap<>();
        int previousEdge = -1;
        for (long pos = begin; pos < end; pos++) {
            final int neighbor = out ? snapshot.outNeighbor(pos) : snapshot.inNeighbor(pos);
            if (identity) {
                final int edge = out ? snapshot.outEdge(pos) : snapshot.inEdge(pos);
                if (edge <= previousEdge)
                    failures.add("vertex [" + vertex.id() + "] " + direction + " adjacency is not in ascending edge ordinal order");
                previousEdge = edge;
                actual.merge(List.of(snapshot.vertexId(neighbor), snapshot.edgeId(edge), snapshot.edgeLabel(edge)), 1, Integer::sum);
            } else {
                actual.merge(List.of(neighbor), 1, Integer::sum);
            }
        }

        if (!expected.equals(actual))
            failures.add("vertex [" + vertex.id() + "] " + direction + " adjacency differs: snapshot-only " + difference(actual, expected)
                    + ", graph-only " + difference(expected, actual));
    }

    // the entries of a that are not matched one for one by entries of b
    private static Map<List<Object>, Integer> difference(final Map<List<Object>, Integer> a, final Map<List<Object>, Integer> b) {
        final Map<List<Object>, Integer> result = new HashMap<>();
        for (final Map.Entry<List<Object>, Integer> entry : a.entrySet()) {
            final int extra = entry.getValue() - b.getOrDefault(entry.getKey(), 0);
            if (extra > 0) result.put(entry.getKey(), extra);
        }
        return result;
    }

    // returns the number of properties on the edge
    private static int verifyEdge(final CsrSnapshot snapshot, final Edge edge, final int outOrdinal, final boolean properties,
                                  final Failures failures) {
        int propertyCount = 0;
        final int e;
        if (snapshot.hasEdgeIdIndex()) {
            e = snapshot.edgeOrdinal(edge.id());
            if (e < 0 || e >= snapshot.edgeCount()) {
                failures.add("edge [" + edge.id() + "] is not in the edge identifier index");
                return countProperties(edge);
            }
        } else {
            // no edge identifier index: find the edge in the out adjacency of its out-vertex instead
            e =findOutEdge(snapshot, outOrdinal, edge.id());
            if (e < 0) {
                failures.add("edge [" + edge.id() + "] is not in the out adjacency of vertex ordinal " + outOrdinal);
                return countProperties(edge);
            }
        }

        if (!Objects.equals(edge.id(), snapshot.edgeId(e)))
            failures.add("edge ordinal " + e + " has id " + snapshot.edgeId(e) + ", expected " + edge.id());
        if (!Objects.equals(edge.label(), snapshot.edgeLabel(e)))
            failures.add("edge [" + edge.id() + "] has label " + snapshot.edgeLabel(e) + ", expected " + edge.label());
        if (snapshot.edgeOut(e) != outOrdinal)
            failures.add("edge [" + edge.id() + "] has out ordinal " + snapshot.edgeOut(e) + ", expected " + outOrdinal);
        final int in = snapshot.vertexOrdinal(edge.inVertex().id());
        if (snapshot.edgeIn(e) != in)
            failures.add("edge [" + edge.id() + "] has in ordinal " + snapshot.edgeIn(e) + ", expected " + in);

        final Iterator<Property<Object>> it = edge.properties();
        while (it.hasNext()) {
            final Property<Object> p = it.next();
            propertyCount++;
            if (!properties) continue;
            final ColumnReader column = snapshot.edgeProperty(p.key());
            if (column == null) {
                failures.add("edge [" + edge.id() + "] property '" + p.key() + "' has no column");
                continue;
            }
            final Object value = column.get(e);
            if (!Objects.equals(p.value(), value))
                failures.add("edge [" + edge.id() + "] property '" + p.key() + "' is " + describe(value) + ", expected " + describe(p.value()));
        }
        return propertyCount;
    }

    private static int findOutEdge(final CsrSnapshot snapshot, final int vertex, final Object edgeId) {
        for (long pos = snapshot.outStart(vertex); pos < snapshot.outEnd(vertex); pos++) {
            final int edge = snapshot.outEdge(pos);
            if (Objects.equals(edgeId, snapshot.edgeId(edge))) return edge;
        }
        return -1;
    }

    private static int countProperties(final Edge edge) {
        int count = 0;
        final Iterator<Property<Object>> it = edge.properties();
        while (it.hasNext()) {
            it.next();
            count++;
        }
        return count;
    }

    private static String describe(final Object value) {
        return value == null ? "null" : value + " (" + value.getClass().getSimpleName() + ")";
    }

    // ---------------------------------------------------------------- formatting

    private static String millis(final long nanos) {
        return String.format(Locale.ROOT, "%,.1f ms", nanos / 1_000_000.0);
    }

    private static String bytes(final long bytes) {
        final String raw = String.format(Locale.ROOT, "%,d", bytes);
        if (bytes < 1024) return raw + " B";
        final String[] units = {"KiB", "MiB", "GiB", "TiB"};
        double value = bytes;
        int unit = -1;
        while (value >= 1024 && unit < units.length - 1) {
            value /= 1024;
            unit++;
        }
        return String.format(Locale.ROOT, "%,d B (%.1f %s)", bytes, value, units[unit]);
    }

    // ---------------------------------------------------------------- arguments

    private static final class UsageException extends RuntimeException {
        UsageException(final String message) {
            super(message);
        }
    }

    private static final class Config {
        private Path input;
        private Source source = Source.TINKERGRAPH;
        private List<String> builders = List.of("heap");
        private SnapshotLayout layout = SnapshotLayout.FULL;
        private long memoryBudget = BuildOptions.DEFAULT_MEMORY_BUDGET_BYTES;
        private boolean groupedFastPath = true;
        private boolean edgeIdIndex = true;
        private Path outputDir;
        private Path scratchDir;
        private boolean verify;
        private boolean compare;

        static Config parse(final String[] args) {
            final Config config = new Config();
            boolean builderGiven = false;
            for (int i = 0; i < args.length; i++) {
                final String arg = args[i];
                switch (arg) {
                    case "--verify":
                        config.verify = true;
                        break;
                    case "--compare":
                        config.compare = true;
                        break;
                    case "--help":
                    case "-h":
                        throw new UsageException("CsrSnapshotBuildBenchmark");
                    default:
                        if (!arg.startsWith("--")) throw new UsageException("Unexpected argument: " + arg);
                        if (i + 1 >= args.length) throw new UsageException("Missing value for " + arg);
                        final String value = args[++i];
                        switch (arg) {
                            case "--input":
                                config.input = Paths.get(value).toAbsolutePath();
                                break;
                            case "--source":
                                try {
                                    config.source = Source.valueOf(value.toUpperCase(Locale.ROOT));
                                } catch (IllegalArgumentException e) {
                                    throw new UsageException("Unknown source: " + value);
                                }
                                break;
                            case "--builder":
                                builderGiven = true;
                                switch (value.toLowerCase(Locale.ROOT)) {
                                    case "heap":
                                        config.builders = List.of("heap");
                                        break;
                                    case "streaming":
                                        config.builders = List.of("streaming");
                                        break;
                                    case "both":
                                        config.builders = List.of("heap", "streaming");
                                        break;
                                    default:
                                        throw new UsageException("Unknown builder: " + value);
                                }
                                break;
                            case "--layout":
                                try {
                                    config.layout = SnapshotLayout.valueOf(value.toUpperCase(Locale.ROOT));
                                } catch (IllegalArgumentException e) {
                                    throw new UsageException("Unknown layout: " + value);
                                }
                                break;
                            case "--memory-budget":
                                config.memoryBudget = parseBytes(value);
                                break;
                            case "--grouped-fast-path":
                                config.groupedFastPath = parseBoolean(arg, value);
                                break;
                            case "--edge-id-index":
                                config.edgeIdIndex = parseBoolean(arg, value);
                                break;
                            case "--output-dir":
                                config.outputDir = Paths.get(value).toAbsolutePath();
                                break;
                            case "--scratch-dir":
                                config.scratchDir = Paths.get(value).toAbsolutePath();
                                break;
                            default:
                                throw new UsageException("Unknown option: " + arg);
                        }
                }
            }

            if (config.input == null) throw new UsageException("--input is required");
            if (!Files.isRegularFile(config.input)) throw new UsageException("Input file does not exist: " + config.input);
            if (config.compare) {
                if (builderGiven && config.builders.size() != 2)
                    throw new UsageException("--compare needs both builders; drop --builder or use --builder both");
                config.builders = List.of("heap", "streaming");
            }
            return config;
        }

        private static boolean parseBoolean(final String option, final String value) {
            if (value.equalsIgnoreCase("true")) return true;
            if (value.equalsIgnoreCase("false")) return false;
            throw new UsageException(option + " must be true or false but was " + value);
        }

        private static long parseBytes(final String value) {
            final String v = value.toLowerCase(Locale.ROOT);
            long multiplier = 1;
            String digits = v;
            if (v.endsWith("k")) multiplier = 1L << 10;
            else if (v.endsWith("m")) multiplier = 1L << 20;
            else if (v.endsWith("g")) multiplier = 1L << 30;
            if (multiplier != 1) digits = v.substring(0, v.length() - 1);
            try {
                final long bytes = Math.multiplyExact(Long.parseLong(digits), multiplier);
                if (bytes <= 0) throw new UsageException("--memory-budget must be positive");
                return bytes;
            } catch (NumberFormatException | ArithmeticException e) {
                throw new UsageException("Invalid --memory-budget: " + value);
            }
        }
    }
}
