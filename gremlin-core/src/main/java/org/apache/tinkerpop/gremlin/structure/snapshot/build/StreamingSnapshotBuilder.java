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
package org.apache.tinkerpop.gremlin.structure.snapshot.build;

import com.carrotsearch.hppc.LongIntHashMap;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.BuildDirectory;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnEncoding;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.IdentifierIndex;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.IdentifierIndexWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelCounts;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelDictionary;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelListWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.Manifest;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.MappedSegment;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.OwnerWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentHeader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentPaths;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SnapshotLayout;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueCodec;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueType;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.VertexPropertyColumnWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.EdgeScanOrder;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.PropertySource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.PropertyVisitor;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SnapshotSource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceVersion;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.UnsupportedSnapshotDataException;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexPropertySource;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.VertexPropertyVisitor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.Function;

/**
 * Builds a snapshot bundle with bounded heap. Provider records are copied straight into scratch spools and
 * memory-mapped segments, following the streaming construction steps of the spike document:
 * <ol>
 *     <li>the vertex scan assigns ordinals and spools labels, identifiers, identifier-index keys and property records;</li>
 *     <li>the vertex identifier index is produced by an external sort of the key spool, or by a copy when the keys
 *     arrived sorted;</li>
 *     <li>the edge scan resolves endpoints through that index, spools the same things for edges, appends the endpoint
 *     ordinals to the edge endpoint segments and counts degrees in memory-mapped arrays, and on the grouped fast path
 *     also appends the out-neighbors sequentially;</li>
 *     <li>the degree arrays are prefix-summed into the offset segments and converted into fill cursors;</li>
 *     <li>the endpoint segments are replayed to scatter the incoming adjacency, and on the generic path the outgoing
 *     adjacency as well, through the mapped cursors;</li>
 *     <li>labels are narrowed, the spools are compacted into columns, the edge index is produced, and the bundle is
 *     published.</li>
 * </ol>
 * The heap holds the label and property-key dictionaries, a small buffer per open spool, and at most one sort chunk,
 * all sized from {@link BuildOptions#memoryBudgetBytes()}. The output is byte-for-byte that of any other builder that
 * follows the determinism rules of the spike document, see the notes below.
 * <p/>
 * Choices the document leaves open, which other builders must share for identical output:
 * <ul>
 *     <li>Label codes and property-key codes are assigned in order of first appearance in the scan, in separate
 *     dictionaries for vertex labels, edge labels, vertex keys and edge keys. The keys of one element are assigned in
 *     the order the source visits them.</li>
 *     <li>{@code outEdgesImplicit} is true exactly when the source reports {@code GROUPED_BY_OUT_VERTEX}, whatever the
 *     layout and whatever {@link BuildOptions#groupedFastPath()} says. The option only selects the construction path.
 *     The monotonic out-vertex check applies to every build from a grouped source.</li>
 *     <li>Parallel edges are not merged. The entries of one vertex's adjacency are in ascending edge ordinal.</li>
 *     <li>Only what the layout publishes is read from the source: {@code TOPOLOGY} ignores labels, edge identifiers and
 *     properties, {@code IDENTITY} ignores properties, and ignored data is not validated.</li>
 *     <li>Dictionaries, identifier columns and the per-key column lists of the manifest are empty or null for the
 *     layouts that do not publish them. The exact-index flags are true exactly when the identifier column holds a single
 *     integral type, independent of whether the edge index is published.</li>
 *     <li>{@code edgeIdIndex} is true exactly for a non-{@code TOPOLOGY} layout with {@link BuildOptions#edgeIdIndex()},
 *     and duplicate edge identifiers are detected only when the edge index is built.</li>
 *     <li>The source version is read once, before the vertex scan. The source defaults are read after the scans and
 *     recorded for every layout. Variables are scanned, after the edges, only for the {@code FULL} layout.</li>
 *     <li>Vertex labels: the layout is {@code MULTI} exactly when some vertex has a label count other than one. The
 *     label dictionary is in order of first appearance, per vertex in source order.</li>
 *     <li>A vertex key is {@code MULTI} exactly when some vertex has more than one vertex property for it. Vertex-property
 *     ordinals of such a key are assigned in vertex order and source order within a vertex. A null value is counted as
 *     present. Meta keys are coded in order of first appearance, in vertex, vertex-property and meta order, in one
 *     dictionary for all vertex keys, and a meta column holds entries only for vertex properties that have the meta key;
 *     a repeated meta key on one vertex property is rejected. Variable keys are coded in {@code scanVariables} order and
 *     a repeated variable key is rejected.</li>
 * </ul>
 * Count-hint mismatches, order violations of a grouped scan and edges with unknown endpoints fail with
 * {@link IllegalStateException}. Unsupported data fails with {@link UnsupportedSnapshotDataException}.
 * <p/>
 * A builder created by {@link HybridSnapshotBuilder} runs the same steps with a storage policy: a byte-accounted
 * budget of {@link BuildOptions#memoryBudgetBytes()} decides which structures stay in heap (the spools, the identifier
 * lookup map, the sort of the index keys, the degree and fill arrays) and which live in scratch files as described
 * above. When a reservation does not fit, the largest spool in heap is moved to its file first. The output is the same.
 */
public final class StreamingSnapshotBuilder implements SnapshotBuilder {

    // vertex and edge ordinals are 32-bit and the largest allowed count is Integer.MAX_VALUE - 1
    private static final long MAX_ELEMENTS = Integer.MAX_VALUE - 1L;

    private static final String SPOOL_DIR = "spool/";
    private static final String DEGREE_DIR = "degree/";

    private final boolean hybrid;

    public StreamingSnapshotBuilder() {
        this(false);
    }

    StreamingSnapshotBuilder(final boolean hybrid) {
        this.hybrid = hybrid;
    }

    @Override
    public BuildStats build(final SnapshotSource source, final Path target, final BuildOptions options) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        Objects.requireNonNull(options);
        try (BuildDirectory dir = BuildDirectory.create(target, options.scratchDirectory().orElse(null))) {
            return new Build(source, dir, options, hybrid).run();
        }
    }

    /**
     * The spool and statistics of one property key.
     */
    private static final class KeyColumn {
        final PropertySpool spool;
        final ColumnStats values = new ColumnStats();
        // vertex-property identifiers; unused for edges
        final ColumnStats ids = new ColumnStats();
        int lastOrdinal = -1;
        // vertex properties only: the number of vertex properties, of vertices owning at least one, and whether some
        // vertex owns more than one
        long propertyCount;
        long ownerCount;
        boolean multi;
        // vertex properties only: the meta columns by meta key code
        final Map<Integer, MetaColumn> metas = new TreeMap<>();

        KeyColumn(final PropertySpool spool) {
            this.spool = spool;
        }
    }

    /**
     * The spool and statistics of one meta key of one vertex key. A record is the running index of the vertex property
     * among those of its key (int64), the owning vertex ordinal (int32) and the encoded value.
     */
    private static final class MetaColumn {
        final PropertySpool spool;
        final ColumnStats values = new ColumnStats();
        long lastIndex = -1;

        MetaColumn(final PropertySpool spool) {
            this.spool = spool;
        }
    }

    /**
     * The state of one build.
     */
    private static final class Build {
        private final boolean hybrid;
        private final SnapshotSource source;
        private final BuildDirectory dir;
        private final SnapshotLayout layout;

        // what the layout publishes
        private final boolean withIdentity;
        private final boolean withProperties;
        private final boolean edgeIndex;
        private final boolean grouped;
        private final boolean fastPath;

        private final long budget;
        private final int ioBuffer;
        private final int spoolBuffer;
        private final BuildBudget account;
        private final ExternalIndexSorter streamingSorter;
        private final Map<String, Long> timers = new LinkedHashMap<>();

        private final List<BuildStats.Phase> phases = new ArrayList<>();
        private final List<Manifest.SegmentInfo> segments = new ArrayList<>();
        private final List<AutoCloseable> open = new ArrayList<>();
        private final SpoolRecord record = new SpoolRecord();

        // vertex scan
        private int vertexCount;
        private final LabelDictionary vertexLabels = new LabelDictionary();
        private final LabelCounts vertexLabelHistogram = new LabelCounts();
        private final LabelCounts edgeLabelHistogram = new LabelCounts();
        private final LabelDictionary vertexKeys = new LabelDictionary();
        private final List<KeyColumn> vertexColumns = new ArrayList<>();
        private FixedSpool vertexLabelSpool;
        private FixedSpool vertexLabelCountSpool;
        private boolean multiLabels;
        private final LabelDictionary metaKeys = new LabelDictionary();
        private PropertySpool vertexIdSpool;
        private final ColumnStats vertexIdStats = new ColumnStats();
        private FixedSpool vertexKeySpool;
        private boolean vertexKeysSorted = true;
        private long lastVertexKey;

        // vertex index
        private Manifest.ColumnInfo vertexIdInfo;
        private IdentifierIndex vertexIndex;
        private long mapBytes;
        // hybrid: the lookup in heap, from index key to the first ordinal with it, chained through vertexNext for keys
        // that more than one identifier shares (non-integral identifiers and mixed integral types)
        private LongIntHashMap vertexMap;
        private int[] vertexNext;
        private ValueType vertexExact;
        private ColumnReader vertexIds;

        // edge scan
        private int edgeCount;
        private final LabelDictionary edgeLabels = new LabelDictionary();
        private final LabelDictionary edgeKeys = new LabelDictionary();
        private final List<KeyColumn> edgeColumns = new ArrayList<>();
        private FixedSpool edgeLabelSpool;
        private PropertySpool edgeIdSpool;
        private final ColumnStats edgeIdStats = new ColumnStats();
        private FixedSpool edgeKeySpool;
        private boolean edgeKeysSorted = true;
        private long lastEdgeKey;
        private SegmentWriter outVertices;
        private SegmentWriter inVertices;
        private SegmentWriter fastOutNeighbors;
        private IntTable outCursors;
        private IntTable inCursors;
        private long sourceNanos;
        private long callbackNanos;
        private long resolveNanos;
        private long lastExitNanos;
        private boolean haveLastOut;
        private int lastOutOrdinal;
        private ValueType lastOutType;
        private long lastOutBits;
        private Object lastOutId;
        private int lastGroupOrdinal = -1;
        private Manifest.ColumnInfo edgeIdInfo;

        // variables are few and held in memory
        private final LabelDictionary variableKeys = new LabelDictionary();
        private final List<Object> variableValues = new ArrayList<>();
        private final ColumnStats variableStats = new ColumnStats();
        private Manifest.ColumnInfo variablesInfo;

        private final VertexProperties vertexProperties = new VertexProperties();
        private final EdgeProperties edgeProperties = new EdgeProperties();

        Build(final SnapshotSource source, final BuildDirectory dir, final BuildOptions options, final boolean hybrid) {
            this.hybrid = hybrid;
            this.source = source;
            this.dir = dir;
            this.layout = options.layout();
            this.withIdentity = layout != SnapshotLayout.TOPOLOGY;
            this.withProperties = layout == SnapshotLayout.FULL;
            this.edgeIndex = withIdentity && options.edgeIdIndex();
            this.grouped = source.edgeScanOrder() == EdgeScanOrder.GROUPED_BY_OUT_VERTEX;
            this.fastPath = grouped && options.groupedFastPath();
            this.budget = options.memoryBudgetBytes();
            this.ioBuffer = (int) Math.max(64, Math.min(SegmentWriter.DEFAULT_BUFFER_BYTES, budget / 64));
            this.spoolBuffer = (int) Math.max(256, Math.min(32 * 1024, budget / 256));
            this.account = hybrid ? new BuildBudget(budget) : BuildBudget.none();
            this.streamingSorter = hybrid ? null : new ExternalIndexSorter(dir::scratchPath, budget);
        }

        // the streaming sorter is sized from the whole budget; in hybrid mode it gets what is left when it is needed
        private ExternalIndexSorter sorter() {
            if (!hybrid) return streamingSorter;
            return new ExternalIndexSorter(dir::scratchPath, Math.max(account.available(), budget / 4));
        }

        BuildStats run() {
            try {
                final SourceVersion version = source.version();

                long start = System.nanoTime();
                scanVertices();
                phase("vertex-scan", start);

                start = System.nanoTime();
                buildVertexIndex();
                phase("vertex-index", start);

                start = System.nanoTime();
                scanEdges();
                phase("edge-scan", start);

                start = System.nanoTime();
                computeOffsets();
                phase("offsets", start);

                start = System.nanoTime();
                replay();
                phase("replay", start);

                start = System.nanoTime();
                compact();
                phase("compaction", start);

                start = System.nanoTime();
                final Manifest manifest = new Manifest();
                fillManifest(manifest, version);
                closeAll();
                dir.deleteScratch();
                final long diskBytes = dir.bytesOnDisk();
                dir.publish(manifest);
                phases.add(new BuildStats.Phase("publish", System.nanoTime() - start, diskBytes));

                return new BuildStats(phases, segmentBytes(manifest), timers, account.peak());
            } finally {
                closeAll();
            }
        }

        // ------------------------------------------------------------ infrastructure

        private void phase(final String name, final long startNanos) {
            final long elapsed = System.nanoTime() - startNanos;
            phases.add(new BuildStats.Phase(name, elapsed, dir.bytesOnDisk()));
        }

        private <T extends AutoCloseable> T track(final T resource) {
            open.add(resource);
            return resource;
        }

        private void closeAll() {
            for (int i = open.size() - 1; i >= 0; i--) {
                try {
                    open.get(i).close();
                } catch (Exception ignored) {
                    // the build is failing, or the resource is already finished; the directory is cleaned up anyway
                }
            }
            open.clear();
        }

        private Function<String, Path> resolver(final boolean published) {
            return published ? dir::segmentPath : dir::scratchPath;
        }

        private Function<String, MappedSegment> opener(final boolean published) {
            final Path root = published ? dir.path() : dir.scratch();
            return relativePath -> MappedSegment.open(root.resolve(relativePath), MappedSegment.Mode.READ_ONLY);
        }

        private SegmentWriter writer(final boolean published, final String relativePath, final int width) {
            return track(SegmentWriter.create(resolver(published).apply(relativePath), width, ioBuffer));
        }

        private void publish(final SegmentWriter writer, final boolean published, final String relativePath) {
            writer.finish();
            if (published) segments.add(writer.info(relativePath));
        }

        private static void checkCount(final String kind, final OptionalLong hint, final long actual) {
            if (hint.isPresent() && hint.getAsLong() != actual) {
                throw new IllegalStateException("The source announced " + hint.getAsLong() + " " + kind
                        + " but emitted " + actual);
            }
        }

        private FixedSpool fixed(final String relativePath, final int width) {
            return track(new FixedSpool(dir.scratchPath(relativePath), width, ioBuffer, account));
        }

        private PropertySpool propertySpool(final String relativePath, final int bufferBytes, final boolean keepOpen) {
            return new PropertySpool(dir.scratchPath(relativePath), bufferBytes, keepOpen, account);
        }

        private KeyColumn column(final List<KeyColumn> columns, final int code, final String relativePath,
                                 final int bufferBytes) {
            if (code < columns.size()) return columns.get(code);
            if (code != columns.size()) throw new IllegalStateException("Property-key code " + code + " skipped");
            final KeyColumn column = new KeyColumn(propertySpool(relativePath, bufferBytes, false));
            columns.add(column);
            return column;
        }

        // ------------------------------------------------------------ step 1: vertex scan

        private void scanVertices() {
            if (withIdentity) {
                vertexLabelSpool = fixed(SPOOL_DIR + "vertex-labels.bin", 4);
                vertexLabelCountSpool = fixed(SPOOL_DIR + "vertex-label-counts.bin", 4);
            }
            vertexIdSpool = track(propertySpool(SPOOL_DIR + "vertex-ids.bin", ioBuffer, true));
            vertexKeySpool = fixed(SPOOL_DIR + "vertex-index-keys.bin", 8);
            lastExitNanos = System.nanoTime();
            source.scanVertices(this::onVertex);
            sourceNanos += System.nanoTime() - lastExitNanos;
            timers.put("vertex-scan.source", sourceNanos);
            timers.put("vertex-scan.callback", callbackNanos);
            sourceNanos = 0;
            callbackNanos = 0;
            checkCount("vertices", source.vertexCount(), vertexCount);
            if (withIdentity) {
                vertexLabelSpool.finish();
                vertexLabelCountSpool.finish();
            }
            vertexIdSpool.finish();
            vertexKeySpool.finish();
            for (final KeyColumn column : vertexColumns) {
                column.spool.finish();
                for (final MetaColumn meta : column.metas.values()) meta.spool.finish();
            }
        }

        private void onVertex(final Object id, final List<String> labels, final VertexPropertySource properties) {
            final long enter = System.nanoTime();
            sourceNanos += enter - lastExitNanos;
            addVertex(id, labels, properties);
            lastExitNanos = System.nanoTime();
            callbackNanos += lastExitNanos - enter;
        }

        private void addVertex(final Object id, final List<String> labels, final VertexPropertySource properties) {
            if (vertexCount >= MAX_ELEMENTS) {
                throw new UnsupportedSnapshotDataException("More than " + MAX_ELEMENTS + " vertices are not supported");
            }
            final int ordinal = vertexCount;
            if (withIdentity) {
                for (int i = 0; i < labels.size(); i++) {
                    final int code = vertexLabels.codeOf(labels.get(i));
                    vertexLabelSpool.writeInt(code);
                    vertexLabelHistogram.increment(code);
                }
                vertexLabelCountSpool.writeInt(labels.size());
                if (labels.size() != 1) multiLabels = true;
            }

            final ValueType type = ValueCodec.requireIdentifierType(id, "vertex identifier", null);
            record.reset();
            final int payload = record.putValue(type, id);
            final long key = IdentifierIndex.keyOf(type, record.bytes(), payload, record.length() - payload);
            vertexIdSpool.write(record);
            vertexIdStats.add(type);
            vertexKeySpool.writeLong(key);
            if (ordinal > 0 && key < lastVertexKey) vertexKeysSorted = false;
            lastVertexKey = key;

            if (withProperties) {
                vertexProperties.ordinal = ordinal;
                properties.forEach(vertexProperties);
            }
            vertexCount++;
        }

        private final class VertexProperties implements VertexPropertyVisitor {
            int ordinal;
            private final MetaProperties metaProperties = new MetaProperties();

            @Override
            public void vertexProperty(final Object id, final String key, final Object value,
                                       final PropertySource metas) {
                final int code = vertexKeys.codeOf(key);
                final ValueType idType = ValueCodec.requireIdentifierType(id, "vertex property identifier", key);
                final ValueType type = value == null ? null : ValueCodec.requireType(value, "vertex property", key);
                final KeyColumn column = column(vertexColumns, code, SPOOL_DIR + "vertex-property-" + code + ".bin",
                        spoolBuffer);
                if (column.lastOrdinal == ordinal) {
                    column.multi = true;
                } else {
                    column.lastOrdinal = ordinal;
                    column.ownerCount++;
                }
                final long index = column.propertyCount++;
                record.reset();
                record.putInt(ordinal);
                if (type == null) {
                    record.putNull();
                } else {
                    record.putValue(type, value);
                }
                record.putValue(idType, id);
                column.spool.write(record);
                if (type == null) {
                    column.values.addNull();
                } else {
                    column.values.add(type, value);
                }
                column.ids.add(idType);

                metaProperties.column = column;
                metaProperties.code = code;
                metaProperties.key = key;
                metaProperties.ordinal = ordinal;
                metaProperties.index = index;
                metas.forEach(metaProperties);
            }
        }

        private final class MetaProperties implements PropertyVisitor {
            KeyColumn column;
            int code;
            String key;
            int ordinal;
            long index;

            @Override
            public void property(final String metaKey, final Object value) {
                final int metaCode = metaKeys.codeOf(metaKey);
                final ValueType type = value == null ? null
                        : ValueCodec.requireType(value, "meta-property of '" + key + "'", metaKey);
                MetaColumn meta = column.metas.get(metaCode);
                if (meta == null) {
                    meta = new MetaColumn(propertySpool(SPOOL_DIR + "vertex-meta-" + code + "-" + metaCode + ".bin",
                            spoolBuffer, false));
                    column.metas.put(metaCode, meta);
                }
                if (meta.lastIndex == index) {
                    throw new UnsupportedSnapshotDataException("A vertex property '" + key + "' of vertex " + ordinal
                            + " has more than one value for meta key '" + metaKey + "'");
                }
                meta.lastIndex = index;
                record.reset();
                record.putLong(index);
                record.putInt(ordinal);
                if (type == null) {
                    record.putNull();
                    meta.values.addNull();
                } else {
                    record.putValue(type, value);
                    meta.values.add(type, value);
                }
                meta.spool.write(record);
            }
        }

        // ------------------------------------------------------------ step 2: vertex index

        private void buildVertexIndex() {
            vertexIdInfo = compactIds(SegmentPaths.VERTEX_IDS_DIR, vertexIdSpool, vertexCount, vertexIdStats);
            final ValueType exact = IdentifierIndex.exactType(vertexIdInfo);
            final ColumnReader ids = exact == null ? openIds(SegmentPaths.VERTEX_IDS_DIR, vertexCount, vertexIdInfo) : null;
            final ExternalIndexSorter.EntrySink tap = hybrid ? reserveLookupMap(exact, ids) : null;
            writeIndex("vertex", vertexKeySpool, vertexKeysSorted, vertexCount, vertexIdInfo, ids, withIdentity,
                    SegmentPaths.VERTEX_ID_INDEX_KEYS, SegmentPaths.VERTEX_ID_INDEX_ORDINALS, "vertex", tap);
            vertexKeySpool.release();
            if (vertexMap == null) {
                final Function<String, MappedSegment> opener = opener(withIdentity);
                vertexIndex = IdentifierIndex.of(opener.apply(SegmentPaths.VERTEX_ID_INDEX_KEYS),
                        opener.apply(SegmentPaths.VERTEX_ID_INDEX_ORDINALS), exact, ids);
            }
        }

        /**
         * Hybrid mode: reserves the heap for the lookup map and returns the sink that fills it from the sorted index
         * entries, or null when the map does not fit and the mapped index serves the lookups. Integral identifiers are
         * keyed by value. Other identifiers are keyed by the 64-bit key hash and chained, and a candidate is checked
         * against the encoded identifier in the identifier column.
         */
        private ExternalIndexSorter.EntrySink reserveLookupMap(final ValueType exact, final ColumnReader ids) {
            final long n = vertexCount;
            // hppc sizes its arrays to the next power of two of n / loadFactor, plus a sentinel slot
            final long capacity = Long.highestOneBit(Math.max(4, (long) Math.ceil(n / 0.75)) - 1) << 1;
            if (capacity > (1L << 30)) return null;
            final long bytes = (capacity + 1) * (Long.BYTES + Integer.BYTES) + (exact == null ? n * Integer.BYTES : 0);
            if (!account.reserve(bytes)) return null;
            mapBytes = bytes;
            vertexExact = exact;
            vertexIds = ids;
            vertexMap = new LongIntHashMap((int) n);
            if (exact != null) return (key, ordinal) -> vertexMap.put(key, ordinal);
            vertexNext = new int[(int) n];
            return (key, ordinal) -> {
                vertexNext[ordinal] = vertexMap.getOrDefault(key, -1);
                vertexMap.put(key, ordinal);
            };
        }

        private int lookupVertex(final Object id) {
            if (vertexMap == null) return vertexIndex.lookup(id);
            final ValueType type = ValueCodec.typeOf(id);
            if (type == null) return -1;
            if (vertexExact != null) {
                return type != vertexExact ? -1 : vertexMap.getOrDefault(((Number) id).longValue(), -1);
            }
            final byte[] encoded = ValueCodec.encode(type, id);
            final long key = IdentifierIndex.keyOf(type, encoded, 0, encoded.length);
            for (int ordinal = vertexMap.getOrDefault(key, -1); ordinal >= 0; ordinal = vertexNext[ordinal]) {
                if (vertexIds.matches(ordinal, type, encoded)) return ordinal;
            }
            return -1;
        }

        /**
         * Compacts an identifier spool into the identifier column, published when the layout has identity.
         */
        private Manifest.ColumnInfo compactIds(final String columnDir, final PropertySpool spool, final long count,
                                               final ColumnStats stats) {
            final Manifest.ColumnInfo info = stats.toInfo(count);
            try (ColumnWriter writer = ColumnWriter.create(resolver(withIdentity), columnDir, count, info);
                 PropertySpoolReader in = spool.reader(ioBuffer)) {
                final PropertySpoolReader.Payload payload = new PropertySpoolReader.Payload();
                for (long ordinal = 0; ordinal < count; ordinal++) {
                    final ValueType type = in.readType();
                    in.readPayload(type, payload);
                    writer.appendEncoded(ordinal, type, payload.bytes, 0, payload.length);
                }
                final List<Manifest.SegmentInfo> written = writer.finish();
                if (withIdentity) segments.addAll(written);
            }
            spool.release();
            return info;
        }

        private ColumnReader openIds(final String columnDir, final long count, final Manifest.ColumnInfo info) {
            return track(ColumnReader.open(opener(withIdentity), columnDir, count, info));
        }

        /**
         * Writes an identifier index from the key spool, whose entry positions are the ordinals, either by copying the
         * spool when it is already in index order or by an external sort. The index is published only when asked.
         */
        private void writeIndex(final String kind, final FixedSpool keySpool, final boolean sorted, final long count,
                                final Manifest.ColumnInfo idInfo, final ColumnReader ids, final boolean published,
                                final String keysPath, final String ordinalsPath, final String sortName,
                                final ExternalIndexSorter.EntrySink tap) {
            final Function<String, Path> resolver = resolver(published);
            try (IdentifierIndexWriter writer = IdentifierIndexWriter.create(resolver.apply(keysPath),
                    resolver.apply(ordinalsPath), kind, count, IdentifierIndex.exactType(idInfo), ids)) {
                // the entries reach the sink in index order, the writer first so that a duplicate fails before the tap
                final ExternalIndexSorter.EntrySink sink = tap == null ? writer::add : (key, ordinal) -> {
                    writer.add(key, ordinal);
                    tap.accept(key, ordinal);
                };
                // the key, the ordinal and the two copies that the stable in-heap sort needs
                final long sortBytes = 24L * count;
                if (sorted) {
                    try (FixedSpool.Reader in = keySpool.reader(ioBuffer)) {
                        for (long ordinal = 0; ordinal < count; ordinal++) sink.accept(in.readLong(), (int) ordinal);
                    }
                } else if (hybrid && count <= MAX_ELEMENTS && account.reserve(sortBytes)) {
                    try {
                        final int n = (int) count;
                        final long[] keys = new long[n];
                        final int[] ordinals = new int[n];
                        try (FixedSpool.Reader in = keySpool.reader(ioBuffer)) {
                            for (int i = 0; i < n; i++) {
                                keys[i] = in.readLong();
                                ordinals[i] = i;
                            }
                        }
                        keySpool.release();
                        IdentifierIndex.sortEntries(keys, ordinals, n);
                        for (int i = 0; i < n; i++) sink.accept(keys[i], ordinals[i]);
                    } finally {
                        account.release(sortBytes);
                    }
                } else {
                    sorter().sort(keySpool.materialize(), sortName, sink);
                }
                final List<Manifest.SegmentInfo> written = writer.finish(keysPath, ordinalsPath);
                if (published) segments.addAll(written);
            }
        }

        // ------------------------------------------------------------ step 3: edge scan

        private void scanEdges() {
            if (withIdentity) {
                edgeLabelSpool = fixed(SPOOL_DIR + "edge-labels.bin", 4);
                edgeIdSpool = track(propertySpool(SPOOL_DIR + "edge-ids.bin", ioBuffer, true));
            }
            if (edgeIndex) edgeKeySpool = fixed(SPOOL_DIR + "edge-index-keys.bin", 8);
            outVertices = writer(withIdentity, SegmentPaths.EDGE_OUT_VERTICES, 4);
            inVertices = writer(withIdentity, SegmentPaths.EDGE_IN_VERTICES, 4);
            if (fastPath) fastOutNeighbors = writer(true, SegmentPaths.OUT_NEIGHBORS, 4);
            outCursors = track(IntTable.allocate(account, vertexCount, dir.scratchPath(DEGREE_DIR + "out.bin")));
            inCursors = track(IntTable.allocate(account, vertexCount, dir.scratchPath(DEGREE_DIR + "in.bin")));

            lastExitNanos = System.nanoTime();
            source.scanEdges(this::onEdge);
            sourceNanos += System.nanoTime() - lastExitNanos;
            timers.put("edge-scan.source", sourceNanos);
            timers.put("edge-scan.callback", callbackNanos);
            timers.put("edge-scan.resolve", resolveNanos);
            if (vertexMap != null) {
                // the endpoints are resolved; the map is not needed again
                vertexMap = null;
                vertexNext = null;
                account.release(mapBytes);
            }
            checkCount("edges", source.edgeCount(), edgeCount);
            if (withProperties) source.scanVariables(this::onVariable);

            if (withIdentity) {
                edgeLabelSpool.finish();
                edgeIdSpool.finish();
            }
            if (edgeIndex) edgeKeySpool.finish();
            for (final KeyColumn column : edgeColumns) column.spool.finish();
            publish(outVertices, withIdentity, SegmentPaths.EDGE_OUT_VERTICES);
            publish(inVertices, withIdentity, SegmentPaths.EDGE_IN_VERTICES);
            if (fastPath) publish(fastOutNeighbors, true, SegmentPaths.OUT_NEIGHBORS);
        }

        private void onEdge(final Object id, final String label, final Object outId, final Object inId,
                            final PropertySource properties) {
            final long enter = System.nanoTime();
            sourceNanos += enter - lastExitNanos;
            addEdge(id, label, outId, inId, properties);
            lastExitNanos = System.nanoTime();
            callbackNanos += lastExitNanos - enter;
        }

        private void addEdge(final Object id, final String label, final Object outId, final Object inId,
                             final PropertySource properties) {
            if (edgeCount >= MAX_ELEMENTS) {
                throw new UnsupportedSnapshotDataException("More than " + MAX_ELEMENTS + " edges are not supported");
            }
            final int ordinal = edgeCount;
            if (withIdentity) {
                final int labelCode = edgeLabels.codeOf(label);
                edgeLabelSpool.writeInt(labelCode);
                edgeLabelHistogram.increment(labelCode);
                final ValueType type = ValueCodec.requireIdentifierType(id, "edge identifier", null);
                record.reset();
                final int payload = record.putValue(type, id);
                edgeIdSpool.write(record);
                edgeIdStats.add(type);
                if (edgeIndex) {
                    final long key = IdentifierIndex.keyOf(type, record.bytes(), payload, record.length() - payload);
                    edgeKeySpool.writeLong(key);
                    if (ordinal > 0 && key < lastEdgeKey) edgeKeysSorted = false;
                    lastEdgeKey = key;
                }
            }

            final long resolveStart = System.nanoTime();
            final int out = resolveOut(outId, ordinal);
            if (grouped) {
                if (out < lastGroupOrdinal) {
                    throw new IllegalStateException("The source reports GROUPED_BY_OUT_VERTEX but edge " + ordinal
                            + " has out-vertex ordinal " + out + " after " + lastGroupOrdinal);
                }
                lastGroupOrdinal = out;
            }
            final int in = lookupVertex(inId);
            resolveNanos += System.nanoTime() - resolveStart;
            if (in < 0) {
                throw new IllegalStateException("Edge " + ordinal + " refers to unknown in-vertex " + inId);
            }

            outVertices.writeInt(out);
            inVertices.writeInt(in);
            outCursors.put(out, outCursors.get(out) + 1);
            inCursors.put(in, inCursors.get(in) + 1);
            if (fastPath) fastOutNeighbors.writeInt(in);

            if (withProperties) {
                edgeProperties.ordinal = ordinal;
                properties.forEach(edgeProperties);
            }
            edgeCount++;
        }

        private void onVariable(final String key, final Object value) {
            final int before = variableKeys.size();
            variableKeys.codeOf(key);
            if (variableKeys.size() == before) {
                throw new UnsupportedSnapshotDataException("Variable '" + key + "' is emitted more than once");
            }
            final ValueType type = value == null ? null : ValueCodec.requireType(value, "variable", key);
            variableValues.add(value);
            if (type == null) {
                variableStats.addNull();
            } else {
                variableStats.add(type);
            }
        }

        /**
         * Resolves the out-vertex, reusing the previous ordinal while the identifier repeats. A fixed-width identifier is
         * remembered by its bits. A variable-width one is remembered by reference, which is safe for the immutable value
         * classes the format supports.
         */
        private int resolveOut(final Object id, final int edgeOrdinal) {
            final ValueType type = ValueCodec.typeOf(id);
            if (haveLastOut && type != null && type == lastOutType) {
                final boolean same = type.isFixedWidth() ? ValueCodec.fixedBits(type, id) == lastOutBits
                        : id.equals(lastOutId);
                if (same) return lastOutOrdinal;
            }
            final int ordinal = lookupVertex(id);
            if (ordinal < 0) {
                throw new IllegalStateException("Edge " + edgeOrdinal + " refers to unknown out-vertex " + id);
            }
            haveLastOut = true;
            lastOutOrdinal = ordinal;
            lastOutType = type;
            if (type.isFixedWidth()) {
                lastOutBits = ValueCodec.fixedBits(type, id);
                lastOutId = null;
            } else {
                lastOutId = id;
            }
            return ordinal;
        }

        private final class EdgeProperties implements PropertyVisitor {
            int ordinal;

            @Override
            public void property(final String key, final Object value) {
                final int code = edgeKeys.codeOf(key);
                final ValueType type = value == null ? null : ValueCodec.requireType(value, "edge property", key);
                final KeyColumn column = column(edgeColumns, code, SPOOL_DIR + "edge-property-" + code + ".bin",
                        spoolBuffer);
                if (column.lastOrdinal == ordinal) {
                    throw new UnsupportedSnapshotDataException("Edge " + ordinal + " has more than one value for key '"
                            + key + "'");
                }
                column.lastOrdinal = ordinal;
                record.reset();
                record.putInt(ordinal);
                if (type == null) {
                    record.putNull();
                    column.values.addNull();
                } else {
                    record.putValue(type, value);
                    column.values.add(type, value);
                }
                column.spool.write(record);
            }
        }

        // ------------------------------------------------------------ step 4: offsets

        /**
         * Prefix-sums the degree arrays into the offset segments and converts each degree array in place into the fill
         * cursors, which start at the offset of their vertex.
         */
        private void computeOffsets() {
            final SegmentWriter outOffsets = writer(true, SegmentPaths.OUT_OFFSETS, 8);
            final SegmentWriter inOffsets = writer(true, SegmentPaths.IN_OFFSETS, 8);
            long outRunning = 0;
            long inRunning = 0;
            for (int v = 0; v < vertexCount; v++) {
                outOffsets.writeLong(outRunning);
                inOffsets.writeLong(inRunning);
                final int outDegree = outCursors.get(v);
                final int inDegree = inCursors.get(v);
                outCursors.put(v, (int) outRunning);
                inCursors.put(v, (int) inRunning);
                outRunning += outDegree;
                inRunning += inDegree;
            }
            if (outRunning != edgeCount || inRunning != edgeCount) {
                throw new IllegalStateException("Degrees sum to " + outRunning + " and " + inRunning + " for "
                        + edgeCount + " edges");
            }
            outOffsets.writeLong(outRunning);
            inOffsets.writeLong(inRunning);
            publish(outOffsets, true, SegmentPaths.OUT_OFFSETS);
            publish(inOffsets, true, SegmentPaths.IN_OFFSETS);
        }

        // ------------------------------------------------------------ step 5: replay

        private void replay() {
            final boolean outEdges = withIdentity && !grouped;
            final IntTable inNeighbors = fill(SegmentPaths.IN_NEIGHBORS);
            final IntTable inEdges = withIdentity ? fill(SegmentPaths.IN_EDGES) : null;
            final IntTable outNeighbors = fastPath ? null : fill(SegmentPaths.OUT_NEIGHBORS);
            final IntTable outEdgeSegment = !fastPath && outEdges ? fill(SegmentPaths.OUT_EDGES) : null;

            try (SegmentReader outReader = SegmentReader.open(resolver(withIdentity).apply(SegmentPaths.EDGE_OUT_VERTICES),
                    ioBuffer);
                 SegmentReader inReader = SegmentReader.open(resolver(withIdentity).apply(SegmentPaths.EDGE_IN_VERTICES),
                         ioBuffer)) {
                for (int edge = 0; edge < edgeCount; edge++) {
                    final int out = outReader.readInt();
                    final int in = inReader.readInt();
                    final int inPosition = inCursors.get(in);
                    inCursors.put(in, inPosition + 1);
                    inNeighbors.put(inPosition, out);
                    if (inEdges != null) inEdges.put(inPosition, edge);
                    if (!fastPath) {
                        final int outPosition = outCursors.get(out);
                        outCursors.put(out, outPosition + 1);
                        outNeighbors.put(outPosition, in);
                        if (outEdgeSegment != null) outEdgeSegment.put(outPosition, edge);
                    }
                }
            }

            // the cursors are spent
            outCursors.close();
            inCursors.close();
            publish(inNeighbors, SegmentPaths.IN_NEIGHBORS);
            if (inEdges != null) publish(inEdges, SegmentPaths.IN_EDGES);
            if (outNeighbors != null) publish(outNeighbors, SegmentPaths.OUT_NEIGHBORS);
            if (outEdgeSegment != null) publish(outEdgeSegment, SegmentPaths.OUT_EDGES);
        }

        // an adjacency array of the edge count: in heap while the budget allows, else the mapped output segment
        private IntTable fill(final String relativePath) {
            return track(IntTable.allocate(account, edgeCount, dir.segmentPath(relativePath)));
        }

        private void publish(final IntTable table, final String relativePath) {
            segments.add(table.publish(dir.segmentPath(relativePath), relativePath, ioBuffer));
            table.close();
        }

        // ------------------------------------------------------------ step 6: compaction

        private final List<Manifest.ColumnInfo> vertexValueInfos = new ArrayList<>();
        private final List<Manifest.ColumnInfo> vertexIdColumnInfos = new ArrayList<>();
        private final List<Manifest.ColumnInfo> edgeValueInfos = new ArrayList<>();
        private final List<Manifest.VertexKeyInfo> vertexKeyInfos = new ArrayList<>();

        private void compact() {
            if (withIdentity) {
                compactVertexLabels();
                edgeIdInfo = compactIds(SegmentPaths.EDGE_IDS_DIR, edgeIdSpool, edgeCount, edgeIdStats);
                if (edgeIndex) {
                    final ValueType exact = IdentifierIndex.exactType(edgeIdInfo);
                    final ColumnReader ids = exact == null
                            ? openIds(SegmentPaths.EDGE_IDS_DIR, edgeCount, edgeIdInfo) : null;
                    writeIndex("edge", edgeKeySpool, edgeKeysSorted, edgeCount, edgeIdInfo, ids, true,
                            SegmentPaths.EDGE_ID_INDEX_KEYS, SegmentPaths.EDGE_ID_INDEX_ORDINALS, "edge", null);
                    edgeKeySpool.release();
                }
                segments.add(narrow(edgeLabelSpool, SegmentPaths.EDGE_LABELS, edgeLabels.size()));
                edgeLabelSpool.release();
            }
            if (withProperties) {
                compactVertexProperties();
                compactEdgeProperties();
                compactVariables();
            }
        }

        /**
         * Narrows the label code spool when every vertex has one label, and otherwise replays it with the per-vertex
         * counts into the label lists.
         */
        private void compactVertexLabels() {
            if (!multiLabels) {
                segments.add(narrow(vertexLabelSpool, SegmentPaths.VERTEX_LABELS, vertexLabels.size()));
                vertexLabelSpool.release();
                vertexLabelCountSpool.release();
                return;
            }
            try (LabelListWriter writer = LabelListWriter.create(dir::segmentPath, vertexLabels.size());
                 FixedSpool.Reader codes = vertexLabelSpool.reader(ioBuffer);
                 FixedSpool.Reader counts = vertexLabelCountSpool.reader(ioBuffer)) {
                for (int v = 0; v < vertexCount; v++) {
                    final int count = counts.readInt();
                    for (int i = 0; i < count; i++) writer.add(codes.readInt());
                    writer.endVertex();
                }
                segments.addAll(writer.finish(vertexCount));
            }
            vertexLabelSpool.release();
            vertexLabelCountSpool.release();
        }

        /**
         * Converts a spool of int32 label codes into a {@code labels.bin} of the narrowest width for the final
         * dictionary, as {@link LabelDictionary#narrow}, reading from heap when the spool is there.
         */
        private Manifest.SegmentInfo narrow(final FixedSpool spool, final String relativePath, final int labelCount) {
            try (FixedSpool.Reader in = spool.reader(ioBuffer);
                 SegmentWriter out = LabelDictionary.createCodeWriter(dir.segmentPath(relativePath), labelCount)) {
                final long n = spool.count();
                for (long i = 0; i < n; i++) {
                    final int code = in.readInt();
                    if (code < 0 || code >= labelCount) {
                        throw new IllegalStateException("Label code " + code + " at index " + i + " of " + relativePath
                                + " is outside the dictionary of " + labelCount + " labels");
                    }
                    LabelDictionary.writeCode(out, code);
                }
                out.finish();
                return out.info(relativePath);
            }
        }

        private void compactVertexProperties() {
            final PropertySpoolReader.Payload value = new PropertySpoolReader.Payload();
            final PropertySpoolReader.Payload id = new PropertySpoolReader.Payload();
            for (int code = 0; code < vertexColumns.size(); code++) {
                final KeyColumn column = vertexColumns.get(code);
                final boolean multi = column.multi;
                // in the multi layout the element space is the key's vertex properties
                final long elementCount = multi ? column.propertyCount : vertexCount;
                final Manifest.ColumnInfo valueInfo = column.values.toInfo(elementCount);
                final Manifest.ColumnInfo idInfo = column.ids.toInfo(elementCount);
                try (VertexPropertyColumnWriter writer = VertexPropertyColumnWriter.create(dir::segmentPath, code,
                        elementCount, valueInfo, idInfo);
                     OwnerWriter owners = multi ? OwnerWriter.create(dir::segmentPath, code, vertexCount,
                             column.ownerCount) : null;
                     PropertySpoolReader in = column.spool.reader(ioBuffer)) {
                    long index = 0;
                    int owner = -1;
                    long ownerProperties = 0;
                    while (in.hasRemaining()) {
                        final int ordinal = in.readInt();
                        final ValueType valueType = in.readType();
                        if (valueType != ValueType.NULL) in.readPayload(valueType, value);
                        final ValueType idType = in.readType();
                        in.readPayload(idType, id);
                        if (multi && ordinal != owner) {
                            if (ownerProperties > 0) owners.owner(owner, ownerProperties);
                            owner = ordinal;
                            ownerProperties = 0;
                        }
                        ownerProperties++;
                        final long position = multi ? index : ordinal;
                        if (valueType == ValueType.NULL) {
                            writer.appendNullValueEncoded(position, idType, id.bytes, 0, id.length);
                        } else {
                            writer.appendEncoded(position, idType, id.bytes, 0, id.length, valueType, value.bytes, 0,
                                    value.length);
                        }
                        index++;
                    }
                    segments.addAll(writer.finish());
                    if (multi) {
                        if (ownerProperties > 0) owners.owner(owner, ownerProperties);
                        segments.addAll(owners.finish(column.propertyCount));
                    }
                }
                column.spool.release();
                vertexValueInfos.add(valueInfo);
                vertexIdColumnInfos.add(idInfo);

                // a meta column is indexed by vertex property, which in the single layout is the vertex ordinal when
                // the parent column is dense and the entry index otherwise
                final boolean byVertex = !multi && (valueInfo.encoding() == ColumnEncoding.DENSE_FIXED
                        || valueInfo.encoding() == ColumnEncoding.DENSE_VARIABLE);
                final long metaElements = byVertex ? vertexCount : column.propertyCount;
                final Map<Integer, Manifest.ColumnInfo> metaInfos = new LinkedHashMap<>();
                for (final Map.Entry<Integer, MetaColumn> entry : column.metas.entrySet()) {
                    metaInfos.put(entry.getKey(), compactMeta(code, entry.getKey(), entry.getValue(), metaElements,
                            byVertex));
                }
                vertexKeyInfos.add(new Manifest.VertexKeyInfo(
                        multi ? Manifest.PropertyLayout.MULTI : Manifest.PropertyLayout.SINGLE,
                        multi ? OwnerWriter.encodingOf(vertexCount, column.ownerCount) : null, column.propertyCount,
                        metaInfos));
            }
        }

        private Manifest.ColumnInfo compactMeta(final int keyCode, final int metaCode, final MetaColumn meta,
                                                final long elementCount, final boolean byVertex) {
            final PropertySpoolReader.Payload value = new PropertySpoolReader.Payload();
            final Manifest.ColumnInfo info = meta.values.toInfo(elementCount);
            try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath,
                    SegmentPaths.vertexPropertyMetaDir(keyCode, metaCode), elementCount, info);
                 PropertySpoolReader in = meta.spool.reader(ioBuffer)) {
                while (in.hasRemaining()) {
                    final long index = in.readLong();
                    final int ordinal = in.readInt();
                    final ValueType type = in.readType();
                    final long position = byVertex ? ordinal : index;
                    if (type == ValueType.NULL) {
                        writer.appendNull(position);
                    } else {
                        in.readPayload(type, value);
                        writer.appendEncoded(position, type, value.bytes, 0, value.length);
                    }
                }
                segments.addAll(writer.finish());
            }
            meta.spool.release();
            return info;
        }

        private void compactEdgeProperties() {
            final PropertySpoolReader.Payload value = new PropertySpoolReader.Payload();
            for (int code = 0; code < edgeColumns.size(); code++) {
                final KeyColumn column = edgeColumns.get(code);
                final Manifest.ColumnInfo info = column.values.toInfo(edgeCount);
                try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath, SegmentPaths.edgePropertyDir(code),
                        edgeCount, info);
                     PropertySpoolReader in = column.spool.reader(ioBuffer)) {
                    while (in.hasRemaining()) {
                        final int ordinal = in.readInt();
                        final ValueType type = in.readType();
                        if (type == ValueType.NULL) {
                            writer.appendNull(ordinal);
                        } else {
                            in.readPayload(type, value);
                            writer.appendEncoded(ordinal, type, value.bytes, 0, value.length);
                        }
                    }
                    segments.addAll(writer.finish());
                }
                column.spool.release();
                edgeValueInfos.add(info);
            }
        }

        private void compactVariables() {
            final int count = variableValues.size();
            if (count == 0) return;
            variablesInfo = variableStats.toInfo(count);
            try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath, SegmentPaths.VARIABLES_DIR, count,
                    variablesInfo)) {
                for (int code = 0; code < count; code++) writer.append(code, variableValues.get(code));
                segments.addAll(writer.finish());
            }
        }

        // ------------------------------------------------------------ manifest

        private void fillManifest(final Manifest manifest, final SourceVersion version) {
            manifest.setSourceId(version.sourceId());
            manifest.setSourceVersion(version.version());
            manifest.setLayout(layout);
            manifest.setVertexCount(vertexCount);
            manifest.setEdgeCount(edgeCount);
            if (withIdentity) {
                manifest.setVertexLabels(new ArrayList<>(vertexLabels.labels()));
                manifest.setEdgeLabels(new ArrayList<>(edgeLabels.labels()));
                manifest.setVertexLabelCounts(vertexLabelHistogram.toArray(vertexLabels.size()));
                manifest.setEdgeLabelCounts(edgeLabelHistogram.toArray(edgeLabels.size()));
                manifest.setVertexLabelLayout(multiLabels ? Manifest.VertexLabelLayout.MULTI
                        : Manifest.VertexLabelLayout.SINGLE);
                manifest.setVertexIds(vertexIdInfo);
                manifest.setEdgeIds(edgeIdInfo);
                manifest.setVertexIdIndexExact(IdentifierIndex.exactType(vertexIdInfo) != null);
                manifest.setEdgeIdIndexExact(IdentifierIndex.exactType(edgeIdInfo) != null);
            }
            if (withProperties) {
                manifest.setVertexKeys(new ArrayList<>(vertexKeys.labels()));
                manifest.setEdgeKeys(new ArrayList<>(edgeKeys.labels()));
                manifest.setVertexProperties(vertexValueInfos);
                manifest.setVertexPropertyIds(vertexIdColumnInfos);
                manifest.setEdgeProperties(edgeValueInfos);
                manifest.setVertexKeyInfos(vertexKeyInfos);
                manifest.setMetaKeys(new ArrayList<>(metaKeys.labels()));
                manifest.setVariableKeys(new ArrayList<>(variableKeys.labels()));
                manifest.setVariables(variablesInfo);
            }
            manifest.setSourceDefaults(source.defaults());
            manifest.setOutEdgesImplicit(grouped);
            manifest.setEdgeIdIndex(edgeIndex);
            final List<Manifest.SegmentInfo> sorted = new ArrayList<>(segments);
            sorted.sort(Comparator.comparing(Manifest.SegmentInfo::path));
            manifest.setSegments(sorted);
        }

        private static Map<String, Long> segmentBytes(final Manifest manifest) {
            final Map<String, Long> bytes = new LinkedHashMap<>();
            for (final Manifest.SegmentInfo s : manifest.getSegments()) {
                bytes.put(s.path(), SegmentHeader.SIZE + s.width() * s.count());
            }
            return bytes;
        }
    }
}
