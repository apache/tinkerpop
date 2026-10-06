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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.ObjectIntHashMap;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.BuildDirectory;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnStats;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.IdentifierIndex;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.IdentifierIndexWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelCounts;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelDictionary;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelListWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.Manifest;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.OwnerWriter;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentPaths;
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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * The simple baseline snapshot builder and the reference output for the streaming builder. It reads the whole source
 * into heap structures, builds both adjacency directions in heap and then writes every segment sequentially through
 * the shared column, index and segment writers of the {@code format} package.
 * <p/>
 * The heap use is proportional to the size of the graph, in particular to the number of properties, because the
 * identifiers and property values are retained as objects until they are written. The rules that decide the bytes of
 * the output are:
 * <ul>
 *     <li>vertex and edge ordinals follow the scan order;</li>
 *     <li>label codes and property-key codes are assigned in order of first appearance, by separate dictionaries for
 *     vertex labels, edge labels, vertex keys, edge keys, meta-property keys and variable keys. Only the layouts that
 *     publish them build them;</li>
 *     <li>vertex labels use the multi-label layout exactly when some vertex has a label count other than one, and a
 *     vertex key uses the multi-property layout exactly when some vertex has more than one vertex property for it;</li>
 *     <li>within one vertex, adjacency entries are in ascending edge ordinal;</li>
 *     <li>the out-edge segment is omitted, and the manifest flag {@code outEdgesImplicit} set, exactly when the
 *     source reports {@link EdgeScanOrder#GROUPED_BY_OUT_VERTEX}, whatever the value of
 *     {@link BuildOptions#groupedFastPath()}, which has no effect on this builder.</li>
 * </ul>
 * Property entries are only read for {@link SnapshotLayout#FULL}; the other layouts do not visit them. Instances are
 * stateless and may be reused, but each build is single-threaded.
 */
public final class HeapSnapshotBuilder implements SnapshotBuilder {

    // ordinals are 32-bit, so at most Integer.MAX_VALUE - 1 vertices or edges are accepted
    private static final int MAX_ELEMENTS = Integer.MAX_VALUE - 1;

    @Override
    public BuildStats build(final SnapshotSource source, final Path target, final BuildOptions options) {
        Objects.requireNonNull(source);
        Objects.requireNonNull(target);
        Objects.requireNonNull(options);
        try (BuildDirectory dir = BuildDirectory.create(target, options.scratchDirectory().orElse(null))) {
            return new Run(source, dir, options).run();
        }
    }

    /**
     * The state of one build.
     */
    private static final class Run {
        private final SnapshotSource source;
        private final BuildDirectory dir;
        private final BuildOptions options;
        private final SnapshotLayout layout;
        private final boolean identity;
        private final boolean full;
        private final boolean grouped;
        private final boolean edgeIdIndex;

        private final List<BuildStats.Phase> phases = new ArrayList<>();
        private final List<Manifest.SegmentInfo> segments = new ArrayList<>();
        private long phaseStart;

        // vertices
        private int vertexCount;
        private ObjectIntHashMap<Object> vertexOrdinals = new ObjectIntHashMap<>();
        private final List<Object> vertexIds = new ArrayList<>();
        private final LongArrayList vertexIndexKeys = new LongArrayList();
        private final ColumnStats vertexIdStats = new ColumnStats();
        private final LabelDictionary vertexLabels = new LabelDictionary();
        private final IntArrayList vertexLabelCodes = new IntArrayList();
        private final IntArrayList vertexLabelCounts = new IntArrayList();
        private final LabelCounts vertexLabelHistogram = new LabelCounts();
        private final LabelCounts edgeLabelHistogram = new LabelCounts();
        private boolean multiLabels;
        private final LabelDictionary vertexKeys = new LabelDictionary();
        private final LabelDictionary metaKeys = new LabelDictionary();
        private final List<PropertyEntries> vertexProperties = new ArrayList<>();

        // edges
        private int edgeCount;
        private final IntArrayList edgeOut = new IntArrayList();
        private final IntArrayList edgeIn = new IntArrayList();
        private final List<Object> edgeIds = new ArrayList<>();
        private final LongArrayList edgeIndexKeys = new LongArrayList();
        private final ColumnStats edgeIdStats = new ColumnStats();
        private final LabelDictionary edgeLabels = new LabelDictionary();
        private final IntArrayList edgeLabelCodes = new IntArrayList();
        private final LabelDictionary edgeKeys = new LabelDictionary();
        private final List<PropertyEntries> edgeProperties = new ArrayList<>();
        private int lastOutOrdinal = -1;
        private Object lastOutId;
        private int lastOutResolved;

        // variables
        private final LabelDictionary variableKeys = new LabelDictionary();
        private final List<Object> variableValues = new ArrayList<>();
        private final ColumnStats variableStats = new ColumnStats();

        // adjacency
        private long[] outOffsets;
        private long[] inOffsets;
        private int[] outNeighbors;
        private int[] outEdges;
        private int[] inNeighbors;
        private int[] inEdges;

        private final Manifest manifest = new Manifest();

        Run(final SnapshotSource source, final BuildDirectory dir, final BuildOptions options) {
            this.source = source;
            this.dir = dir;
            this.options = options;
            this.layout = options.layout();
            this.identity = layout != SnapshotLayout.TOPOLOGY;
            this.full = layout == SnapshotLayout.FULL;
            this.grouped = source.edgeScanOrder() == EdgeScanOrder.GROUPED_BY_OUT_VERTEX;
            this.edgeIdIndex = identity && options.edgeIdIndex();
        }

        BuildStats run() {
            phaseStart = System.nanoTime();
            final SourceVersion version = source.version();

            source.scanVertices(this::onVertex);
            checkCount("vertex", vertexCount, source.vertexCount());
            endPhase("scan-vertices");

            source.scanEdges(this::onEdge);
            checkCount("edge", edgeCount, source.edgeCount());
            vertexOrdinals = null;
            lastOutId = null;
            endPhase("scan-edges");

            if (full) {
                source.scanVariables(variableVisitor);
                endPhase("scan-variables");
            }

            buildAdjacency();
            endPhase("build-adjacency");

            manifest.setSourceId(version.sourceId());
            manifest.setSourceVersion(version.version());
            manifest.setLayout(layout);
            manifest.setVertexCount(vertexCount);
            manifest.setEdgeCount(edgeCount);
            manifest.setOutEdgesImplicit(grouped);
            manifest.setEdgeIdIndex(edgeIdIndex);
            manifest.setSourceDefaults(source.defaults());

            if (identity) {
                writeVertices();
                endPhase("write-vertices");
                writeEdges();
                endPhase("write-edges");
            }
            writeAdjacency();
            endPhase("write-adjacency");
            if (full) {
                writeProperties();
                endPhase("write-properties");
            }

            segments.sort(Comparator.comparing(Manifest.SegmentInfo::path));
            manifest.setSegments(segments);
            dir.publish(manifest);
            return finishStats();
        }

        // ------------------------------------------------------------------ phases and stats

        private void endPhase(final String name) {
            final long elapsed = System.nanoTime() - phaseStart;
            phases.add(new BuildStats.Phase(name, elapsed, dir.bytesOnDisk()));
            phaseStart = System.nanoTime();
        }

        private BuildStats finishStats() {
            final long elapsed = System.nanoTime() - phaseStart;
            final Path root = dir.target();
            final Map<String, Long> sizes = new LinkedHashMap<>();
            long disk = 0;
            try {
                for (final Manifest.SegmentInfo s : segments) {
                    final long size = Files.size(root.resolve(s.path()));
                    sizes.put(s.path(), size);
                    disk += size;
                }
                disk += Files.size(root.resolve(SegmentPaths.MANIFEST));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            phases.add(new BuildStats.Phase("publish", elapsed, disk));
            return new BuildStats(phases, sizes);
        }

        private static void checkCount(final String kind, final long emitted, final OptionalLong expected) {
            if (expected.isPresent() && expected.getAsLong() != emitted) {
                throw new IllegalStateException("The source announced " + expected.getAsLong() + " " + kind
                        + " elements but emitted " + emitted);
            }
        }

        // ------------------------------------------------------------------ vertex scan

        private int currentVertex;
        private PropertyEntries currentEntries;
        private final PropertyVisitor metaVisitor = (metaKey, metaValue) ->
                currentEntries.meta(metaKeys.codeOf(metaKey)).add(currentEntries.ordinals.size() - 1, metaValue,
                        metaKey);
        private final VertexPropertyVisitor vertexPropertyVisitor = (id, key, value, metaProperties) -> {
            final int code = vertexKeys.codeOf(key);
            while (vertexProperties.size() <= code) vertexProperties.add(new PropertyEntries(true));
            currentEntries = vertexProperties.get(code);
            currentEntries.add(currentVertex, id, value, "vertex property", key);
            metaProperties.forEach(metaVisitor);
        };

        private void onVertex(final Object id, final List<String> labels, final VertexPropertySource props) {
            if (vertexCount >= MAX_ELEMENTS) {
                throw new UnsupportedSnapshotDataException("More than " + MAX_ELEMENTS + " vertices are not supported");
            }
            final int ordinal = vertexCount;
            final ValueType idType = ValueCodec.requireIdentifierType(id, "vertex identifier", null);
            final int existing = vertexOrdinals.indexOf(id);
            if (vertexOrdinals.indexExists(existing)) {
                throw new UnsupportedSnapshotDataException("Duplicate vertex identifier " + id + " ("
                        + id.getClass().getSimpleName() + ") at ordinals " + vertexOrdinals.indexGet(existing)
                        + " and " + ordinal);
            }
            vertexOrdinals.indexInsert(existing, id, ordinal);
            if (identity) {
                vertexIdStats.add(idType);
                vertexIds.add(id);
                vertexIndexKeys.add(IdentifierIndex.keyOf(id));
                for (final String label : labels) {
                    final int code = vertexLabels.codeOf(label);
                    vertexLabelCodes.add(code);
                    vertexLabelHistogram.increment(code);
                }
                vertexLabelCounts.add(labels.size());
                if (labels.size() != 1) multiLabels = true;
            }
            if (full) {
                currentVertex = ordinal;
                props.forEach(vertexPropertyVisitor);
            }
            vertexCount++;
        }

        // ------------------------------------------------------------------ edge scan

        private int currentEdge;
        private final PropertyVisitor edgePropertyVisitor = (key, value) -> {
            final int code = edgeKeys.codeOf(key);
            while (edgeProperties.size() <= code) edgeProperties.add(new PropertyEntries(false));
            edgeProperties.get(code).add(currentEdge, null, value, "edge property", key);
        };

        private void onEdge(final Object id, final String label, final Object outVertexId, final Object inVertexId,
                            final PropertySource props) {
            if (edgeCount >= MAX_ELEMENTS) {
                throw new UnsupportedSnapshotDataException("More than " + MAX_ELEMENTS + " edges are not supported");
            }
            final int ordinal = edgeCount;
            final int out;
            if (lastOutId != null && lastOutId.equals(outVertexId)) {
                out = lastOutResolved;
            } else {
                out = resolve(outVertexId, "out", ordinal);
                lastOutId = outVertexId;
                lastOutResolved = out;
            }
            final int in = resolve(inVertexId, "in", ordinal);
            if (grouped && out < lastOutOrdinal) {
                throw new IllegalStateException("The source reports edges grouped by out-vertex, but edge " + ordinal
                        + " has out-vertex ordinal " + out + " after " + lastOutOrdinal);
            }
            lastOutOrdinal = out;
            edgeOut.add(out);
            edgeIn.add(in);
            if (identity) {
                edgeIdStats.add(ValueCodec.requireIdentifierType(id, "edge identifier", null));
                edgeIds.add(id);
                if (edgeIdIndex) edgeIndexKeys.add(IdentifierIndex.keyOf(id));
                final int labelCode = edgeLabels.codeOf(label);
                edgeLabelCodes.add(labelCode);
                edgeLabelHistogram.increment(labelCode);
            }
            if (full) {
                currentEdge = ordinal;
                props.forEach(edgePropertyVisitor);
            }
            edgeCount++;
        }

        private int resolve(final Object vertexId, final String side, final int edgeOrdinal) {
            final int idx = vertexOrdinals.indexOf(vertexId);
            if (!vertexOrdinals.indexExists(idx)) {
                throw new IllegalStateException("Edge " + edgeOrdinal + " has " + side + "-vertex identifier "
                        + vertexId + " that was not emitted by the vertex scan");
            }
            return vertexOrdinals.indexGet(idx);
        }

        // ------------------------------------------------------------------ variable scan

        private final PropertyVisitor variableVisitor = (key, value) -> {
            final int known = variableKeys.size();
            if (variableKeys.codeOf(key) < known) {
                throw new UnsupportedSnapshotDataException("Duplicate variable key '" + key + "'");
            }
            if (value == null) variableStats.addNull();
            else variableStats.add(ValueCodec.requireType(value, "variable", key));
            variableValues.add(value);
        };

        // ------------------------------------------------------------------ adjacency

        private void buildAdjacency() {
            final int[] out = edgeOut.buffer;
            final int[] in = edgeIn.buffer;
            outOffsets = offsets(out);
            inOffsets = offsets(in);
            outNeighbors = new int[edgeCount];
            inNeighbors = new int[edgeCount];
            // with implicit out-edges the out-adjacency position of an edge is its ordinal
            if (identity && !grouped) outEdges = new int[edgeCount];
            if (identity) inEdges = new int[edgeCount];
            final int[] outCursor = cursors(outOffsets);
            final int[] inCursor = cursors(inOffsets);
            for (int e = 0; e < edgeCount; e++) {
                final int p = outCursor[out[e]]++;
                outNeighbors[p] = in[e];
                if (outEdges != null) outEdges[p] = e;
                final int q = inCursor[in[e]]++;
                inNeighbors[q] = out[e];
                if (inEdges != null) inEdges[q] = e;
            }
        }

        private long[] offsets(final int[] endpoints) {
            final long[] offsets = new long[vertexCount + 1];
            for (int e = 0; e < edgeCount; e++) offsets[endpoints[e] + 1]++;
            for (int v = 0; v < vertexCount; v++) offsets[v + 1] += offsets[v];
            return offsets;
        }

        private int[] cursors(final long[] offsets) {
            final int[] cursors = new int[vertexCount];
            for (int v = 0; v < vertexCount; v++) cursors[v] = (int) offsets[v];
            return cursors;
        }

        // ------------------------------------------------------------------ writing

        private void writeVertices() {
            final Manifest.ColumnInfo idInfo = writeIds(SegmentPaths.VERTEX_IDS_DIR, vertexIds, vertexIdStats,
                    vertexCount);
            manifest.setVertexIds(idInfo);
            manifest.setVertexIdIndexExact(IdentifierIndex.exactType(idInfo) != null);
            if (multiLabels) {
                writeLabelLists();
                manifest.setVertexLabelLayout(Manifest.VertexLabelLayout.MULTI);
            } else {
                writeLabels(SegmentPaths.VERTEX_LABELS, vertexLabels, vertexLabelCodes, vertexCount);
                manifest.setVertexLabelLayout(Manifest.VertexLabelLayout.SINGLE);
            }
            manifest.setVertexLabels(new ArrayList<>(vertexLabels.labels()));
            manifest.setVertexLabelCounts(vertexLabelHistogram.toArray(vertexLabels.size()));
            writeIndex("vertex", SegmentPaths.VERTEX_IDS_DIR, SegmentPaths.VERTEX_ID_INDEX_KEYS,
                    SegmentPaths.VERTEX_ID_INDEX_ORDINALS, idInfo, vertexIndexKeys, vertexCount);
            vertexIds.clear();
        }

        private void writeEdges() {
            final Manifest.ColumnInfo idInfo = writeIds(SegmentPaths.EDGE_IDS_DIR, edgeIds, edgeIdStats, edgeCount);
            manifest.setEdgeIds(idInfo);
            manifest.setEdgeIdIndexExact(IdentifierIndex.exactType(idInfo) != null);
            writeLabels(SegmentPaths.EDGE_LABELS, edgeLabels, edgeLabelCodes, edgeCount);
            manifest.setEdgeLabels(new ArrayList<>(edgeLabels.labels()));
            manifest.setEdgeLabelCounts(edgeLabelHistogram.toArray(edgeLabels.size()));
            writeInts(SegmentPaths.EDGE_OUT_VERTICES, edgeOut.buffer, edgeCount);
            writeInts(SegmentPaths.EDGE_IN_VERTICES, edgeIn.buffer, edgeCount);
            if (edgeIdIndex) {
                writeIndex("edge", SegmentPaths.EDGE_IDS_DIR, SegmentPaths.EDGE_ID_INDEX_KEYS,
                        SegmentPaths.EDGE_ID_INDEX_ORDINALS, idInfo, edgeIndexKeys, edgeCount);
            }
            edgeIds.clear();
        }

        private void writeAdjacency() {
            writeLongs(SegmentPaths.OUT_OFFSETS, outOffsets, vertexCount + 1);
            writeInts(SegmentPaths.OUT_NEIGHBORS, outNeighbors, edgeCount);
            writeLongs(SegmentPaths.IN_OFFSETS, inOffsets, vertexCount + 1);
            writeInts(SegmentPaths.IN_NEIGHBORS, inNeighbors, edgeCount);
            if (identity) {
                if (outEdges != null) writeInts(SegmentPaths.OUT_EDGES, outEdges, edgeCount);
                writeInts(SegmentPaths.IN_EDGES, inEdges, edgeCount);
            }
        }

        private void writeProperties() {
            final List<Manifest.ColumnInfo> valueInfos = new ArrayList<>();
            final List<Manifest.ColumnInfo> idInfos = new ArrayList<>();
            final List<Manifest.VertexKeyInfo> keyInfos = new ArrayList<>();
            for (int code = 0; code < vertexProperties.size(); code++) {
                final PropertyEntries entries = vertexProperties.get(code);
                final int count = entries.ordinals.size();
                final boolean multi = entries.multi;
                final long elementCount = multi ? count : vertexCount;
                final Manifest.ColumnInfo valueInfo = entries.stats.toInfo(elementCount);
                final Manifest.ColumnInfo idInfo = entries.idStats.toInfo(elementCount);
                try (VertexPropertyColumnWriter writer = VertexPropertyColumnWriter.create(dir::segmentPath, code,
                        elementCount, valueInfo, idInfo)) {
                    for (int i = 0; i < count; i++) {
                        writer.append(multi ? i : entries.ordinals.get(i), entries.ids.get(i), entries.values.get(i));
                    }
                    segments.addAll(writer.finish());
                }
                Manifest.OwnerEncoding ownerEncoding = null;
                if (multi) {
                    ownerEncoding = OwnerWriter.encodingOf(vertexCount, entries.owners);
                    try (OwnerWriter writer = OwnerWriter.create(dir::segmentPath, code, vertexCount,
                            entries.owners)) {
                        int start = 0;
                        while (start < count) {
                            int end = start + 1;
                            while (end < count && entries.ordinals.get(end) == entries.ordinals.get(start)) end++;
                            writer.owner(entries.ordinals.get(start), end - start);
                            start = end;
                        }
                        segments.addAll(writer.finish(count));
                    }
                }
                // the vertex-property ordinal is the entry index, except that a dense single column is indexed by
                // the vertex ordinal
                final boolean byVertex = !multi && valueInfo.encoding().name().startsWith("DENSE");
                final Map<Integer, Manifest.ColumnInfo> metaInfos = new TreeMap<>();
                for (final Map.Entry<Integer, MetaEntries> meta : entries.meta.entrySet()) {
                    final MetaEntries me = meta.getValue();
                    final long metaElements = byVertex ? vertexCount : count;
                    final Manifest.ColumnInfo metaInfo = me.stats.toInfo(metaElements);
                    try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath,
                            SegmentPaths.vertexPropertyMetaDir(code, meta.getKey()), metaElements, metaInfo)) {
                        for (int i = 0; i < me.entryIndexes.size(); i++) {
                            final int entry = me.entryIndexes.get(i);
                            writer.append(byVertex ? entries.ordinals.get(entry) : entry, me.values.get(i));
                        }
                        segments.addAll(writer.finish());
                    }
                    metaInfos.put(meta.getKey(), metaInfo);
                }
                keyInfos.add(new Manifest.VertexKeyInfo(multi ? Manifest.PropertyLayout.MULTI
                        : Manifest.PropertyLayout.SINGLE, ownerEncoding, count, metaInfos));
                valueInfos.add(valueInfo);
                idInfos.add(idInfo);
                vertexProperties.set(code, null);
            }
            manifest.setVertexKeys(new ArrayList<>(vertexKeys.labels()));
            manifest.setVertexProperties(valueInfos);
            manifest.setVertexPropertyIds(idInfos);
            manifest.setVertexKeyInfos(keyInfos);
            manifest.setMetaKeys(new ArrayList<>(metaKeys.labels()));

            final List<Manifest.ColumnInfo> edgeInfos = new ArrayList<>();
            for (int code = 0; code < edgeProperties.size(); code++) {
                final PropertyEntries entries = edgeProperties.get(code);
                final Manifest.ColumnInfo info = entries.stats.toInfo(edgeCount);
                try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath, SegmentPaths.edgePropertyDir(code),
                        edgeCount, info)) {
                    for (int i = 0; i < entries.ordinals.size(); i++) {
                        writer.append(entries.ordinals.get(i), entries.values.get(i));
                    }
                    segments.addAll(writer.finish());
                }
                edgeInfos.add(info);
                edgeProperties.set(code, null);
            }
            manifest.setEdgeKeys(new ArrayList<>(edgeKeys.labels()));
            manifest.setEdgeProperties(edgeInfos);

            final int variableCount = variableKeys.size();
            if (variableCount > 0) {
                final Manifest.ColumnInfo info = variableStats.toInfo(variableCount);
                try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath, SegmentPaths.VARIABLES_DIR,
                        variableCount, info)) {
                    for (int code = 0; code < variableCount; code++) writer.append(code, variableValues.get(code));
                    segments.addAll(writer.finish());
                }
                manifest.setVariableKeys(new ArrayList<>(variableKeys.labels()));
                manifest.setVariables(info);
            }
        }

        private Manifest.ColumnInfo writeIds(final String relativeDir, final List<Object> ids,
                                             final ColumnStats stats, final int count) {
            final Manifest.ColumnInfo info = stats.toInfo(count);
            try (ColumnWriter writer = ColumnWriter.create(dir::segmentPath, relativeDir, count, info)) {
                for (int i = 0; i < count; i++) writer.append(i, ids.get(i));
                segments.addAll(writer.finish());
            }
            return info;
        }

        private void writeLabels(final String relativePath, final LabelDictionary dictionary, final IntArrayList codes,
                                 final int count) {
            try (SegmentWriter writer = LabelDictionary.createCodeWriter(dir.segmentPath(relativePath),
                    dictionary.size())) {
                for (int i = 0; i < count; i++) LabelDictionary.writeCode(writer, codes.buffer[i]);
                writer.finish();
                segments.add(writer.info(relativePath));
            }
        }

        private void writeLabelLists() {
            try (LabelListWriter writer = LabelListWriter.create(dir::segmentPath, vertexLabels.size())) {
                int position = 0;
                for (int v = 0; v < vertexCount; v++) {
                    for (int n = vertexLabelCounts.get(v); n > 0; n--) writer.add(vertexLabelCodes.get(position++));
                    writer.endVertex();
                }
                segments.addAll(writer.finish(vertexCount));
            }
        }

        // the identifier column must be complete, and is mapped when the index is not exact
        private void writeIndex(final String kind, final String idsDir, final String keysPath,
                                final String ordinalsPath, final Manifest.ColumnInfo idInfo, final LongArrayList keys,
                                final int count) {
            final ValueType exactType = IdentifierIndex.exactType(idInfo);
            final ColumnReader ids = exactType == null
                    ? ColumnReader.open(ColumnReader.readOnlyOpener(dir.path()), idsDir, count, idInfo) : null;
            try (IdentifierIndexWriter writer = IdentifierIndexWriter.create(dir.segmentPath(keysPath),
                    dir.segmentPath(ordinalsPath), kind, count, exactType, ids)) {
                writer.addAllSorting(keys.buffer, count);
                segments.addAll(writer.finish(keysPath, ordinalsPath));
            } finally {
                if (ids != null) ids.close();
            }
        }

        private void writeInts(final String relativePath, final int[] values, final int count) {
            try (SegmentWriter writer = SegmentWriter.create(dir.segmentPath(relativePath), Integer.BYTES)) {
                for (int i = 0; i < count; i++) writer.writeInt(values[i]);
                writer.finish();
                segments.add(writer.info(relativePath));
            }
        }

        private void writeLongs(final String relativePath, final long[] values, final int count) {
            try (SegmentWriter writer = SegmentWriter.create(dir.segmentPath(relativePath), Long.BYTES)) {
                for (int i = 0; i < count; i++) writer.writeLong(values[i]);
                writer.finish();
                segments.add(writer.info(relativePath));
            }
        }
    }

    /**
     * The entries of one property key in ascending ordinal order, with the statistics of their value types and, for
     * vertex properties, of their identifier types. A vertex can have several vertex properties of the key, which
     * are consecutive entries with the same ordinal. The meta-property entries refer to their vertex property by entry
     * index.
     */
    private static final class PropertyEntries {
        private final ColumnStats stats = new ColumnStats();
        private final ColumnStats idStats;
        private final IntArrayList ordinals = new IntArrayList();
        private final List<Object> values = new ArrayList<>();
        private final List<Object> ids;
        private final Map<Integer, MetaEntries> meta = new TreeMap<>();
        private boolean multi;
        private int owners;

        PropertyEntries(final boolean withIds) {
            this.idStats = withIds ? new ColumnStats() : null;
            this.ids = withIds ? new ArrayList<>() : null;
        }

        void add(final int ordinal, final Object id, final Object value, final String kind, final String key) {
            final int n = ordinals.size();
            if (n > 0 && ordinals.get(n - 1) == ordinal) {
                if (ids == null) {
                    throw new UnsupportedSnapshotDataException("Duplicate " + kind + " '" + key + "' on ordinal "
                            + ordinal);
                }
                multi = true;
            } else {
                owners++;
            }
            if (ids != null) {
                idStats.add(ValueCodec.requireIdentifierType(id, kind + " identifier", key));
                ids.add(id);
            }
            if (value == null) stats.addNull();
            else stats.add(ValueCodec.requireType(value, kind, key), value);
            ordinals.add(ordinal);
            values.add(value);
        }

        MetaEntries meta(final int metaKeyCode) {
            return meta.computeIfAbsent(metaKeyCode, c -> new MetaEntries());
        }
    }

    /**
     * The entries of one meta-property key of one vertex-property key in ascending entry order.
     */
    private static final class MetaEntries {
        private final ColumnStats stats = new ColumnStats();
        private final IntArrayList entryIndexes = new IntArrayList();
        private final List<Object> values = new ArrayList<>();

        void add(final int entryIndex, final Object value, final String key) {
            final int n = entryIndexes.size();
            if (n > 0 && entryIndexes.get(n - 1) == entryIndex) {
                throw new UnsupportedSnapshotDataException("Duplicate meta-property '" + key + "'");
            }
            if (value == null) stats.addNull();
            else stats.add(ValueCodec.requireType(value, "meta-property", key), value);
            entryIndexes.add(entryIndex);
            values.add(value);
        }
    }
}
