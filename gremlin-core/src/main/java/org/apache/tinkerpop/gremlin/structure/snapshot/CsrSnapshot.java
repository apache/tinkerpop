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
package org.apache.tinkerpop.gremlin.structure.snapshot;

import org.apache.tinkerpop.gremlin.structure.snapshot.format.ColumnReader;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.IdentifierIndex;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.LabelDictionary;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.Manifest;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ManifestIO;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.MappedSegment;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.OwnerCursor;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SegmentPaths;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.SnapshotLayout;
import org.apache.tinkerpop.gremlin.structure.snapshot.format.ValueType;
import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceDefaults;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * Read-only access to a completed snapshot bundle through memory-mapped segments. The bundle's
 * {@code manifest.json} is read, every segment it lists is mapped, and each segment's header is checked against the
 * manifest entry. Adjacency is exposed by position: the out-adjacency of vertex {@code v} occupies positions
 * {@code [outStart(v), outEnd(v))} and {@link #outNeighbor(long)} and {@link #outEdge(long)} give the neighbor and edge
 * ordinals at a position, likewise for the incoming direction. Identifiers, labels, endpoints and properties are
 * keyed by vertex or edge ordinal.
 * <p/>
 * Which methods are available depends on the {@link SnapshotLayout}. A method that needs a segment the layout does not
 * publish throws {@link UnsupportedOperationException}: adjacency edge ordinals, identifiers, labels, endpoints and
 * identifier lookup need {@code IDENTITY} or {@code FULL}, properties need {@code FULL} and edge identifier lookup
 * also needs the edge identifier index.
 * <p/>
 * Ordinal arguments are not range-checked beyond the bounds check of the underlying segment, which throws
 * {@link IndexOutOfBoundsException}. Instances may be used from several threads. Nothing may be called after
 * {@link #close()}.
 * <p/>
 * Vertices may have several labels ({@link #vertexLabelCount(int)}), and a vertex property key may hold several vertex
 * properties per vertex. Vertex properties of one key are addressed by their vertex-property ordinal, see
 * {@link #vertexPropertyStart(int, int)}; meta-properties, graph variables and the source defaults are also exposed.
 */
public final class CsrSnapshot implements AutoCloseable {

    private final Path root;
    private final Manifest manifest;
    private final SnapshotLayout layout;
    private final int vertexCount;
    private final int edgeCount;
    private final boolean outEdgesImplicit;

    // every mapped segment, by bundle-relative path
    private final Map<String, MappedSegment> segments;

    private final MappedSegment outOffsets;
    private final MappedSegment outNeighbors;
    private final MappedSegment outEdges;
    private final MappedSegment inOffsets;
    private final MappedSegment inNeighbors;
    private final MappedSegment inEdges;

    private final String[] vertexLabels;
    private final String[] edgeLabels;
    private final MappedSegment vertexLabelCodes;
    private final boolean multiLabel;
    private final MappedSegment vertexLabelOffsets;
    private final MappedSegment vertexLabelCodeList;
    private final MappedSegment edgeLabelCodes;
    private final MappedSegment edgeOutVertices;
    private final MappedSegment edgeInVertices;

    private final ColumnReader vertexIds;
    private final ColumnReader edgeIds;
    private final IdentifierIndex vertexIdIndex;
    private final IdentifierIndex edgeIdIndex;

    // statistics: elements per label code, from the manifest or computed at open; null when the layout has no labels
    private final long[] vertexLabelCounts;
    private final long[] edgeLabelCounts;

    private final Map<String, Integer> vertexLabelByName = new HashMap<>();
    private final Map<String, Integer> edgeLabelByName = new HashMap<>();

    private final List<String> vertexKeys;
    private final List<String> edgeKeys;
    private final Map<String, Integer> vertexKeyCodes = new HashMap<>();
    private final Map<String, Integer> edgeKeyCodes = new HashMap<>();
    private final List<ColumnReader> vertexProperties = new ArrayList<>();
    private final List<ColumnReader> vertexPropertyIds = new ArrayList<>();
    private final List<ColumnReader> edgeProperties = new ArrayList<>();

    private final SourceDefaults sourceDefaults;
    // per vertex key code: multi layout flag, owner files (null in the single layout, ordinals null for dense owners),
    // and the meta-property key codes with their columns, both in ascending meta key code order
    private final boolean[] multiKeys;
    private final List<MappedSegment> ownerOffsets = new ArrayList<>();
    private final List<MappedSegment> ownerOrdinals = new ArrayList<>();
    private final List<int[]> metaCodes = new ArrayList<>();
    private final List<ColumnReader[]> metaColumns = new ArrayList<>();
    private final List<String> metaKeys = new ArrayList<>();
    private final Map<String, Integer> metaKeyCodes = new HashMap<>();
    private final Map<String, Object> variables = new LinkedHashMap<>();
    private final Map<String, Object> variablesView = Collections.unmodifiableMap(variables);

    private CsrSnapshot(final Path root, final Manifest manifest, final Map<String, MappedSegment> segments) {
        this.root = root;
        this.manifest = manifest;
        this.segments = segments;
        this.layout = manifest.getLayout();
        this.outEdgesImplicit = manifest.isOutEdgesImplicit();

        final Function<String, MappedSegment> opener = path -> {
            final MappedSegment segment = segments.get(path);
            if (segment == null) {
                throw new IllegalStateException("Segment " + path + " is not listed in the manifest of " + root);
            }
            return segment;
        };

        final long vertices = manifest.getVertexCount();
        final long edges = manifest.getEdgeCount();
        if (vertices < 0 || vertices >= Integer.MAX_VALUE || edges < 0 || edges >= Integer.MAX_VALUE) {
            throw new IllegalStateException("Counts of " + vertices + " vertices and " + edges
                    + " edges exceed the 32-bit ordinals of this reader");
        }
        vertexCount = (int) vertices;
        edgeCount = (int) edges;
        final SourceDefaults defaults = manifest.getSourceDefaults();
        sourceDefaults = defaults == null ? SourceDefaults.DEFAULT : defaults;
        multiLabel = layout != SnapshotLayout.TOPOLOGY
                && manifest.getVertexLabelLayout() == Manifest.VertexLabelLayout.MULTI;

        // adjacency, present in every layout
        outOffsets = require(opener, SegmentPaths.OUT_OFFSETS, 8, vertices + 1);
        outNeighbors = require(opener, SegmentPaths.OUT_NEIGHBORS, 4, edges);
        inOffsets = require(opener, SegmentPaths.IN_OFFSETS, 8, vertices + 1);
        inNeighbors = require(opener, SegmentPaths.IN_NEIGHBORS, 4, edges);
        checkOffsets(outOffsets, edges);
        checkOffsets(inOffsets, edges);

        if (layout == SnapshotLayout.TOPOLOGY) {
            outEdges = null;
            inEdges = null;
            vertexLabels = null;
            edgeLabels = null;
            vertexLabelCounts = null;
            edgeLabelCounts = null;
            vertexLabelCodes = null;
            vertexLabelOffsets = null;
            vertexLabelCodeList = null;
            multiKeys = new boolean[0];
            edgeLabelCodes = null;
            edgeOutVertices = null;
            edgeInVertices = null;
            vertexIds = null;
            edgeIds = null;
            vertexIdIndex = null;
            edgeIdIndex = null;
            vertexKeys = Collections.emptyList();
            edgeKeys = Collections.emptyList();
            return;
        }

        outEdges = outEdgesImplicit ? null : require(opener, SegmentPaths.OUT_EDGES, 4, edges);
        inEdges = require(opener, SegmentPaths.IN_EDGES, 4, edges);

        vertexLabels = manifest.getVertexLabels().toArray(new String[0]);
        edgeLabels = manifest.getEdgeLabels().toArray(new String[0]);
        for (int code = 0; code < vertexLabels.length; code++) vertexLabelByName.putIfAbsent(vertexLabels[code], code);
        for (int code = 0; code < edgeLabels.length; code++) edgeLabelByName.putIfAbsent(edgeLabels[code], code);
        if (multiLabel) {
            vertexLabelCodes = null;
            vertexLabelOffsets = require(opener, SegmentPaths.VERTEX_LABEL_OFFSETS, 8, vertices + 1);
            if (vertexLabelOffsets.getLong(0) != 0) {
                throw new IllegalStateException("Label offsets of " + root + " do not start at zero");
            }
            vertexLabelCodeList = require(opener, SegmentPaths.VERTEX_LABEL_CODES,
                    LabelDictionary.codeWidth(vertexLabels.length), vertexLabelOffsets.getLong(vertices));
        } else {
            vertexLabelCodes = require(opener, SegmentPaths.VERTEX_LABELS,
                    LabelDictionary.codeWidth(vertexLabels.length), vertices);
            vertexLabelOffsets = null;
            vertexLabelCodeList = null;
        }
        edgeLabelCodes = require(opener, SegmentPaths.EDGE_LABELS, LabelDictionary.codeWidth(edgeLabels.length),
                edges);
        edgeOutVertices = require(opener, SegmentPaths.EDGE_OUT_VERTICES, 4, edges);
        edgeInVertices = require(opener, SegmentPaths.EDGE_IN_VERTICES, 4, edges);

        vertexLabelCounts = computeVertexLabelCounts(manifest.getVertexLabelCounts());
        edgeLabelCounts = computeEdgeLabelCounts(manifest.getEdgeLabelCounts());

        if (manifest.getVertexIds() == null || manifest.getEdgeIds() == null) {
            throw new IllegalStateException("The manifest of a " + layout + " snapshot lacks an identifier column");
        }
        vertexIds = ColumnReader.open(opener, SegmentPaths.VERTEX_IDS_DIR, vertices, manifest.getVertexIds());
        edgeIds = ColumnReader.open(opener, SegmentPaths.EDGE_IDS_DIR, edges, manifest.getEdgeIds());
        if (vertexIds.presentCount() != vertices || edgeIds.presentCount() != edges) {
            throw new IllegalStateException("Identifier columns of " + root + " do not cover every element");
        }
        vertexIdIndex = openIndex(opener, SegmentPaths.VERTEX_ID_INDEX_KEYS, SegmentPaths.VERTEX_ID_INDEX_ORDINALS,
                vertices, manifest.getVertexIds(), manifest.isVertexIdIndexExact(), vertexIds);
        edgeIdIndex = manifest.isEdgeIdIndex()
                ? openIndex(opener, SegmentPaths.EDGE_ID_INDEX_KEYS, SegmentPaths.EDGE_ID_INDEX_ORDINALS, edges,
                        manifest.getEdgeIds(), manifest.isEdgeIdIndexExact(), edgeIds)
                : null;

        if (layout == SnapshotLayout.FULL) {
            vertexKeys = Collections.unmodifiableList(new ArrayList<>(manifest.getVertexKeys()));
            edgeKeys = Collections.unmodifiableList(new ArrayList<>(manifest.getEdgeKeys()));
            if (manifest.getVertexProperties().size() != vertexKeys.size()
                    || manifest.getVertexPropertyIds().size() != vertexKeys.size()
                    || manifest.getEdgeProperties().size() != edgeKeys.size()) {
                throw new IllegalStateException("Property column lists of " + root
                        + " are not parallel to the key dictionaries");
            }
            metaKeys.addAll(manifest.getMetaKeys());
            for (int code = 0; code < metaKeys.size(); code++) {
                if (metaKeyCodes.put(metaKeys.get(code), code) != null) {
                    throw new IllegalStateException("Duplicate meta-property key " + metaKeys.get(code));
                }
            }
            final List<Manifest.VertexKeyInfo> keyInfos = manifest.getVertexKeyInfos();
            if (keyInfos.size() != vertexKeys.size()) {
                throw new IllegalStateException("Vertex key layouts of " + root
                        + " are not parallel to the key dictionary");
            }
            multiKeys = new boolean[vertexKeys.size()];
            for (int code = 0; code < vertexKeys.size(); code++) {
                if (vertexKeyCodes.put(vertexKeys.get(code), code) != null) {
                    throw new IllegalStateException("Duplicate vertex property key " + vertexKeys.get(code));
                }
                final Manifest.VertexKeyInfo keyInfo = keyInfos.get(code);
                final boolean multi = keyInfo.layout() == Manifest.PropertyLayout.MULTI;
                multiKeys[code] = multi;
                final ColumnReader values = ColumnReader.open(opener, SegmentPaths.vertexPropertyDir(code),
                        multi ? keyInfo.propertyCount() : vertices, manifest.getVertexProperties().get(code));
                if (values.presentCount() != keyInfo.propertyCount()
                        || (multi && values.entryCount() != keyInfo.propertyCount())) {
                    throw new IllegalStateException("Vertex property key " + vertexKeys.get(code) + " has "
                            + values.presentCount() + " entries but the manifest says " + keyInfo.propertyCount()
                            + " vertex properties");
                }
                vertexProperties.add(values);
                vertexPropertyIds.add(ColumnReader.openCompanion(opener, SegmentPaths.vertexPropertyIdsDir(code),
                        values, manifest.getVertexPropertyIds().get(code)));
                if (multi) {
                    if (keyInfo.ownerEncoding() == null) {
                        throw new IllegalStateException("Multi vertex property key " + vertexKeys.get(code)
                                + " has no owner encoding");
                    }
                    final boolean sparse = keyInfo.ownerEncoding() == Manifest.OwnerEncoding.SPARSE;
                    MappedSegment ordinals = null;
                    long owners = vertices;
                    if (sparse) {
                        ordinals = opener.apply(SegmentPaths.vertexPropertyOwnerOrdinals(code));
                        if (ordinals.valueWidth() != 4) {
                            throw new IllegalStateException("Segment " + ordinals.path() + " has width "
                                    + ordinals.valueWidth() + ", expected 4");
                        }
                        owners = ordinals.count();
                    }
                    final MappedSegment offsets = require(opener, SegmentPaths.vertexPropertyOwnerOffsets(code), 8,
                            owners + 1);
                    if (offsets.getLong(0) != 0 || offsets.getLong(owners) != keyInfo.propertyCount()) {
                        throw new IllegalStateException("Owner offsets " + offsets.path() + " do not span "
                                + keyInfo.propertyCount() + " vertex properties");
                    }
                    ownerOffsets.add(offsets);
                    ownerOrdinals.add(ordinals);
                } else {
                    ownerOffsets.add(null);
                    ownerOrdinals.add(null);
                }
                final int[] codes = new int[keyInfo.metaColumns().size()];
                final ColumnReader[] columns = new ColumnReader[codes.length];
                int n = 0;
                for (final Map.Entry<Integer, Manifest.ColumnInfo> meta : keyInfo.metaColumns().entrySet()) {
                    final int metaCode = meta.getKey();
                    if (metaCode < 0 || metaCode >= metaKeys.size()) {
                        throw new IllegalStateException("Meta-property key code " + metaCode + " of vertex property "
                                + vertexKeys.get(code) + " is outside the dictionary");
                    }
                    codes[n] = metaCode;
                    columns[n++] = ColumnReader.open(opener, SegmentPaths.vertexPropertyMetaDir(code, metaCode),
                            values.entryCount(), meta.getValue());
                }
                metaCodes.add(codes);
                metaColumns.add(columns);
            }
            for (int code = 0; code < edgeKeys.size(); code++) {
                if (edgeKeyCodes.put(edgeKeys.get(code), code) != null) {
                    throw new IllegalStateException("Duplicate edge property key " + edgeKeys.get(code));
                }
                edgeProperties.add(ColumnReader.open(opener, SegmentPaths.edgePropertyDir(code), edges,
                        manifest.getEdgeProperties().get(code)));
            }
            final List<String> variableKeys = manifest.getVariableKeys();
            if (!variableKeys.isEmpty()) {
                if (manifest.getVariables() == null) {
                    throw new IllegalStateException("The manifest of " + root + " lists variables but no column");
                }
                final ColumnReader column = ColumnReader.open(opener, SegmentPaths.VARIABLES_DIR,
                        variableKeys.size(), manifest.getVariables());
                for (int code = 0; code < variableKeys.size(); code++) {
                    final String key = variableKeys.get(code);
                    if (variables.containsKey(key) || !column.isPresent(code)) {
                        throw new IllegalStateException("Variable " + key + " is duplicated or has no entry");
                    }
                    variables.put(key, column.get(code));
                }
            }
        } else {
            vertexKeys = Collections.emptyList();
            edgeKeys = Collections.emptyList();
            multiKeys = new boolean[0];
        }
    }

    /**
     * Opens the bundle in a directory.
     *
     * @param root            the bundle root, containing {@code manifest.json}
     * @param verifyChecksums whether to recompute the checksum of every segment, which reads the whole bundle. The
     *                        checksums stored in the segment headers are always compared with the manifest.
     * @throws java.io.UncheckedIOException if a file cannot be read or mapped or a segment header is malformed
     * @throws IllegalStateException        if the manifest and the segments disagree
     */
    public static CsrSnapshot open(final Path root, final boolean verifyChecksums) {
        Objects.requireNonNull(root);
        final Manifest manifest = ManifestIO.read(root.resolve(SegmentPaths.MANIFEST));
        if (manifest.getFormatVersion() != Manifest.FORMAT_VERSION) {
            throw new IllegalStateException("Unsupported snapshot format version " + manifest.getFormatVersion()
                    + " in " + root);
        }
        if (manifest.getLayout() == null) {
            throw new IllegalStateException("The manifest of " + root + " has no layout");
        }
        if (manifest.getVertexOrdinalWidth() != Integer.BYTES || manifest.getEdgeOrdinalWidth() != Integer.BYTES) {
            throw new IllegalStateException("Unsupported ordinal widths " + manifest.getVertexOrdinalWidth() + " and "
                    + manifest.getEdgeOrdinalWidth() + " in " + root);
        }

        final Map<String, MappedSegment> segments = new LinkedHashMap<>();
        try {
            final Function<String, MappedSegment> opener = ColumnReader.readOnlyOpener(root);
            for (final Manifest.SegmentInfo info : manifest.getSegments()) {
                final MappedSegment segment = opener.apply(info.path());
                segments.put(info.path(), segment);
                if (segment.valueWidth() != info.width() || segment.count() != info.count()
                        || segment.checksum() != info.checksum()) {
                    throw new IllegalStateException("Segment " + info.path() + " has width " + segment.valueWidth()
                            + ", count " + segment.count() + " and checksum " + segment.checksum()
                            + " but the manifest says " + info.width() + ", " + info.count() + " and "
                            + info.checksum());
                }
            }
            final CsrSnapshot snapshot = new CsrSnapshot(root, manifest, segments);
            if (verifyChecksums) snapshot.verifyChecksums();
            return snapshot;
        } catch (RuntimeException e) {
            for (final MappedSegment segment : segments.values()) segment.close();
            throw e;
        }
    }

    private MappedSegment require(final Function<String, MappedSegment> opener, final String path, final int width,
                                  final long count) {
        final MappedSegment segment = opener.apply(path);
        if (segment.valueWidth() != width || segment.count() != count) {
            throw new IllegalStateException("Segment " + path + " has width " + segment.valueWidth() + " and count "
                    + segment.count() + ", expected width " + width + " and count " + count);
        }
        return segment;
    }

    // offsets must start at zero and end at the number of adjacency entries
    private static void checkOffsets(final MappedSegment offsets, final long edges) {
        if (offsets.getLong(0) != 0 || offsets.getLong(offsets.count() - 1) != edges) {
            throw new IllegalStateException("Offsets " + offsets.path() + " do not span " + edges + " entries");
        }
    }

    // the manifest's vertex label counts when they match the dictionary, otherwise one pass over the label codes
    private long[] computeVertexLabelCounts(final long[] stored) {
        if (stored != null && stored.length == vertexLabels.length) return stored;
        final long[] counts = new long[vertexLabels.length];
        final MappedSegment codes = multiLabel ? vertexLabelCodeList : vertexLabelCodes;
        final long entries = codes.count();
        for (long i = 0; i < entries; i++) counts[checkedCode(LabelDictionary.readCode(codes, i), counts.length)]++;
        return counts;
    }

    private long[] computeEdgeLabelCounts(final long[] stored) {
        if (stored != null && stored.length == edgeLabels.length) return stored;
        final long[] counts = new long[edgeLabels.length];
        for (long i = 0; i < edgeCount; i++) {
            counts[checkedCode(LabelDictionary.readCode(edgeLabelCodes, i), counts.length)]++;
        }
        return counts;
    }

    private static int checkedCode(final int code, final int labels) {
        if (code < 0 || code >= labels) {
            throw new IllegalStateException("Label code " + code + " is outside the dictionary of " + labels
                    + " labels");
        }
        return code;
    }

    private IdentifierIndex openIndex(final Function<String, MappedSegment> opener, final String keysPath,
                                      final String ordinalsPath, final long count, final Manifest.ColumnInfo idInfo,
                                      final boolean manifestExact, final ColumnReader ids) {
        final MappedSegment keys = require(opener, keysPath, 8, count);
        final MappedSegment ordinals = require(opener, ordinalsPath, 4, count);
        final ValueType exactType = IdentifierIndex.exactType(idInfo);
        if (manifestExact != (exactType != null)) {
            throw new IllegalStateException("The exact-index flag of " + keysPath + " disagrees with the identifier "
                    + "column types " + idInfo.valueTypes());
        }
        return IdentifierIndex.of(keys, ordinals, exactType, ids);
    }

    // ---------------------------------------------------------------- bundle

    public Path root() {
        return root;
    }

    public Manifest manifest() {
        return manifest;
    }

    public SnapshotLayout layout() {
        return layout;
    }

    /**
     * The {@code sourceId} of the source version the snapshot was built from.
     */
    public String sourceId() {
        return manifest.getSourceId();
    }

    /**
     * The {@code version} of the source version the snapshot was built from.
     */
    public String sourceVersion() {
        return manifest.getSourceVersion();
    }

    public int vertexCount() {
        return vertexCount;
    }

    public int edgeCount() {
        return edgeCount;
    }

    /**
     * Whether out-adjacency positions are edge ordinals, so that {@link #outEdge(long)} is the identity.
     */
    public boolean isOutEdgesImplicit() {
        return outEdgesImplicit;
    }

    /**
     * Whether edge identifiers can be looked up with {@link #edgeOrdinal(Object)}.
     */
    public boolean hasEdgeIdIndex() {
        return edgeIdIndex != null;
    }

    /**
     * Recomputes the checksum of every segment and compares it with the header.
     *
     * @throws java.io.UncheckedIOException if a segment does not match its header
     */
    public void verifyChecksums() {
        for (final MappedSegment segment : segments.values()) segment.verifyChecksum();
    }

    // ---------------------------------------------------------------- dictionaries

    /**
     * The vertex label dictionary; the code of a label is its index.
     */
    public List<String> vertexLabels() {
        requireIdentity("vertex labels");
        return manifest.getVertexLabels();
    }

    public List<String> edgeLabels() {
        requireIdentity("edge labels");
        return manifest.getEdgeLabels();
    }

    /**
     * The vertex property keys; the code of a key is its index in this list, so the key at index {@code i} is the key
     * of the column at code {@code i}. Empty unless the layout is {@code FULL}.
     */
    public List<String> vertexPropertyKeys() {
        return vertexKeys;
    }

    /**
     * The edge property keys; the code of a key is its index in this list. Empty unless the layout is {@code FULL}.
     */
    public List<String> edgePropertyKeys() {
        return edgeKeys;
    }

    // ---------------------------------------------------------------- adjacency

    /**
     * The first out-adjacency position of the vertex.
     */
    public long outStart(final int vertex) {
        return outOffsets.getLong(vertex);
    }

    /**
     * One past the last out-adjacency position of the vertex.
     */
    public long outEnd(final int vertex) {
        return outOffsets.getLong(vertex + 1L);
    }

    /**
     * The ordinal of the vertex at the far end of the out-adjacency entry at {@code position}.
     */
    public int outNeighbor(final long position) {
        return outNeighbors.getInt(position);
    }

    /**
     * The edge ordinal of the out-adjacency entry at {@code position}, which is {@code position} itself when
     * out-edges are implicit.
     *
     * @throws UnsupportedOperationException if the layout publishes no adjacency edge ordinals
     */
    public int outEdge(final long position) {
        if (outEdgesImplicit) {
            Objects.checkIndex(position, edgeCount);
            return (int) position;
        }
        if (outEdges == null) throw unsupported("out-edge ordinals");
        return outEdges.getInt(position);
    }

    public long inStart(final int vertex) {
        return inOffsets.getLong(vertex);
    }

    public long inEnd(final int vertex) {
        return inOffsets.getLong(vertex + 1L);
    }

    /**
     * The ordinal of the vertex at the far end of the in-adjacency entry at {@code position}.
     */
    public int inNeighbor(final long position) {
        return inNeighbors.getInt(position);
    }

    /**
     * The edge ordinal of the in-adjacency entry at {@code position}.
     *
     * @throws UnsupportedOperationException if the layout publishes no adjacency edge ordinals
     */
    public int inEdge(final long position) {
        if (inEdges == null) throw unsupported("in-edge ordinals");
        return inEdges.getInt(position);
    }

    public int outDegree(final int vertex) {
        return (int) (outEnd(vertex) - outStart(vertex));
    }

    public int inDegree(final int vertex) {
        return (int) (inEnd(vertex) - inStart(vertex));
    }

    // ---------------------------------------------------------------- statistics and code-level access

    /**
     * The number of vertices with each vertex label, indexed by label code; a multi-label vertex counts once for each
     * of its labels. Read from the manifest when it carries the statistic and computed at open otherwise. The array is
     * shared and must not be modified.
     *
     * @throws UnsupportedOperationException if the layout is {@code TOPOLOGY}
     */
    public long[] vertexLabelCounts() {
        requireIdentity("vertex labels");
        return vertexLabelCounts;
    }

    /**
     * The number of edges with each edge label, indexed by label code; see {@link #vertexLabelCounts()}.
     *
     * @throws UnsupportedOperationException if the layout is {@code TOPOLOGY}
     */
    public long[] edgeLabelCounts() {
        requireIdentity("edge labels");
        return edgeLabelCounts;
    }

    /**
     * The code of the vertex property key, or -1 if the snapshot has no such key. Always -1 unless the layout is
     * {@code FULL}.
     */
    public int vertexKeyCodeOf(final String key) {
        final Integer code = vertexKeyCodes.get(key);
        return code == null ? -1 : code;
    }

    /**
     * The code of the edge property key, or -1 if the snapshot has no such key. Always -1 unless the layout is
     * {@code FULL}.
     */
    public int edgeKeyCodeOf(final String key) {
        final Integer code = edgeKeyCodes.get(key);
        return code == null ? -1 : code;
    }

    /**
     * The value column of the vertex property key with the given code; see {@link #vertexProperty(String)} for how
     * to address it.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader vertexPropertyColumn(final int keyCode) {
        requireFull("vertex properties");
        return vertexProperties.get(keyCode);
    }

    /**
     * The vertex-property identifier column of the key with the given code.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader vertexPropertyIdColumn(final int keyCode) {
        requireFull("vertex-property identifiers");
        return vertexPropertyIds.get(keyCode);
    }

    /**
     * The value column of the edge property key with the given code, addressed by edge ordinal.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader edgePropertyColumn(final int keyCode) {
        requireFull("edge properties");
        return edgeProperties.get(keyCode);
    }

    /**
     * The meta-property column for the vertex property key and meta key, or null if no vertex property of the key has
     * the meta key. Its entries are addressed by vertex-property ordinal.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader metaPropertyColumn(final int keyCode, final int metaKeyCode) {
        return metaColumn(keyCode, metaKeyCode);
    }

    /**
     * A cursor over the vertex-property ordinal ranges of the key for ascending vertex ordinals, for both the single
     * and the multi layout. Use one cursor per scan.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public OwnerCursor vertexPropertyCursor(final int keyCode) {
        requireFull("vertex properties");
        if (!multiKeys[keyCode]) return OwnerCursor.ofColumn(vertexProperties.get(keyCode));
        return OwnerCursor.ofOwners(ownerOffsets.get(keyCode), ownerOrdinals.get(keyCode), vertexCount);
    }

    // ---------------------------------------------------------------- vertices

    /**
     * The identifier of the vertex.
     */
    public Object vertexId(final int vertex) {
        requireIdentity("vertex identifiers");
        return vertexIds.get(vertex);
    }

    /**
     * The first label of the vertex in source order, or the empty string when it has none.
     */
    public String vertexLabel(final int vertex) {
        final int code = vertexLabelCode(vertex);
        return code < 0 ? "" : label(vertexLabels, code);
    }

    /**
     * Whether vertices may have zero or several labels, that is whether the snapshot uses the multi-label layout.
     */
    public boolean isMultiLabel() {
        return multiLabel;
    }

    /**
     * The number of labels of the vertex, which is always 1 unless {@link #isMultiLabel()}.
     */
    public int vertexLabelCount(final int vertex) {
        requireIdentity("vertex labels");
        if (!multiLabel) {
            Objects.checkIndex(vertex, vertexCount);
            return 1;
        }
        return (int) (vertexLabelOffsets.getLong(vertex + 1L) - vertexLabelOffsets.getLong(vertex));
    }

    /**
     * The code of the {@code index}th label of the vertex in source order, an index into {@link #vertexLabels()}.
     *
     * @throws IndexOutOfBoundsException if the index is not in {@code [0, vertexLabelCount(vertex))}
     */
    public int vertexLabelCodeAt(final int vertex, final int index) {
        requireIdentity("vertex labels");
        if (!multiLabel) {
            Objects.checkIndex(index, 1);
            return LabelDictionary.readCode(vertexLabelCodes, vertex);
        }
        final long start = vertexLabelOffsets.getLong(vertex);
        Objects.checkIndex(index, vertexLabelOffsets.getLong(vertex + 1L) - start);
        return LabelDictionary.readCode(vertexLabelCodeList, start + index);
    }

    /**
     * The ordinal of the vertex with the given identifier, or -1 if there is none.
     */
    public int vertexOrdinal(final Object id) {
        requireIdentity("vertex identifier lookup");
        return vertexIdIndex.lookup(id);
    }

    /**
     * Ordinal of the vertex with this id, or -1. Integral ids (Byte/Short/Integer/Long) match any stored integral id
     * with the same numeric value when the vertex id index is exact; all other ids match on encoded bytes like
     * {@link #vertexOrdinal(Object)}.
     */
    public int vertexOrdinalCoerced(final Object id) {
        requireIdentity("vertex identifier lookup");
        return coerced(vertexIdIndex, id);
    }

    /**
     * The raw code of the vertex's first label in source order, an index into {@link #vertexLabels()}, or -1 when the
     * vertex has no label.
     */
    public int vertexLabelCode(final int vertex) {
        requireIdentity("vertex labels");
        if (!multiLabel) return LabelDictionary.readCode(vertexLabelCodes, vertex);
        final long start = vertexLabelOffsets.getLong(vertex);
        if (vertexLabelOffsets.getLong(vertex + 1L) == start) return -1;
        return LabelDictionary.readCode(vertexLabelCodeList, start);
    }

    /**
     * The code of the vertex label, or -1 if it is not in the dictionary.
     */
    public int vertexLabelCodeOf(final String label) {
        requireIdentity("vertex labels");
        final Integer code = vertexLabelByName.get(label);
        return code == null ? -1 : code;
    }

    // ---------------------------------------------------------------- edges

    public Object edgeId(final int edge) {
        requireIdentity("edge identifiers");
        return edgeIds.get(edge);
    }

    public String edgeLabel(final int edge) {
        requireIdentity("edge labels");
        return label(edgeLabels, LabelDictionary.readCode(edgeLabelCodes, edge));
    }

    /**
     * The ordinal of the edge with the given identifier, or -1 if there is none.
     *
     * @throws UnsupportedOperationException if the snapshot has no edge identifier index
     */
    public int edgeOrdinal(final Object id) {
        if (edgeIdIndex == null) throw unsupported("edge identifier lookup");
        return edgeIdIndex.lookup(id);
    }

    /**
     * Same as {@link #vertexOrdinalCoerced(Object)} for edges.
     *
     * @throws UnsupportedOperationException if the snapshot has no edge identifier index
     */
    public int edgeOrdinalCoerced(final Object id) {
        if (edgeIdIndex == null) throw unsupported("edge identifier lookup");
        return coerced(edgeIdIndex, id);
    }

    /**
     * The raw label code of the edge, an index into {@link #edgeLabels()}.
     */
    public int edgeLabelCode(final int edge) {
        requireIdentity("edge labels");
        return LabelDictionary.readCode(edgeLabelCodes, edge);
    }

    /**
     * The code of the edge label, or -1 if it is not in the dictionary.
     */
    public int edgeLabelCodeOf(final String label) {
        requireIdentity("edge labels");
        final Integer code = edgeLabelByName.get(label);
        return code == null ? -1 : code;
    }

    /**
     * The ordinal of the edge's out-vertex.
     */
    public int edgeOut(final int edge) {
        requireIdentity("edge endpoints");
        return edgeOutVertices.getInt(edge);
    }

    /**
     * The ordinal of the edge's in-vertex.
     */
    public int edgeIn(final int edge) {
        requireIdentity("edge endpoints");
        return edgeInVertices.getInt(edge);
    }

    // ---------------------------------------------------------------- properties

    /**
     * The column of the vertex property key, or null if the snapshot has no such key. The column of a key that is
     * {@link #isMultiProperty(int) multi} is addressed by vertex-property ordinal rather than by vertex ordinal, so
     * this is only meaningful for other keys; {@link #vertexPropertyValue(int, long)} works for both.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader vertexProperty(final String key) {
        requireFull("vertex properties");
        final Integer code = vertexKeyCodes.get(key);
        return code == null ? null : vertexProperties.get(code);
    }

    /**
     * The column of vertex-property identifiers for the key, or null if the snapshot has no such key. It has an entry
     * exactly where {@link #vertexProperty(String)} has a value.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader vertexPropertyId(final String key) {
        requireFull("vertex-property identifiers");
        final Integer code = vertexKeyCodes.get(key);
        return code == null ? null : vertexPropertyIds.get(code);
    }

    /**
     * The column of the edge property key, or null if the snapshot has no such key.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public ColumnReader edgeProperty(final String key) {
        requireFull("edge properties");
        final Integer code = edgeKeyCodes.get(key);
        return code == null ? null : edgeProperties.get(code);
    }

    /**
     * The first vertex-property ordinal of the vertex for the key. Together with
     * {@link #vertexPropertyEnd(int, int)} this is the half-open range {@code [start, end)} of the vertex properties
     * the vertex has for the key, empty when it has none. In the single layout the ordinal is the entry index of the
     * key's column, so the range is {@code [e, e + 1)} when the vertex has a value.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public long vertexPropertyStart(final int keyCode, final int vertex) {
        requireFull("vertex properties");
        if (!multiKeys[keyCode]) {
            final long entry = vertexProperties.get(keyCode).entryIndex(vertex);
            return entry < 0 ? 0 : entry;
        }
        final MappedSegment offsets = ownerOffsets.get(keyCode);
        final MappedSegment ordinals = ownerOrdinals.get(keyCode);
        if (ordinals == null) {
            Objects.checkIndex(vertex, vertexCount);
            return offsets.getLong(vertex);
        }
        final long owner = ownerIndex(ordinals, vertex);
        return owner < 0 ? 0 : offsets.getLong(owner);
    }

    /**
     * One past the last vertex-property ordinal of the vertex for the key; see
     * {@link #vertexPropertyStart(int, int)}.
     */
    public long vertexPropertyEnd(final int keyCode, final int vertex) {
        requireFull("vertex properties");
        if (!multiKeys[keyCode]) {
            final long entry = vertexProperties.get(keyCode).entryIndex(vertex);
            return entry < 0 ? 0 : entry + 1;
        }
        final MappedSegment offsets = ownerOffsets.get(keyCode);
        final MappedSegment ordinals = ownerOrdinals.get(keyCode);
        if (ordinals == null) {
            Objects.checkIndex(vertex, vertexCount);
            return offsets.getLong(vertex + 1L);
        }
        final long owner = ownerIndex(ordinals, vertex);
        return owner < 0 ? 0 : offsets.getLong(owner + 1);
    }

    /**
     * The value of a vertex property, or null if the value is null.
     *
     * @param vertexProperty an ordinal in a range given by {@link #vertexPropertyStart(int, int)}
     */
    public Object vertexPropertyValue(final int keyCode, final long vertexProperty) {
        requireFull("vertex properties");
        return vertexProperties.get(keyCode).getAt(vertexProperty);
    }

    /**
     * The identifier of a vertex property.
     *
     * @param vertexProperty an ordinal in a range given by {@link #vertexPropertyStart(int, int)}
     */
    public Object vertexPropertyIdentifier(final int keyCode, final long vertexProperty) {
        requireFull("vertex-property identifiers");
        return vertexPropertyIds.get(keyCode).getAt(vertexProperty);
    }

    /**
     * Whether some vertex has more than one vertex property for the key.
     */
    public boolean isMultiProperty(final int keyCode) {
        requireFull("vertex properties");
        return multiKeys[keyCode];
    }

    /**
     * The meta-property key dictionary; the code of a key is its index. Empty unless the layout is {@code FULL}.
     */
    public List<String> metaPropertyKeys() {
        return Collections.unmodifiableList(metaKeys);
    }

    /**
     * The code of the meta-property key, or -1 if no vertex property has it.
     */
    public int metaKeyCodeOf(final String metaKey) {
        final Integer code = metaKeyCodes.get(metaKey);
        return code == null ? -1 : code;
    }

    /**
     * The meta-property key codes used on vertex properties of the key, in ascending order. The array is shared and
     * must not be modified.
     *
     * @throws UnsupportedOperationException if the layout is not {@code FULL}
     */
    public int[] metaKeyCodes(final int keyCode) {
        requireFull("meta-properties");
        return metaCodes.get(keyCode);
    }

    /**
     * Whether the vertex property has a meta-property with the key, which may have a null value.
     */
    public boolean hasMetaProperty(final int keyCode, final int metaKeyCode, final long vertexProperty) {
        final ColumnReader column = metaColumn(keyCode, metaKeyCode);
        return column != null && column.isPresent(vertexProperty);
    }

    /**
     * The value of the vertex property's meta-property, or null when it has none or it is null; use
     * {@link #hasMetaProperty(int, int, long)} to tell the two apart.
     */
    public Object metaPropertyValue(final int keyCode, final int metaKeyCode, final long vertexProperty) {
        final ColumnReader column = metaColumn(keyCode, metaKeyCode);
        return column == null ? null : column.get(vertexProperty);
    }

    /**
     * The graph variables, decoded, in key-code order. Empty when the source had none or the layout is not
     * {@code FULL}. The map is read-only.
     */
    public Map<String, Object> variables() {
        return variablesView;
    }

    /**
     * The defaults and cardinalities of the source graph, {@link SourceDefaults#DEFAULT} if it reported none.
     */
    public SourceDefaults sourceDefaults() {
        return sourceDefaults;
    }

    // ---------------------------------------------------------------- lifecycle

    /**
     * Releases the segments. The mappings themselves are reclaimed by the garbage collector.
     */
    @Override
    public void close() {
        for (final MappedSegment segment : segments.values()) segment.close();
    }

    // ---------------------------------------------------------------- helpers

    // the index of the vertex among the sorted owner ordinals, or -1
    private static long ownerIndex(final MappedSegment ordinals, final int vertex) {
        long lo = 0;
        long hi = ordinals.count() - 1;
        while (lo <= hi) {
            final long mid = (lo + hi) >>> 1;
            final int v = ordinals.getInt(mid);
            if (v < vertex) {
                lo = mid + 1;
            } else if (v > vertex) {
                hi = mid - 1;
            } else {
                return mid;
            }
        }
        return -1;
    }

    private ColumnReader metaColumn(final int keyCode, final int metaKeyCode) {
        requireFull("meta-properties");
        final int index = Arrays.binarySearch(metaCodes.get(keyCode), metaKeyCode);
        return index < 0 ? null : metaColumns.get(keyCode)[index];
    }

    private static int coerced(final IdentifierIndex index, final Object id) {
        if (index.isExact() && (id instanceof Long || id instanceof Integer || id instanceof Short
                || id instanceof Byte)) {
            return index.lookupIntegral(((Number) id).longValue());
        }
        return index.lookup(id);
    }

    private static String label(final String[] dictionary, final int code) {
        if (code < 0 || code >= dictionary.length) {
            throw new IllegalStateException("Label code " + code + " is outside the dictionary of "
                    + dictionary.length + " labels");
        }
        return dictionary[code];
    }

    private void requireIdentity(final String what) {
        if (layout == SnapshotLayout.TOPOLOGY) throw unsupported(what);
    }

    private void requireFull(final String what) {
        if (layout != SnapshotLayout.FULL) throw unsupported(what);
    }

    private UnsupportedOperationException unsupported(final String what) {
        return new UnsupportedOperationException("The " + layout + " snapshot in " + root + " does not include "
                + what);
    }
}
