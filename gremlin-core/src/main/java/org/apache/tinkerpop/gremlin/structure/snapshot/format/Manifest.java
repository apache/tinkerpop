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
package org.apache.tinkerpop.gremlin.structure.snapshot.format;

import org.apache.tinkerpop.gremlin.structure.snapshot.spi.SourceDefaults;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The model of {@code manifest.json}. This is a plain mutable data holder so builders can fill it in as they go and
 * the JSON codec can populate it. It deliberately holds no timestamps, host names or builder names, so the same input
 * always produces the same manifest.
 * <p/>
 * Dictionaries are lists in which the index is the code: a label code indexes {@code vertexLabels} or
 * {@code edgeLabels} and a property-key code indexes {@code vertexKeys} or {@code edgeKeys}. Meta-property key codes
 * index {@code metaKeys} and variable ordinals index {@code variableKeys}. The per-key column lists, including
 * {@code vertexKeyInfos}, are parallel to the key lists and are empty unless the layout is
 * {@link SnapshotLayout#FULL}.
 * <p/>
 * Builders assign label, key, meta-key and variable-key codes in order of first appearance: vertex scan order, then
 * vertex-property order within a vertex, then meta-property order within a vertex property, and variables in
 * {@code scanVariables} order. A vertex key uses the {@link PropertyLayout#MULTI} layout if and only if some vertex has
 * more than one vertex property for it, and the vertex label layout is {@link VertexLabelLayout#MULTI} if and only if
 * some vertex has a label count other than one.
 */
public final class Manifest {

    /**
     * The internal format version written to manifests and segment headers.
     */
    public static final int FORMAT_VERSION = 1;

    /**
     * Encoding, value types and present count of one column.
     *
     * @param encoding     the file layout of the column
     * @param valueTypes   the observed value types, in ascending {@link ValueType#code()} order; more than one means
     *                     the column is mixed-type and has a {@code types.bin} segment
     * @param presentCount the number of elements with an entry, including null entries
     * @param nullCount    the number of present entries that are null; when greater than zero the column has a
     *                     {@code nulls.bin}. Null entries contribute no value type.
     * @param minValue     optional statistic: the smallest non-null value of a column whose value types are all
     *                     integral, or null when unknown
     * @param maxValue     optional statistic: the largest non-null value, see {@code minValue}
     */
    public record ColumnInfo(ColumnEncoding encoding, List<ValueType> valueTypes, long presentCount, long nullCount,
                             Long minValue, Long maxValue) {

        /**
         * A column without a known integral range.
         */
        public ColumnInfo(final ColumnEncoding encoding, final List<ValueType> valueTypes, final long presentCount,
                          final long nullCount) {
            this(encoding, valueTypes, presentCount, nullCount, null, null);
        }

        /**
         * A column without null values.
         */
        public ColumnInfo(final ColumnEncoding encoding, final List<ValueType> valueTypes, final long presentCount) {
            this(encoding, valueTypes, presentCount, 0);
        }

        /**
         * Whether the column has a recorded integral range.
         */
        public boolean hasRange() {
            return minValue != null && maxValue != null;
        }

        public boolean isMixed() {
            return valueTypes.size() > 1;
        }
    }

    /**
     * How vertex labels are stored: {@code SINGLE} is one code per vertex in {@code labels.bin}, {@code MULTI} is
     * {@code label-offsets.bin} and {@code label-codes.bin}.
     */
    public enum VertexLabelLayout {
        SINGLE, MULTI
    }

    /**
     * How the vertex properties of one key are stored: {@code SINGLE} if no vertex has more than one, {@code MULTI}
     * otherwise.
     */
    public enum PropertyLayout {
        SINGLE, MULTI
    }

    /**
     * The encoding of the owner files of a {@link PropertyLayout#MULTI} key: {@code DENSE} has only
     * {@code owner-offsets.bin} over all vertices, {@code SPARSE} also has {@code owner-ordinals.bin}.
     */
    public enum OwnerEncoding {
        DENSE, SPARSE
    }

    /**
     * Layout of the vertex properties of one key.
     *
     * @param layout        single or multi
     * @param ownerEncoding dense or sparse owners, null for the single layout
     * @param propertyCount the number of vertex properties of the key, which is the element count of its value,
     *                      identifier and meta columns in the multi layout. In the single layout it equals the present
     *                      count of the value column.
     * @param metaColumns   the meta-property columns by meta key code, in ascending code order. The element space of
     *                      each is the key's vertex-property ordinals, which in the single layout are the column entry
     *                      indexes. Empty when no vertex property of the key has meta-properties.
     */
    public record VertexKeyInfo(PropertyLayout layout, OwnerEncoding ownerEncoding, long propertyCount,
                                Map<Integer, ColumnInfo> metaColumns) {

        public VertexKeyInfo {
            metaColumns = new TreeMap<>(metaColumns);
        }
    }

    /**
     * Metadata of one published segment.
     *
     * @param path     the path relative to the bundle root, using {@code '/'} separators
     * @param width    the value width in bytes
     * @param count    the number of values
     * @param checksum the CRC32C of the payload, in the low 32 bits
     */
    public record SegmentInfo(String path, int width, long count, long checksum) {
    }

    private int formatVersion = FORMAT_VERSION;
    private String sourceId;
    private String sourceVersion;
    private SnapshotLayout layout;
    private long vertexCount;
    private long edgeCount;
    private int vertexOrdinalWidth = Integer.BYTES;
    private int edgeOrdinalWidth = Integer.BYTES;
    private List<String> vertexLabels = new ArrayList<>();
    private List<String> edgeLabels = new ArrayList<>();
    private List<String> vertexKeys = new ArrayList<>();
    private List<String> edgeKeys = new ArrayList<>();
    private VertexLabelLayout vertexLabelLayout = VertexLabelLayout.SINGLE;
    private SourceDefaults sourceDefaults = SourceDefaults.DEFAULT;
    private List<String> metaKeys = new ArrayList<>();
    private List<String> variableKeys = new ArrayList<>();
    private List<VertexKeyInfo> vertexKeyInfos = new ArrayList<>();
    private ColumnInfo variables;
    private ColumnInfo vertexIds;
    private ColumnInfo edgeIds;
    private List<ColumnInfo> vertexProperties = new ArrayList<>();
    private List<ColumnInfo> vertexPropertyIds = new ArrayList<>();
    private List<ColumnInfo> edgeProperties = new ArrayList<>();
    private long[] vertexLabelCounts;
    private long[] edgeLabelCounts;
    private boolean outEdgesImplicit;
    private boolean edgeIdIndex;
    private boolean vertexIdIndexExact;
    private boolean edgeIdIndexExact;
    private List<SegmentInfo> segments = new ArrayList<>();

    public int getFormatVersion() {
        return formatVersion;
    }

    public void setFormatVersion(final int formatVersion) {
        this.formatVersion = formatVersion;
    }

    /**
     * {@code sourceId} of the {@code SourceVersion} the snapshot was built from.
     */
    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(final String sourceId) {
        this.sourceId = sourceId;
    }

    /**
     * {@code version} of the {@code SourceVersion} the snapshot was built from.
     */
    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(final String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public SnapshotLayout getLayout() {
        return layout;
    }

    public void setLayout(final SnapshotLayout layout) {
        this.layout = layout;
    }

    public long getVertexCount() {
        return vertexCount;
    }

    public void setVertexCount(final long vertexCount) {
        this.vertexCount = vertexCount;
    }

    public long getEdgeCount() {
        return edgeCount;
    }

    public void setEdgeCount(final long edgeCount) {
        this.edgeCount = edgeCount;
    }

    /**
     * Width in bytes of vertex ordinals, currently always 4.
     */
    public int getVertexOrdinalWidth() {
        return vertexOrdinalWidth;
    }

    public void setVertexOrdinalWidth(final int vertexOrdinalWidth) {
        this.vertexOrdinalWidth = vertexOrdinalWidth;
    }

    /**
     * Width in bytes of edge ordinals, currently always 4.
     */
    public int getEdgeOrdinalWidth() {
        return edgeOrdinalWidth;
    }

    public void setEdgeOrdinalWidth(final int edgeOrdinalWidth) {
        this.edgeOrdinalWidth = edgeOrdinalWidth;
    }

    /**
     * Vertex label dictionary, in order of first appearance. Empty for {@link SnapshotLayout#TOPOLOGY}.
     */
    public List<String> getVertexLabels() {
        return vertexLabels;
    }

    public void setVertexLabels(final List<String> vertexLabels) {
        this.vertexLabels = vertexLabels;
    }

    public List<String> getEdgeLabels() {
        return edgeLabels;
    }

    public void setEdgeLabels(final List<String> edgeLabels) {
        this.edgeLabels = edgeLabels;
    }

    /**
     * Vertex property-key dictionary, in order of first appearance. Empty unless the layout is
     * {@link SnapshotLayout#FULL}.
     */
    public List<String> getVertexKeys() {
        return vertexKeys;
    }

    public void setVertexKeys(final List<String> vertexKeys) {
        this.vertexKeys = vertexKeys;
    }

    public List<String> getEdgeKeys() {
        return edgeKeys;
    }

    public void setEdgeKeys(final List<String> edgeKeys) {
        this.edgeKeys = edgeKeys;
    }

    /**
     * How vertex labels are stored. Meaningful only when the layout publishes labels, that is not
     * {@link SnapshotLayout#TOPOLOGY}.
     */
    public VertexLabelLayout getVertexLabelLayout() {
        return vertexLabelLayout;
    }

    public void setVertexLabelLayout(final VertexLabelLayout vertexLabelLayout) {
        this.vertexLabelLayout = vertexLabelLayout;
    }

    /**
     * The source's default labels and cardinalities, see {@link SourceDefaults}.
     */
    public SourceDefaults getSourceDefaults() {
        return sourceDefaults;
    }

    public void setSourceDefaults(final SourceDefaults sourceDefaults) {
        this.sourceDefaults = sourceDefaults;
    }

    /**
     * Meta-property key dictionary, in order of first appearance. Empty unless the layout is
     * {@link SnapshotLayout#FULL}.
     */
    public List<String> getMetaKeys() {
        return metaKeys;
    }

    public void setMetaKeys(final List<String> metaKeys) {
        this.metaKeys = metaKeys;
    }

    /**
     * Graph variable key dictionary, in {@code scanVariables} order; a key's code is its variable ordinal. Empty
     * unless the layout is {@link SnapshotLayout#FULL}.
     */
    public List<String> getVariableKeys() {
        return variableKeys;
    }

    public void setVariableKeys(final List<String> variableKeys) {
        this.variableKeys = variableKeys;
    }

    /**
     * The layout of each vertex key's vertex properties, parallel to {@link #getVertexKeys()}.
     */
    public List<VertexKeyInfo> getVertexKeyInfos() {
        return vertexKeyInfos;
    }

    public void setVertexKeyInfos(final List<VertexKeyInfo> vertexKeyInfos) {
        this.vertexKeyInfos = vertexKeyInfos;
    }

    /**
     * The graph variables column, whose element space is the variable ordinals, or null when the source has no
     * variables or the layout is not {@link SnapshotLayout#FULL}.
     */
    public ColumnInfo getVariables() {
        return variables;
    }

    public void setVariables(final ColumnInfo variables) {
        this.variables = variables;
    }

    /**
     * The vertex identifier column, or null for {@link SnapshotLayout#TOPOLOGY}.
     */
    public ColumnInfo getVertexIds() {
        return vertexIds;
    }

    public void setVertexIds(final ColumnInfo vertexIds) {
        this.vertexIds = vertexIds;
    }

    /**
     * The edge identifier column, or null for {@link SnapshotLayout#TOPOLOGY}.
     */
    public ColumnInfo getEdgeIds() {
        return edgeIds;
    }

    public void setEdgeIds(final ColumnInfo edgeIds) {
        this.edgeIds = edgeIds;
    }

    /**
     * Vertex property value columns, parallel to {@link #getVertexKeys()}. The element space of a column is the vertex
     * ordinals in the single layout and the key's vertex-property ordinals in the multi layout.
     */
    public List<ColumnInfo> getVertexProperties() {
        return vertexProperties;
    }

    public void setVertexProperties(final List<ColumnInfo> vertexProperties) {
        this.vertexProperties = vertexProperties;
    }

    /**
     * Vertex-property identifier columns, parallel to {@link #getVertexKeys()}.
     */
    public List<ColumnInfo> getVertexPropertyIds() {
        return vertexPropertyIds;
    }

    public void setVertexPropertyIds(final List<ColumnInfo> vertexPropertyIds) {
        this.vertexPropertyIds = vertexPropertyIds;
    }

    /**
     * Edge property value columns, parallel to {@link #getEdgeKeys()}.
     */
    public List<ColumnInfo> getEdgeProperties() {
        return edgeProperties;
    }

    public void setEdgeProperties(final List<ColumnInfo> edgeProperties) {
        this.edgeProperties = edgeProperties;
    }

    /**
     * Optional statistic: the number of vertices with each vertex label, indexed by label code; a multi-label vertex
     * counts once for each of its labels. Null when absent, in which case the reader computes it at open.
     */
    public long[] getVertexLabelCounts() {
        return vertexLabelCounts;
    }

    public void setVertexLabelCounts(final long[] vertexLabelCounts) {
        this.vertexLabelCounts = vertexLabelCounts;
    }

    /**
     * Optional statistic: the number of edges with each edge label, indexed by label code. Null when absent.
     */
    public long[] getEdgeLabelCounts() {
        return edgeLabelCounts;
    }

    public void setEdgeLabelCounts(final long[] edgeLabelCounts) {
        this.edgeLabelCounts = edgeLabelCounts;
    }

    /**
     * True when {@code out-edges.bin} is omitted because each edge's out-adjacency position equals its edge ordinal.
     */
    public boolean isOutEdgesImplicit() {
        return outEdgesImplicit;
    }

    public void setOutEdgesImplicit(final boolean outEdgesImplicit) {
        this.outEdgesImplicit = outEdgesImplicit;
    }

    /**
     * True when the edge identifier index is published.
     */
    public boolean isEdgeIdIndex() {
        return edgeIdIndex;
    }

    public void setEdgeIdIndex(final boolean edgeIdIndex) {
        this.edgeIdIndex = edgeIdIndex;
    }

    /**
     * True when the vertex identifier column holds a single integral type, so index keys are the identifiers themselves.
     */
    public boolean isVertexIdIndexExact() {
        return vertexIdIndexExact;
    }

    public void setVertexIdIndexExact(final boolean vertexIdIndexExact) {
        this.vertexIdIndexExact = vertexIdIndexExact;
    }

    /**
     * True when the edge identifier column holds a single integral type. Meaningful only when the edge identifier
     * index is published.
     */
    public boolean isEdgeIdIndexExact() {
        return edgeIdIndexExact;
    }

    public void setEdgeIdIndexExact(final boolean edgeIdIndexExact) {
        this.edgeIdIndexExact = edgeIdIndexExact;
    }

    /**
     * Every published segment, in ascending path order.
     */
    public List<SegmentInfo> getSegments() {
        return segments;
    }

    public void setSegments(final List<SegmentInfo> segments) {
        this.segments = segments;
    }
}
