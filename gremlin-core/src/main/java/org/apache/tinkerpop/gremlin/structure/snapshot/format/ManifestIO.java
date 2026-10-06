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
import org.apache.tinkerpop.shaded.jackson.core.JsonFactory;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.core.util.DefaultIndenter;
import org.apache.tinkerpop.shaded.jackson.core.util.DefaultPrettyPrinter;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reads and writes {@code manifest.json}. The output is deterministic: fields are written in a fixed order, the
 * segment list is sorted by path, indentation uses two spaces and {@code '\n'} regardless of platform, and nothing
 * depends on time, host or builder. The same {@link Manifest} content therefore always yields the same bytes.
 * <p/>
 * The JSON is written by hand rather than by bean reflection so the field order and the handling of the
 * {@link Manifest} records are fixed by this class alone. I/O and parse failures are reported as
 * {@link UncheckedIOException}.
 */
public final class ManifestIO {

    private static final JsonFactory FACTORY = new JsonFactory();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private ManifestIO() {
    }

    /**
     * Writes {@code manifest} to {@code path} as UTF-8, replacing an existing file.
     */
    public static void write(final Manifest manifest, final Path path) {
        try {
            Files.write(path, toJson(manifest).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Reads a manifest file written by {@link #write}.
     *
     * @throws UncheckedIOException if the file cannot be read or does not hold a valid manifest
     */
    public static Manifest read(final Path path) {
        try {
            return fromJson(new String(Files.readAllBytes(path), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Serializes {@code manifest} to its deterministic JSON text, ending with a newline. The segment list is written
     * sorted by path.
     */
    public static String toJson(final Manifest m) {
        final StringWriter out = new StringWriter();
        try (JsonGenerator g = FACTORY.createGenerator(out)) {
            final DefaultPrettyPrinter pp = new DefaultPrettyPrinter();
            final DefaultIndenter indenter = new DefaultIndenter("  ", "\n");
            pp.indentObjectsWith(indenter);
            pp.indentArraysWith(indenter);
            g.setPrettyPrinter(pp);

            g.writeStartObject();
            g.writeNumberField("formatVersion", m.getFormatVersion());
            g.writeStringField("sourceId", m.getSourceId());
            g.writeStringField("sourceVersion", m.getSourceVersion());
            g.writeStringField("layout", m.getLayout() == null ? null : m.getLayout().name());
            g.writeNumberField("vertexCount", m.getVertexCount());
            g.writeNumberField("edgeCount", m.getEdgeCount());
            g.writeNumberField("vertexOrdinalWidth", m.getVertexOrdinalWidth());
            g.writeNumberField("edgeOrdinalWidth", m.getEdgeOrdinalWidth());
            writeStrings(g, "vertexLabels", m.getVertexLabels());
            writeStrings(g, "edgeLabels", m.getEdgeLabels());
            writeStrings(g, "vertexKeys", m.getVertexKeys());
            writeStrings(g, "edgeKeys", m.getEdgeKeys());
            writeStrings(g, "metaKeys", m.getMetaKeys());
            writeStrings(g, "variableKeys", m.getVariableKeys());
            g.writeStringField("vertexLabelLayout", m.getVertexLabelLayout().name());
            g.writeObjectFieldStart("sourceDefaults");
            g.writeStringField("defaultVertexLabel", m.getSourceDefaults().defaultVertexLabel());
            g.writeStringField("defaultEdgeLabel", m.getSourceDefaults().defaultEdgeLabel());
            g.writeStringField("vertexLabelCardinality", m.getSourceDefaults().vertexLabelCardinality());
            g.writeStringField("defaultVertexPropertyCardinality",
                    m.getSourceDefaults().defaultVertexPropertyCardinality());
            g.writeEndObject();
            g.writeFieldName("vertexIds");
            writeColumn(g, m.getVertexIds());
            g.writeFieldName("edgeIds");
            writeColumn(g, m.getEdgeIds());
            writeColumns(g, "vertexProperties", m.getVertexProperties());
            writeColumns(g, "vertexPropertyIds", m.getVertexPropertyIds());
            writeColumns(g, "edgeProperties", m.getEdgeProperties());
            g.writeArrayFieldStart("vertexKeyInfos");
            for (final Manifest.VertexKeyInfo info : m.getVertexKeyInfos()) {
                g.writeStartObject();
                g.writeStringField("layout", info.layout().name());
                g.writeStringField("ownerEncoding", info.ownerEncoding() == null ? null : info.ownerEncoding().name());
                g.writeNumberField("propertyCount", info.propertyCount());
                g.writeArrayFieldStart("metaColumns");
                for (final Map.Entry<Integer, Manifest.ColumnInfo> meta : info.metaColumns().entrySet()) {
                    g.writeStartObject();
                    g.writeNumberField("metaKeyCode", meta.getKey());
                    g.writeFieldName("column");
                    writeColumn(g, meta.getValue());
                    g.writeEndObject();
                }
                g.writeEndArray();
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeFieldName("variables");
            writeColumn(g, m.getVariables());
            writeCounts(g, "vertexLabelCounts", m.getVertexLabelCounts());
            writeCounts(g, "edgeLabelCounts", m.getEdgeLabelCounts());
            g.writeBooleanField("outEdgesImplicit", m.isOutEdgesImplicit());
            g.writeBooleanField("edgeIdIndex", m.isEdgeIdIndex());
            g.writeBooleanField("vertexIdIndexExact", m.isVertexIdIndexExact());
            g.writeBooleanField("edgeIdIndexExact", m.isEdgeIdIndexExact());

            final List<Manifest.SegmentInfo> sorted = new ArrayList<>(m.getSegments());
            sorted.sort(Comparator.comparing(Manifest.SegmentInfo::path));
            g.writeArrayFieldStart("segments");
            for (final Manifest.SegmentInfo s : sorted) {
                g.writeStartObject();
                g.writeStringField("path", s.path());
                g.writeNumberField("width", s.width());
                g.writeNumberField("count", s.count());
                g.writeNumberField("checksum", s.checksum());
                g.writeEndObject();
            }
            g.writeEndArray();
            g.writeEndObject();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toString() + "\n";
    }

    /**
     * Parses the JSON text produced by {@link #toJson}.
     *
     * @throws UncheckedIOException if the text is not a valid manifest
     */
    public static Manifest fromJson(final String json) {
        final JsonNode root;
        try {
            root = MAPPER.readTree(json);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        if (root == null || !root.isObject()) throw invalid("manifest is not a JSON object");

        final Manifest m = new Manifest();
        m.setFormatVersion(required(root, "formatVersion").asInt());
        m.setSourceId(optionalText(root, "sourceId"));
        m.setSourceVersion(optionalText(root, "sourceVersion"));
        final String layout = optionalText(root, "layout");
        if (layout != null) {
            try {
                m.setLayout(SnapshotLayout.valueOf(layout));
            } catch (IllegalArgumentException e) {
                throw invalid("unknown layout " + layout);
            }
        }
        m.setVertexCount(required(root, "vertexCount").asLong());
        m.setEdgeCount(required(root, "edgeCount").asLong());
        m.setVertexOrdinalWidth(required(root, "vertexOrdinalWidth").asInt());
        m.setEdgeOrdinalWidth(required(root, "edgeOrdinalWidth").asInt());
        m.setVertexLabels(readStrings(required(root, "vertexLabels")));
        m.setEdgeLabels(readStrings(required(root, "edgeLabels")));
        m.setVertexKeys(readStrings(required(root, "vertexKeys")));
        m.setEdgeKeys(readStrings(required(root, "edgeKeys")));
        m.setMetaKeys(readStrings(required(root, "metaKeys")));
        m.setVariableKeys(readStrings(required(root, "variableKeys")));
        m.setVertexLabelLayout(readEnum(Manifest.VertexLabelLayout.class, required(root, "vertexLabelLayout")));
        final JsonNode defaults = required(root, "sourceDefaults");
        m.setSourceDefaults(new SourceDefaults(required(defaults, "defaultVertexLabel").asText(),
                required(defaults, "defaultEdgeLabel").asText(), required(defaults, "vertexLabelCardinality").asText(),
                required(defaults, "defaultVertexPropertyCardinality").asText()));
        m.setVertexIds(readColumn(root.get("vertexIds")));
        m.setEdgeIds(readColumn(root.get("edgeIds")));
        m.setVertexProperties(readColumns(required(root, "vertexProperties")));
        m.setVertexPropertyIds(readColumns(required(root, "vertexPropertyIds")));
        m.setEdgeProperties(readColumns(required(root, "edgeProperties")));
        final List<Manifest.VertexKeyInfo> keyInfos = new ArrayList<>();
        for (final JsonNode n : required(root, "vertexKeyInfos")) {
            final String owner = optionalText(n, "ownerEncoding");
            final Map<Integer, Manifest.ColumnInfo> metaColumns = new TreeMap<>();
            for (final JsonNode meta : required(n, "metaColumns")) {
                metaColumns.put(required(meta, "metaKeyCode").asInt(), readColumn(required(meta, "column")));
            }
            keyInfos.add(new Manifest.VertexKeyInfo(readEnum(Manifest.PropertyLayout.class, required(n, "layout")),
                    owner == null ? null : readEnum(Manifest.OwnerEncoding.class, n.get("ownerEncoding")),
                    required(n, "propertyCount").asLong(), metaColumns));
        }
        m.setVertexKeyInfos(keyInfos);
        m.setVariables(readColumn(root.get("variables")));
        m.setVertexLabelCounts(readCounts(root.get("vertexLabelCounts")));
        m.setEdgeLabelCounts(readCounts(root.get("edgeLabelCounts")));
        m.setOutEdgesImplicit(required(root, "outEdgesImplicit").asBoolean());
        m.setEdgeIdIndex(required(root, "edgeIdIndex").asBoolean());
        m.setVertexIdIndexExact(required(root, "vertexIdIndexExact").asBoolean());
        m.setEdgeIdIndexExact(required(root, "edgeIdIndexExact").asBoolean());

        final List<Manifest.SegmentInfo> segments = new ArrayList<>();
        for (final JsonNode s : required(root, "segments")) {
            segments.add(new Manifest.SegmentInfo(required(s, "path").asText(), required(s, "width").asInt(),
                    required(s, "count").asLong(), required(s, "checksum").asLong()));
        }
        m.setSegments(segments);
        return m;
    }

    private static void writeStrings(final JsonGenerator g, final String name, final List<String> values)
            throws IOException {
        g.writeArrayFieldStart(name);
        for (final String v : values) g.writeString(v);
        g.writeEndArray();
    }

    // optional: absent when null
    private static void writeCounts(final JsonGenerator g, final String name, final long[] counts) throws IOException {
        if (counts == null) return;
        g.writeArrayFieldStart(name);
        for (final long c : counts) g.writeNumber(c);
        g.writeEndArray();
    }

    private static long[] readCounts(final JsonNode array) {
        if (array == null || array.isNull()) return null;
        final long[] out = new long[array.size()];
        for (int i = 0; i < out.length; i++) out[i] = array.get(i).asLong();
        return out;
    }

    private static void writeColumns(final JsonGenerator g, final String name, final List<Manifest.ColumnInfo> columns)
            throws IOException {
        g.writeArrayFieldStart(name);
        for (final Manifest.ColumnInfo c : columns) writeColumn(g, c);
        g.writeEndArray();
    }

    private static void writeColumn(final JsonGenerator g, final Manifest.ColumnInfo c) throws IOException {
        if (c == null) {
            g.writeNull();
            return;
        }
        g.writeStartObject();
        g.writeStringField("encoding", c.encoding().name());
        g.writeArrayFieldStart("valueTypes");
        for (final ValueType t : c.valueTypes()) g.writeString(t.name());
        g.writeEndArray();
        g.writeNumberField("presentCount", c.presentCount());
        g.writeNumberField("nullCount", c.nullCount());
        if (c.hasRange()) {
            g.writeNumberField("minValue", c.minValue());
            g.writeNumberField("maxValue", c.maxValue());
        }
        g.writeEndObject();
    }

    private static List<String> readStrings(final JsonNode array) {
        final List<String> out = new ArrayList<>();
        for (final JsonNode n : array) out.add(n.asText());
        return out;
    }

    private static List<Manifest.ColumnInfo> readColumns(final JsonNode array) {
        final List<Manifest.ColumnInfo> out = new ArrayList<>();
        for (final JsonNode n : array) out.add(readColumn(n));
        return out;
    }

    private static Manifest.ColumnInfo readColumn(final JsonNode n) {
        if (n == null || n.isNull()) return null;
        final ColumnEncoding encoding;
        try {
            encoding = ColumnEncoding.valueOf(required(n, "encoding").asText());
        } catch (IllegalArgumentException e) {
            throw invalid("unknown column encoding " + n.get("encoding"));
        }
        final List<ValueType> types = new ArrayList<>();
        for (final JsonNode t : required(n, "valueTypes")) {
            try {
                types.add(ValueType.valueOf(t.asText()));
            } catch (IllegalArgumentException e) {
                throw invalid("unknown value type " + t);
            }
        }
        final JsonNode min = n.get("minValue");
        final JsonNode max = n.get("maxValue");
        final boolean range = min != null && !min.isNull() && max != null && !max.isNull();
        return new Manifest.ColumnInfo(encoding, types, required(n, "presentCount").asLong(),
                required(n, "nullCount").asLong(), range ? min.asLong() : null, range ? max.asLong() : null);
    }

    private static <E extends Enum<E>> E readEnum(final Class<E> type, final JsonNode n) {
        try {
            return Enum.valueOf(type, n.asText());
        } catch (IllegalArgumentException e) {
            throw invalid("unknown " + type.getSimpleName() + " " + n);
        }
    }

    private static JsonNode required(final JsonNode parent, final String field) {
        final JsonNode n = parent.get(field);
        if (n == null || n.isNull()) throw invalid("missing field " + field);
        return n;
    }

    private static String optionalText(final JsonNode parent, final String field) {
        final JsonNode n = parent.get(field);
        return n == null || n.isNull() ? null : n.asText();
    }

    private static UncheckedIOException invalid(final String message) {
        return new UncheckedIOException(new IOException("Invalid manifest: " + message));
    }
}
