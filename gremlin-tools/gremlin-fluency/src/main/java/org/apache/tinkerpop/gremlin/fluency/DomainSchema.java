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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.gremlin.process.traversal.GType;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A domain schema from the gremlin-test resources, written in the subset of GQL graph type notation that the
 * developer documentation describes. Besides labels, endpoints and the type of each value, a schema marks the edge
 * types that are understood to form a hierarchy, where a vertex has at most one outgoing edge of that label and
 * following them never returns to the start, or a containment, where a vertex has at most one incoming edge of that
 * label. No edge in a domain graph connects a vertex to itself.
 */
final class DomainSchema {
    static final String SCHEMAS_ROOT = "org/apache/tinkerpop/gremlin/test/schemas";

    enum Shape { FREE, HIERARCHY, CONTAINMENT }

    private static final Pattern VERTEX_TYPE = Pattern.compile("^\\(:(\\w+)\\s*=>\\s*\\{(.*)}\\s*\\)$", Pattern.DOTALL);
    private static final Pattern EDGE_TYPE = Pattern.compile(
            "^\\(:(\\w+)\\)-\\[:(\\w+)\\s*(?:\\{(.*)})?\\s*]->\\(:(\\w+)\\)\\s*(?:/\\*\\s*(\\w+)\\s*\\*/)?$", Pattern.DOTALL);
    private static final Pattern KEY = Pattern.compile("^(\\w+)\\s*::\\s*(.*)$", Pattern.DOTALL);
    private static final Pattern COMMENT = Pattern.compile("/\\*(.*?)\\*/", Pattern.DOTALL);
    private static final Pattern LIST = Pattern.compile("^LIST<(.+)>$");

    private static List<DomainSchema> schemas;

    private final String name;
    private final Map<String, Map<String, ValueType>> vertexTypes = new LinkedHashMap<>();
    private final Map<String, EdgeType> edgeTypes = new LinkedHashMap<>();

    private DomainSchema(final String name) {
        this.name = name;
    }

    String getName() {
        return name;
    }

    EdgeType getEdgeType(final String label) {
        return edgeTypes.get(label);
    }

    /**
     * The schemas in the gremlin-test jar, or directory, on the classpath.
     */
    static synchronized List<DomainSchema> all() {
        if (null == schemas)
            schemas = Collections.unmodifiableList(Corpus.fromClasspathDirectory(SCHEMAS_ROOT, DomainSchema::fromDirectory));
        return schemas;
    }

    /**
     * The schema whose vertex labels include every label in the graph, or {@code null} for an empty graph or one that
     * follows no domain schema, such as the preloaded graphs.
     */
    static DomainSchema forGraph(final Graph graph) {
        final Set<String> labels = new HashSet<>();
        graph.vertices().forEachRemaining(v -> labels.add(v.label()));
        if (labels.isEmpty()) return null;
        return all().stream().filter(s -> s.vertexTypes.keySet().containsAll(labels)).findFirst().orElse(null);
    }

    static List<DomainSchema> fromDirectory(final Path directory) throws IOException {
        final List<Path> files;
        try (Stream<Path> paths = Files.list(directory)) {
            files = paths.filter(p -> p.toString().endsWith(".gql")).sorted().collect(Collectors.toList());
        }
        final List<DomainSchema> parsed = new ArrayList<>();
        for (Path file : files) {
            final String fileName = file.getFileName().toString();
            parsed.add(parse(fileName.substring(0, fileName.length() - ".gql".length()),
                    new String(Files.readAllBytes(file), StandardCharsets.UTF_8)));
        }
        return parsed;
    }

    /**
     * Parses a schema, reading only what lies between the {@code tag::schema[]} and {@code end::schema[]} markers
     * when the text has them.
     */
    static DomainSchema parse(final String name, final String text) {
        final int start = text.indexOf("tag::schema[]");
        final int end = text.indexOf("end::schema[]");
        final String body = start < 0 || end < 0 ? text :
                text.substring(text.indexOf('\n', start) + 1, text.lastIndexOf('\n', end));

        final DomainSchema schema = new DomainSchema(name);
        for (String entry : split(body)) {
            final Matcher vertex = VERTEX_TYPE.matcher(entry);
            final Matcher edge = EDGE_TYPE.matcher(entry);
            if (vertex.matches()) {
                schema.vertexTypes.put(vertex.group(1), keys(vertex.group(2)));
            } else if (edge.matches()) {
                final Shape shape = null == edge.group(5) ? Shape.FREE : Shape.valueOf(edge.group(5).toUpperCase());
                schema.edgeTypes.put(edge.group(2), new EdgeType(edge.group(2), edge.group(1), edge.group(4),
                        null == edge.group(3) ? Collections.emptyMap() : keys(edge.group(3)), shape));
            } else {
                throw new IllegalArgumentException(String.format("The %s schema has an entry that is neither a vertex nor an edge type: %s", name, entry));
            }
        }
        return schema;
    }

    /**
     * Describes everything in the graph that the schema or its conventions rule out, or nothing if the graph follows
     * the schema.
     */
    List<String> violations(final Graph graph) {
        final List<String> violations = new ArrayList<>();
        graph.vertices().forEachRemaining(v -> {
            final Map<String, ValueType> keys = vertexTypes.get(v.label());
            if (null == keys) {
                violations.add(String.format("%s has the label %s, which the schema does not define", describe(v), v.label()));
                return;
            }
            final Map<String, Integer> counts = new HashMap<>();
            v.properties().forEachRemaining(vp -> {
                counts.merge(vp.key(), 1, Integer::sum);
                check(v, vp, keys.get(vp.key()), violations);
            });
            counts.forEach((key, count) -> {
                final ValueType type = keys.get(key);
                if (count > 1 && null != type && !type.multi)
                    violations.add(String.format("%s has %d values for %s, which is not a multi-property", describe(v), count, key));
            });
        });

        graph.edges().forEachRemaining(e -> {
            final EdgeType type = edgeTypes.get(e.label());
            if (null == type) {
                violations.add(String.format("%s has the label %s, which the schema does not define", describe(e), e.label()));
                return;
            }
            if (!e.outVertex().label().equals(type.outLabel) || !e.inVertex().label().equals(type.inLabel))
                violations.add(String.format("%s connects %s to %s, where the schema has %s to %s", describe(e),
                        e.outVertex().label(), e.inVertex().label(), type.outLabel, type.inLabel));
            if (e.outVertex().id().equals(e.inVertex().id()))
                violations.add(String.format("%s connects a vertex to itself", describe(e)));
            e.properties().forEachRemaining(p -> check(e, p, type.keys.get(p.key()), violations));
        });

        for (EdgeType type : edgeTypes.values()) {
            if (type.shape == Shape.FREE) continue;
            graph.vertices().forEachRemaining(v -> {
                final long count = IteratorUtils.count(type.shape == Shape.HIERARCHY ?
                        v.edges(Direction.OUT, type.label) :
                        v.edges(Direction.IN, type.label));
                if (count > 1)
                    violations.add(String.format("%s has %d %s %s edges, where %s is a %s", describe(v), count,
                            type.shape == Shape.HIERARCHY ? "outgoing" : "incoming", type.label, type.label,
                            type.shape.name().toLowerCase()));
            });
            if (type.shape == Shape.HIERARCHY) {
                graph.vertices().forEachRemaining(v -> {
                    if (reaches(v, v, type.label))
                        violations.add(String.format("%s leads back to itself through %s edges", describe(v), type.label));
                });
            }
        }
        return violations;
    }

    /**
     * Determines if following outgoing edges with the label from {@code from} arrives at {@code to}, taking at least
     * one step.
     */
    static boolean reaches(final Vertex from, final Vertex to, final String label) {
        final Set<Object> seen = new HashSet<>();
        final List<Vertex> frontier = new ArrayList<>();
        frontier.add(from);
        while (!frontier.isEmpty()) {
            final Vertex current = frontier.remove(frontier.size() - 1);
            final Iterator<Vertex> next = current.vertices(Direction.OUT, label);
            while (next.hasNext()) {
                final Vertex v = next.next();
                if (v.id().equals(to.id())) return true;
                if (seen.add(v.id())) frontier.add(v);
            }
        }
        return false;
    }

    private static void check(final Element element, final Property<?> property, final ValueType type,
                              final List<String> violations) {
        if (null == type) {
            violations.add(String.format("%s has the key %s, which the schema does not define for %s",
                    describe(element), property.key(), element.label()));
        } else if (null != type.type && !type.type.getType().isInstance(property.value())) {
            violations.add(String.format("%s has %s as a %s, where the schema has %s", describe(element),
                    property.key(), property.value().getClass().getSimpleName(), type.type));
        }
    }

    private static String describe(final Element element) {
        return (element instanceof Vertex ? "vertex " : "edge ") + element.id();
    }

    private static Map<String, ValueType> keys(final String body) {
        final Map<String, ValueType> keys = new LinkedHashMap<>();
        for (String entry : split(body)) {
            final Matcher key = KEY.matcher(entry);
            if (!key.matches()) throw new IllegalArgumentException("Not a property type: " + entry);
            keys.put(key.group(1), valueType(key.group(2)));
        }
        return keys;
    }

    /**
     * Reads a GQL value type. A comment names the TinkerPop type where the GQL type does not, a {@code LIST} is a
     * multi-property of its element type and {@code ANY} allows any type.
     */
    private static ValueType valueType(final String text) {
        final Matcher comment = COMMENT.matcher(text);
        final String note = comment.find() ? comment.group(1).trim() : "";
        final String gql = COMMENT.matcher(text).replaceAll("").replace("NOT NULL", "").trim();

        if (gql.equals("ANY")) return new ValueType(null, false);
        final Matcher list = LIST.matcher(gql);
        if (list.matches()) return new ValueType(gqlType(list.group(1)), true);

        final String named = note.split("\\s+")[0];
        for (GType type : GType.values()) {
            if (type.name().equals(named)) return new ValueType(type, false);
        }
        return new ValueType(gqlType(gql), false);
    }

    private static GType gqlType(final String gql) {
        switch (gql) {
            case "STRING": return GType.STRING;
            case "BOOLEAN": return GType.BOOLEAN;
            case "INT": return GType.INT;
            case "DOUBLE": return GType.DOUBLE;
            case "UUID": return GType.UUID;
            case "DURATION": return GType.DURATION;
            case "ZONED DATETIME": return GType.DATETIME;
            default: throw new IllegalArgumentException("No TinkerPop type is known for the GQL type " + gql);
        }
    }

    /**
     * Splits on the commas that are not inside brackets or comments.
     */
    private static List<String> split(final String text) {
        final List<String> parts = new ArrayList<>();
        int depth = 0;
        int from = 0;
        for (int i = 0; i < text.length(); i++) {
            final char c = text.charAt(i);
            if (c == '/' && i + 1 < text.length() && text.charAt(i + 1) == '*') {
                i = text.indexOf("*/", i + 2) + 1;
                if (i == 0) throw new IllegalArgumentException("Unclosed comment in " + text);
            } else if (c == '(' || c == '[' || c == '{' || c == '<') {
                depth++;
            } else if (c == ')' || c == ']' || c == '}' || (c == '>' && text.charAt(i - 1) != '-' && text.charAt(i - 1) != '=')) {
                depth--;
            } else if (c == ',' && depth == 0) {
                parts.add(text.substring(from, i).trim());
                from = i + 1;
            }
        }
        final String last = text.substring(from).trim();
        if (!last.isEmpty()) parts.add(last);
        return parts;
    }

    static final class EdgeType {
        final String label;
        final String outLabel;
        final String inLabel;
        final Map<String, ValueType> keys;
        final Shape shape;

        private EdgeType(final String label, final String outLabel, final String inLabel,
                         final Map<String, ValueType> keys, final Shape shape) {
            this.label = label;
            this.outLabel = outLabel;
            this.inLabel = inLabel;
            this.keys = keys;
            this.shape = shape;
        }
    }

    static final class ValueType {
        /**
         * The type of each value, or {@code null} where any type is allowed.
         */
        final GType type;
        final boolean multi;

        private ValueType(final GType type, final boolean multi) {
            this.type = type;
            this.multi = multi;
        }
    }
}
