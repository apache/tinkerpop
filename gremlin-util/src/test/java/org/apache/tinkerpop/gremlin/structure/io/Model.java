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
package org.apache.tinkerpop.gremlin.structure.io;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory.detach;

/**
 * Defines the supported types for IO and the versions (and configurations) to which they apply and are tested.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Model {

    private static final Model model = new Model();

    private final Map<String, List<Entry>> entries = new HashMap<>();
    
    private Model() {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name());
        final TinkerGraph graph = TinkerGraph.open(conf);
        TinkerFactory.generateTheCrew(graph);
        final GraphTraversalSource g = graph.traversal();

        // IMPORTANT - the "title" or name of the Entry needs to be unique
        addCoreEntry(Double.MAX_VALUE, "max-double");
        addCoreEntry(Double.MIN_VALUE, "min-double");
        addCoreEntry(-Double.MAX_VALUE, "neg-max-double");
        addCoreEntry(-Double.MIN_VALUE, "neg-min-double");
        addCoreEntry(Double.NaN, "nan-double");
        addCoreEntry(Double.POSITIVE_INFINITY, "pos-inf-double");
        addCoreEntry(Double.NEGATIVE_INFINITY, "neg-inf-double");
        addCoreEntry(-0.0, "neg-zero-double");

        addCoreEntry(Float.MAX_VALUE, "max-float");
        addCoreEntry(Float.MIN_VALUE, "min-float");
        addCoreEntry(-Float.MAX_VALUE, "neg-max-float");
        addCoreEntry(-Float.MIN_VALUE, "neg-min-float");
        addCoreEntry(Float.NaN, "nan-float");
        addCoreEntry(Float.POSITIVE_INFINITY, "pos-inf-float");
        addCoreEntry(Float.NEGATIVE_INFINITY, "neg-inf-float");
        addCoreEntry(-0.0f, "neg-zero-float");

        addCoreEntry(Integer.MAX_VALUE, "max-int");
        addCoreEntry(Integer.MIN_VALUE, "min-int");

        addCoreEntry(Long.MAX_VALUE, "max-long");
        addCoreEntry(Long.MIN_VALUE, "min-long");

        addCoreEntry(Arrays.asList(1, "person", true, null), "var-type-list", "List is used to distinguish between different collection types as JSON is not explicit enough for all of Gremlin's requirements.");
        addCoreEntry(Collections.EMPTY_LIST, "empty-list");

        final Map<Object,Object> map = new HashMap<>();
        map.put("test", 123);
        map.put(OffsetDateTime.ofInstant(Instant.ofEpochMilli(1481295L), ZoneOffset.UTC), "red");
        map.put(Arrays.asList(1,2,3), OffsetDateTime.ofInstant(Instant.ofEpochMilli(1481295L), ZoneOffset.UTC));
        map.put(null, null);
        addCoreEntry(map, "var-type-map", "Map is redefined so that to provide the ability to allow for non-String keys, which is not possible in JSON.");
        addCoreEntry(Collections.EMPTY_MAP, "empty-map");

        addCoreEntry(new HashSet<>(Arrays.asList(2, "person", true, null)), "var-type-set", "Allows a JSON collection to behave as a Set.");
        addCoreEntry(Collections.EMPTY_SET, "empty-set");

        addCoreEntry(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786"), "specified-uuid");
        addCoreEntry(new UUID(0, 0), "nil-uuid");

        addCoreEntry(Direction.OUT, "out-direction", "");
        addCoreEntry(T.id, "id-t", "");

        addCoreEntry('a', "single-byte-char", "");
        addCoreEntry('\u03A9', "multi-byte-char", "");

        addEntry("Core", () -> null, "unspecified-null", "");

        addCoreEntry(true, "true-boolean", "");
        addCoreEntry(false, "false-boolean", "");

        addCoreEntry("abc", "single-byte-string", "");
        addCoreEntry("abc\u0391\u0392\u0393", "mixed-string", "");

        addCoreEntry(g.V(10).out().tree().next(), "traversal-tree", "");

        addGraphStructureEntry(graph.edges().next(), "traversal-edge", "");
        addGraphStructureEntry(DetachedFactory.detach(graph.edges().next(), false), "no-prop-edge", "");

        addGraphStructureEntry(detach(g.V().out().out().path().next(), false), "traversal-path", "");
        addGraphStructureEntry(EmptyPath.instance(), "empty-path", "");
        addGraphStructureEntry(detach(g.V().out().out().path().next(), true), "prop-path", "");

        addGraphStructureEntry(graph.edges().next().properties().next(), "edge-property", "");
        addGraphStructureEntry(new DetachedProperty<>("", null), "null-property", "");

        addGraphStructureEntry(graph, "tinker-graph", "`TinkerGraph` has a custom serializer that is registered as part of the `TinkerIoRegistry`.");

        addGraphStructureEntry(graph.vertices().next(), "traversal-vertex", "");
        addGraphStructureEntry(DetachedFactory.detach(graph.vertices().next(), false), "no-prop-vertex", "");

        addGraphStructureEntry(graph.vertices().next().properties().next(), "traversal-vertexproperty", "");
        final Map<String,Object> metaProperties = new HashMap<>();
        metaProperties.put("a", "b");
        addGraphStructureEntry(new DetachedVertexProperty<>(1, "person", "stephen", metaProperties), "meta-vertexproperty", "");
        final Set<Object> setProperty = new HashSet<>();
        setProperty.add("stephen");
        setProperty.add("marko");
        addGraphStructureEntry(new DetachedVertexProperty<>(1, "person", setProperty, metaProperties), "set-cardinality-vertexproperty", "");

        final BulkSet<String> bulkSet = new BulkSet<>();
        bulkSet.add("marko", 1);
        bulkSet.add("josh", 2);
        addGraphProcessEntry(bulkSet, "var-bulklist", "");
        addGraphProcessEntry(new BulkSet(), "empty-bulklist", "");

        addGraphProcessEntry(g.V().hasLabel("person").asAdmin().nextTraverser(), "vertex-traverser", "");
        addGraphProcessEntry(g.V().both().barrier().asAdmin().nextTraverser(), "bulked-traverser", "");
        addGraphProcessEntry(EmptyTraverser.instance(), "empty-traverser", "");

        addExtendedEntry(new BigDecimal("123.456789987654321123456789987654321"), "pos-bigdecimal", "");
        addExtendedEntry(new BigDecimal("-123.456789987654321123456789987654321"), "neg-bigdecimal", "");

        addExtendedEntry(new BigInteger("123456789987654321123456789987654321"), "pos-biginteger", "");
        addExtendedEntry(new BigInteger("-123456789987654321123456789987654321"), "neg-biginteger", "");

        addExtendedEntry(Byte.MAX_VALUE, "max-byte", "");
        addExtendedEntry(Byte.MIN_VALUE, "min-byte", "");

        addExtendedEntry(ByteBuffer.wrap(new byte[0]), "empty-binary", "");
        addEntry("Extended", () -> java.nio.ByteBuffer.wrap("some bytes for you".getBytes(StandardCharsets.UTF_8)), "str-binary", "");

        addExtendedEntry(Duration.ZERO, "zero-duration","The following example is a zero `Duration`");
        addExtendedEntry(ChronoUnit.FOREVER.getDuration(), "forever-duration","");

        addExtendedEntry(OffsetDateTime.MAX, "max-offsetdatetime", "");
        addExtendedEntry(OffsetDateTime.MIN, "min-offsetdatetime", "");

        addExtendedEntry(Short.MIN_VALUE, "min-short", "");
        addExtendedEntry(Short.MAX_VALUE, "max-short", "");
    }

    public static Model instance() {
        return model;
    }

    public Set<String> groups() {
        return Collections.unmodifiableSet(entries.keySet());
    }

    public List<Entry> entries(final String key) {
        return Collections.unmodifiableList(entries.get(key));
    }

    public List<Entry> entries() {
        return Collections.unmodifiableList(entries.values().stream().flatMap(Collection::stream).collect(Collectors.toList()));
    }

    public Optional<Entry> find(final String resource) {
        return entries.values().stream().flatMap(Collection::stream).filter(e -> e.getResourceName().equals(resource)).findFirst();
    }

    private void addCoreEntry(final Object obj, final String title) {
        addEntry("Core", obj, title, "");
    }

    private void addCoreEntry(final Object obj, final String title, final String description) {
        addEntry("Core", obj, title, description);
    }

    private void addGraphProcessEntry(final Object obj, final String title, final String description) {
        addEntry("Graph Process", obj, title, description);
    }

    private void addRequestMessageEntry(final Object obj, final String title, final String description) {
        addEntry("RequestMessage", obj, title, description);
    }

    private void addResponseMessageEntry(final Object obj, final String title, final String description) {
        addEntry("ResponseMessage", obj, title, description);
    }

    private void addGraphStructureEntry(final Object obj, final String title, final String description) {
        addEntry("Graph Structure", obj, title, description);
    }

    private void addEntry(final String group, final Supplier<?> maker, final String title, final String description) {
        addEntry(group, null, title, description, maker);
    }

    private void addEntry(final String group, final Object obj, final String title, final String description,
                          final Supplier<?> maker) {
        if (!entries.containsKey(group))
            entries.put(group, new ArrayList<>());

        entries.get(group).add(new Entry(title, obj, description, maker));
    }

    private void addExtendedEntry(final Object obj, final String title, final String description) {
        addEntry("Extended", obj, title, description);
    }
    
    private void addEntry(final String group, final Object obj, final String title, final String description) {
        addEntry(group, obj, title, description, null);
    }

    public static class Entry {

        private final String title;
        private final Object object;
        private final String description;
        private final Supplier<?> maker;

        public Entry(final String title, final Object object, final String description,
                     final Supplier<?> maker) {
            this.title = title;
            this.object = object;
            this.description = description;
            this.maker = maker;
        }

        public String getTitle() {
            return title;
        }

        public String getResourceName() {
            return title.replace(" ", "").toLowerCase();
        }

        public <T> T getObject() {
            return (T) ((null == object) ? maker.get() : object);
        }

        public String getDescription() {
            return description;
        }
    }
}
