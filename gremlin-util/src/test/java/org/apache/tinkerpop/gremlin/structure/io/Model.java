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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Pick;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.SackFunctions;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

        addCoreEntry(File.class, "Class", "");
        addCoreEntry(new Date(1481750076295L), "Date");
        addCoreEntry(100.00d, "Double");
        addCoreEntry(100.00f, "Float", "");
        addCoreEntry(100, "Integer");
        addCoreEntry(Arrays.asList(1,"person", true), "List", "List is used to distinguish between different collection types as JSON is not explicit enough for all of Gremlin's requirements.");
        addCoreEntry(100L, "Long", "");

        final Map<Object,Object> map = new HashMap<>();
        map.put("test", 123);
        map.put(new Date(1481750076295L), "red");
        map.put(Arrays.asList(1,2,3), new Date(1481750076295L));
        addCoreEntry(map, "Map", "Map is redefined so that to provide the ability to allow for non-String keys, which is not possible in JSON.");

        addCoreEntry(new HashSet<>(Arrays.asList(1,"person", true)), "Set", "Allows a JSON collection to behave as a Set.");
        addCoreEntry(new java.sql.Timestamp(1481750076295L), "Timestamp", "");
        addCoreEntry(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786"), "UUID");

        addGraphStructureEntry(IteratorUtils.list(graph.edges()).stream()
                .sorted((e1, e2) -> Integer.compare((Integer)e1.id(), (Integer)e2.id()))
                .iterator().next(), "Edge", "");
        addGraphStructureEntry(g.V().order().by(T.id).out().out().path().next(), "Path", "");
        addGraphStructureEntry(IteratorUtils.list(IteratorUtils.list(graph.edges()).stream()
                .sorted((e1, e2) -> Integer.compare((Integer)e1.id(), (Integer)e2.id()))
                .iterator().next().properties()).stream()
                .sorted((p1, p2) -> p1.key().compareTo(p2.key()))
                .iterator().next(), "Property", "");
        addGraphStructureEntry(graph, "TinkerGraph", "`TinkerGraph` has a custom serializer that is registered as part of the `TinkerIoRegistry`.");
        addGraphStructureEntry(IteratorUtils.list(graph.vertices()).stream()
                .sorted((v1, v2) -> Integer.compare((Integer)v1.id(), (Integer)v2.id()))
                .iterator().next(), "Vertex", "");
        addGraphStructureEntry(IteratorUtils.list(IteratorUtils.list(graph.vertices()).stream()
                .sorted((v1, v2) -> Integer.compare((Integer)v1.id(), (Integer)v2.id()))
                .iterator().next().properties()).stream()
                .sorted((p1, p2) -> Long.compare((Long)p1.id(), (Long)p2.id()))
                .iterator().next(), "VertexProperty", "");

        addGraphProcessEntry(SackFunctions.Barrier.normSack, "Barrier", "");
        addGraphProcessEntry(new Bytecode.Binding("x", 1), "Binding", "A \"Binding\" refers to a `Bytecode.Binding`.");

        final BulkSet<String> bulkSet = new BulkSet<>();
        bulkSet.add("marko", 1);
        bulkSet.add("josh", 2);
        addGraphProcessEntry(bulkSet, "BulkSet", "");

        addGraphProcessEntry(g.V().hasLabel("person").out().in().tree().asAdmin().getBytecode(), "Bytecode", "The following `Bytecode` example represents the traversal of `g.V().hasLabel('person').out().in().tree()`. Obviously the serialized `Bytecode` woudl be quite different for the endless variations of commands that could be used together in the Gremlin language.");
        addGraphProcessEntry(VertexProperty.Cardinality.list, "Cardinality", "");
        addGraphProcessEntry(Column.keys, "Column", "");
        addGraphProcessEntry(Direction.OUT, "Direction", "");
        addGraphProcessEntry(Operator.sum, "Operator", "");
        addGraphProcessEntry(Order.shuffle, "Order", "");
        addGraphProcessEntry(Pick.any, "Pick", "");
        addGraphProcessEntry(Pop.all, "Pop", "");
        addGraphProcessEntry(org.apache.tinkerpop.gremlin.util.function.Lambda.function("{ it.get() }"), "Lambda", "");
        final TraversalMetrics tm = createStaticTraversalMetrics();
        final MutableMetrics metrics = new MutableMetrics(tm.getMetrics("7.0.0()"));
        metrics.addNested(new MutableMetrics(tm.getMetrics("3.0.0()")));
        addGraphProcessEntry(metrics, "Metrics", "");
        addGraphProcessEntry(P.gt(0), "P", "");
        addGraphProcessEntry(P.within(1), "P within", "");
        addGraphProcessEntry(P.without(1,2), "P without", "");
        addGraphProcessEntry(P.gt(0).and(P.lt(10)), "P and", "");
        addGraphProcessEntry(P.gt(0).or(P.within(-1, -10, -100)), "P or", "");
        addGraphProcessEntry(Scope.local, "Scope", "");
        addGraphProcessEntry(T.label, "T", "");
        // TextP was only added at 3.4.0 and is not supported with untyped GraphSON of any sort
        addGraphProcessEntry(TextP.containing("ark"), "TextP", "");
        addGraphProcessEntry(createStaticTraversalMetrics(), "TraversalMetrics", "");
        addGraphProcessEntry(g.V().hasLabel("person").order().by(T.id).asAdmin().nextTraverser(), "Traverser", "");

        final Map<String,Object> requestBindings = new HashMap<>();
        requestBindings.put("x", 1);

        final Map<String,Object> requestAliases = new HashMap<>();
        requestAliases.put("g", "social");

        RequestMessage requestMessage;
        requestMessage = RequestMessage.build("authentication").
                overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
                add("saslMechanism", "PLAIN", "sasl", "AHN0ZXBocGhlbgBwYXNzd29yZA==").create();
        addRequestMessageEntry(requestMessage, "Authentication Response", "The following `RequestMessage` is an example of the response that should be made to a SASL-based authentication challenge.");
        requestMessage = RequestMessage.build("eval").processor("session").
                overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
                add("gremlin", "g.V(x)", "bindings", requestBindings, "language", "gremlin-groovy", "session", "unique-session-identifier").create();
        addRequestMessageEntry(requestMessage, "Session Eval", "The following `RequestMessage` is an example of a simple session request for a script evaluation with parameters.");
        requestMessage = RequestMessage.build("eval").processor("session").
                overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
                add("gremlin", "social.V(x)", "bindings", requestBindings, "language", "gremlin-groovy", "aliases", requestAliases, "session","unique-session-identifier").create();
        addRequestMessageEntry(requestMessage, "Session Eval Aliased", "The following `RequestMessage` is an example of a session request for a script evaluation with an alias that binds the `TraversalSource` of \"g\" to \"social\".");
        requestMessage = RequestMessage.build("close").processor("session").
                overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
                add("session", "unique-session-identifier").create();
        addRequestMessageEntry(requestMessage, "Session Close", "The following `RequestMessage` is an example of a request to close a session.");
        requestMessage = RequestMessage.build("eval").
                overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
                add("gremlin", "g.V(x)", "bindings", requestBindings, "language", "gremlin-groovy").create();
        addRequestMessageEntry(requestMessage, "Sessionless Eval", "The following `RequestMessage` is an example of a simple sessionless request for a script evaluation with parameters.");
        requestMessage = RequestMessage.build("eval").
                overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
                add("gremlin", "social.V(x)", "bindings", requestBindings, "language", "gremlin-groovy", "aliases", requestAliases).create();
        addRequestMessageEntry(requestMessage, "Sessionless Eval Aliased", "The following `RequestMessage` is an example of a sessionless request for a script evaluation with an alias that binds the `TraversalSource` of \"g\" to \"social\".");

        ResponseMessage responseMessage = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
                code(org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode.AUTHENTICATE).create();
        addResponseMessageEntry(responseMessage, "Authentication Challenge", "When authentication is enabled, an initial request to the server will result in an authentication challenge. The typical response message will appear as follows, but handling it could be different depending on the SASL implementation (e.g. multiple challenges maybe requested in some cases, but not in the default provided by Gremlin Server).");
        responseMessage = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
                code(org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode.SUCCESS).
                result(Collections.singletonList(IteratorUtils.list(graph.vertices()).stream()
                        .sorted((v1, v2) -> Integer.compare((Integer)v1.id(), (Integer)v2.id()))
                        .iterator().next())).create();
        addResponseMessageEntry(responseMessage, "Standard Result", "The following `ResponseMessage` is a typical example of the typical successful response Gremlin Server will return when returning results from a script.");
        
        addExtendedEntry(new BigDecimal(new BigInteger("123456789987654321123456789987654321")), "BigDecimal", "");
        addExtendedEntry(new BigInteger("123456789987654321123456789987654321"), "BigInteger", "");
        addExtendedEntry(new Byte("1"), "Byte", "");
        addEntry("Extended", () -> java.nio.ByteBuffer.wrap("some bytes for you".getBytes()), "ByteBuffer", "");
        addExtendedEntry("x".charAt(0), "Char", "");
        addExtendedEntry(Duration.ofDays(5), "Duration","The following example is a `Duration` of five days.");
        try {
            addEntry("Extended", InetAddress.getByName("localhost"), "InetAddress", "");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        addExtendedEntry(Instant.parse("2016-12-14T16:39:19.349Z"), "Instant", "");
        addExtendedEntry(LocalDate.of(2016, 1, 1), "LocalDate", "");
        addExtendedEntry(LocalDateTime.of(2016, 1, 1, 12, 30), "LocalDateTime", "");
        addExtendedEntry(LocalTime.of(12, 30, 45), "LocalTime", "");
        addExtendedEntry(MonthDay.of(1, 1), "MonthDay", "");
        addExtendedEntry(OffsetDateTime.parse("2007-12-03T10:15:30+01:00"), "OffsetDateTime", "");
        addExtendedEntry(OffsetTime.parse("10:15:30+01:00"), "OffsetTime", "");
        addExtendedEntry(Period.of(1, 6, 15), "Period", "The following example is a `Period` of one year, six months and fifteen days.");
        addExtendedEntry(new Short("100"), "Short", "");
        addExtendedEntry(Year.of(2016), "Year", "The following example is of the `Year` \"2016\".");
        addExtendedEntry(YearMonth.of(2016, 6), "YearMonth", "The following example is a `YearMonth` of \"June 2016\"");
        addExtendedEntry(ZonedDateTime.of(2016, 12, 23, 12, 12, 24, 36, ZoneId.of("GMT+2")), "ZonedDateTime", "");
        addExtendedEntry(ZoneOffset.ofHoursMinutesSeconds(3, 6, 9), "ZoneOffset", "The following example is a `ZoneOffset` of three hours, six minutes, and nine seconds.");
    }

    private static DefaultTraversalMetrics createStaticTraversalMetrics() {
        // based on g.V().hasLabel("person").out().out().tree().profile().next()
        final List<MutableMetrics> traversalMutableMetrics = new ArrayList<>();
        final MutableMetrics m7 = new MutableMetrics("7.0.0()", "TinkerGraphStep(vertex,[~label.eq(person)])");
        m7.setDuration(100, TimeUnit.MILLISECONDS);
        m7.setCount("traverserCount", 4);
        m7.setCount("elementCount", 4);
        m7.setAnnotation("percentDur", 25.0d);
        traversalMutableMetrics.add(m7);

        final MutableMetrics m2 = new MutableMetrics("2.0.0()", "VertexStep(OUT,vertex)");
        m2.setDuration(100, TimeUnit.MILLISECONDS);
        m2.setCount("traverserCount", 13);
        m2.setCount("elementCount", 13);
        m2.setAnnotation("percentDur", 25.0d);
        traversalMutableMetrics.add(m2);

        final MutableMetrics m3 = new MutableMetrics("3.0.0()", "VertexStep(OUT,vertex)");
        m3.setDuration(100, TimeUnit.MILLISECONDS);
        m3.setCount("traverserCount", 7);
        m3.setCount("elementCount", 7);
        m3.setAnnotation("percentDur", 25.0d);
        traversalMutableMetrics.add(m3);

        final MutableMetrics m4 = new MutableMetrics("4.0.0()", "TreeStep");
        m4.setDuration(100, TimeUnit.MILLISECONDS);
        m4.setCount("traverserCount", 1);
        m4.setCount("elementCount", 1);
        m4.setAnnotation("percentDur", 25.0d);
        traversalMutableMetrics.add(m4);

        return new DefaultTraversalMetrics(4000, traversalMutableMetrics);
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
