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

import org.apache.tinkerpop.shaded.kryo.io.Output

import java.time.*
import java.nio.file.*
import org.apache.tinkerpop.gremlin.driver.ser.*
import org.apache.tinkerpop.gremlin.process.traversal.*
import org.apache.tinkerpop.gremlin.tinkergraph.structure.*
import org.apache.tinkerpop.gremlin.structure.*
import org.apache.tinkerpop.gremlin.structure.io.graphson.*
import org.apache.tinkerpop.gremlin.driver.message.*
import org.apache.tinkerpop.gremlin.process.traversal.step.*
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalOptionParent.Pick
import org.apache.tinkerpop.gremlin.structure.io.gryo.*

new File("dev-docs/").mkdirs()
new File("test-case-data/io/gryo").mkdirs()

graph = TinkerFactory.createTheCrew()
g = graph.traversal()

toGryo = { o, type, mapper, suffix = "" ->
    def fileToWriteTo = new File("test-case-data/io/gryo/" + type.toLowerCase().replace(" ","") + "-" + suffix + ".kryo")
    if (fileToWriteTo.exists()) fileToWriteTo.delete()
    final Output out = new Output(new FileOutputStream(fileToWriteTo))
    mapper.writeObject(out, o)
    out.close()
}

toGryoV1d0 = { o, type, mapper, comment = "" ->
    toGryo(o, type, mapper, "v1d0")
}

writeSupportedObjects = { mapper, toGryoFunction ->
    //toGryoFunction(File, "Class", mapper)
    toGryoFunction(new Date(1481750076295L), "Date", mapper)
    toGryoFunction(100.00d, "Double", mapper)
    toGryoFunction(100.00f, "Float", mapper)
    toGryoFunction(100, "Integer", mapper)
    toGryoFunction(100L, "Long", mapper)
    //toGryoFunction(new java.sql.Timestamp(1481750076295L), "Timestamp", mapper)
    toGryoFunction(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786"), "UUID", mapper)

    toGryoFunction(graph.edges().next(), "Edge", mapper)
    toGryoFunction(g.V().out().out().path().next(), "Path", mapper)
    toGryoFunction(graph.edges().next().properties().next(), "Property", mapper)
    toGryoFunction(org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.of(graph.vertices().next()), "StarGraph", mapper)
    toGryoFunction(graph, "TinkerGraph", mapper)
    toGryoFunction(g.V().out().out().tree().next(), "Tree", mapper)
    toGryoFunction(graph.vertices().next(), "Vertex", mapper)
    toGryoFunction(graph.vertices().next().properties().next(), "VertexProperty", mapper)

    toGryoFunction(SackFunctions.Barrier.normSack, "Barrier", mapper)
    toGryoFunction(new Bytecode.Binding("x", 1), "Binding", mapper)
    toGryoFunction(g.V().hasLabel('person').out().in().tree().asAdmin().getBytecode(), "Bytecode", mapper)
    toGryoFunction(VertexProperty.Cardinality.list, "Cardinality", mapper)
    toGryoFunction(Column.keys, "Column", mapper)
    toGryoFunction(Direction.OUT, "Direction", mapper)
    toGryoFunction(Operator.sum, "Operator", mapper)
    toGryoFunction(Order.incr, "Order", mapper)
    toGryoFunction(Pop.all, "Pop", mapper)
    toGryoFunction(org.apache.tinkerpop.gremlin.util.function.Lambda.function("{ it.get() }"), "Lambda", mapper)
    tm = g.V().hasLabel('person').out().out().tree().profile().next()
    metrics = new org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics(tm.getMetrics(0))
    metrics.addNested(new org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics(tm.getMetrics(1)))
    toGryoFunction(metrics, "Metrics", mapper)
    toGryoFunction(P.gt(0), "P", mapper)
    toGryoFunction(P.gt(0).and(P.lt(10)), "P and", mapper)
    toGryoFunction(P.gt(0).or(P.within(-1, -10, -100)), "P or", mapper)
    toGryoFunction(Scope.local, "Scope", mapper)
    toGryoFunction(T.label, "T", mapper)
    toGryoFunction(g.V().hasLabel('person').out().out().tree().profile().next(), "TraversalMetrics", mapper)
    toGryoFunction(g.V().hasLabel('person').nextTraverser(), "Traverser", mapper)

    /* not directly supported yet - there is a custom serializer in the way
    def msg = null
    msg = RequestMessage.build("authentication").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("saslMechanism", "PLAIN", "sasl", "AHN0ZXBocGhlbgBwYXNzd29yZA==").create()
    toGryoFunction(msg, "Authentication Response", mapper)
    msg = RequestMessage.build("eval").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "g.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    toGryoFunction(msg, "Session Eval", mapper)
    msg = RequestMessage.build("eval").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "social.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "aliases", [g: "social"], "session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    toGryoFunction(msg, "Session Eval", mapper)
    msg = RequestMessage.build("close").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    toGryoFunction(msg, "Session Close", mapper)
    msg = RequestMessage.build("eval").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "g.V(x)", "bindings", [x: 1], "language", "gremlin-groovy").create()
    toGryoFunction(msg, "Sessionless Eval", mapper)
    msg = RequestMessage.build("eval").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "social.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "aliases", [g: "social"]).create()
    toGryoFunction(msg, "Sessionless Eval", mapper)

    msg = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
            code(org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.AUTHENTICATE).create()
    toGryoFunction(msg, "Authentication Challenge", mapper)
    msg = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
            code(org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.SUCCESS).
            result(Arrays.asList(graph.vertices().next())).create()
    toGryoFunction(msg, "Standard Result", mapper)
    */

    toGryoFunction(new java.math.BigDecimal(new java.math.BigInteger("123456789987654321123456789987654321")), "BigDecimal", mapper)
    toGryoFunction(new java.math.BigInteger("123456789987654321123456789987654321"), "BigInteger", mapper)
    toGryoFunction(new Byte("1"), "Byte", mapper)
    //toGryoFunction(java.nio.ByteBuffer.wrap([1,2,3,4,5] as byte[]), "ByteBuffer", mapper)
    toGryoFunction("x".charAt(0), "Char", mapper)
    toGryoFunction(Duration.ofDays(5), "Duration", mapper)
    //toGryoFunction(java.net.InetAddress.getByName("localhost"), "InetAddress", mapper)
    toGryoFunction(Instant.parse("2016-12-14T16:39:19.349Z"), "Instant", mapper)
    toGryoFunction(LocalDate.of(2016, 1, 1), "LocalDate", mapper)
    toGryoFunction(LocalDateTime.of(2016, 1, 1, 12, 30), "LocalDateTime", mapper)
    toGryoFunction(LocalTime.of(12, 30, 45), "LocalTime", mapper)
    toGryoFunction(MonthDay.of(1, 1), "MonthDay", mapper)
    toGryoFunction(OffsetDateTime.parse("2007-12-03T10:15:30+01:00"), "OffsetDateTime", mapper)
    toGryoFunction(OffsetTime.parse("10:15:30+01:00"), "OffsetTime", mapper)
    toGryoFunction(Period.of(1, 6, 15), "Period", mapper)
    toGryoFunction(new Short("100"), "Short", mapper)
    toGryoFunction(Year.of(2016), "Year", mapper)
    toGryoFunction(YearMonth.of(2016, 6), "YearMonth", mapper)
    toGryoFunction(ZonedDateTime.of(2016, 12, 23, 12, 12, 24, 36, ZoneId.of("GMT+2")), "ZonedDateTime", mapper)
    toGryoFunction(ZoneOffset.ofHoursMinutesSeconds(3, 6, 9), "ZoneOffset", mapper)
}

mapper = GryoMapper.build().addRegistry(TinkerIoRegistryV2d0.getInstance()).create().createMapper()

writeSupportedObjects(mapper, toGryoV1d0)

