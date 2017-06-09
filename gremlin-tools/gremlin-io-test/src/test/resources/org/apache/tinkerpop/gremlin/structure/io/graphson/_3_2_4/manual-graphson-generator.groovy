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


import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics

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
import org.apache.commons.configuration.BaseConfiguration

import java.util.concurrent.TimeUnit

new File("dev-docs/").mkdirs()
new File("test-case-data/io/graphson").mkdirs()

conf = new BaseConfiguration()
conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_DEFAULT_VERTEX_PROPERTY_CARDINALITY, VertexProperty.Cardinality.list.name())
graph = TinkerGraph.open(conf)
TinkerFactory.generateTheCrew(graph)
g = graph.traversal()

toJson = { o, type, mapper, comment = "", suffix = "" ->
    def jsonSample = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(o)

    def fileToWriteTo = new File("test-case-data/io/graphson/" + type.toLowerCase().replace(" ","") + "-" + suffix + ".json")
    if (fileToWriteTo.exists()) fileToWriteTo.delete()
    fileToWriteTo.withWriter{ it.write(jsonSample) }

    return type + "\n" +
            "^".multiply(type.length()) + "\n\n" +
            (comment.isEmpty() ? "" : comment + "\n\n") +
            "[source,json]\n" +
            "----\n" +
            jsonSample + "\n" +
            "----\n" +
            "\n"
}

toJsonV1d0NoTypes = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v1d0")
}

writeSupportedV1Objects = { writer, mapper ->
    writer.write("Graph Structure\n")
    writer.write("~~~~~~~~~~~~~~~\n\n")
    writer.write(toJsonV1d0NoTypes(graph.edges().next(), "Edge", mapper))
    writer.write(toJsonV1d0NoTypes(g.V().out().out().path().next(), "Path", mapper))
    writer.write(toJsonV1d0NoTypes(graph.edges().next().properties().next(), "Property", mapper))
    writer.write(toJsonV1d0NoTypes(new org.apache.tinkerpop.gremlin.structure.util.star.DirectionalStarGraph(org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.of(graph.vertices().next()), Direction.BOTH), "StarGraph", mapper))
    writer.write(toJsonV1d0NoTypes(graph, "TinkerGraph", mapper, "`TinkerGraph` has a custom serializer that is registered as part of the `TinkerIoRegistry`."))
    writer.write(toJsonV1d0NoTypes(g.V(1).out().out().tree().next(), "Tree", mapper))
    writer.write(toJsonV1d0NoTypes(graph.vertices().next(), "Vertex", mapper))
    writer.write(toJsonV1d0NoTypes(graph.vertices().next().properties().next(), "VertexProperty", mapper))

    writer.write("\n")
    writer.write("RequestMessage\n")
    writer.write("~~~~~~~~~~~~~~\n\n")
    def msg = null
    msg = RequestMessage.build("authentication").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("saslMechanism", "PLAIN", "sasl", "AHN0ZXBocGhlbgBwYXNzd29yZA==").create()
    writer.write(toJsonV1d0NoTypes(msg, "Authentication Response", mapper, "The following `RequestMessage` is an example of the response that should be made to a SASL-based authentication challenge."))
    msg = RequestMessage.build("eval").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "g.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    writer.write(toJsonV1d0NoTypes(msg, "Session Eval", mapper, "The following `RequestMessage` is an example of a simple session request for a script evaluation with parameters."))
    msg = RequestMessage.build("eval").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "social.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "aliases", [g: "social"], "session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    writer.write(toJsonV1d0NoTypes(msg, "Session Eval Aliased", mapper, "The following `RequestMessage` is an example of a session request for a script evaluation with an alias that binds the `TraversalSource` of \"g\" to \"social\"."))
    msg = RequestMessage.build("close").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    writer.write(toJsonV1d0NoTypes(msg, "Session Close", mapper, "The following `RequestMessage` is an example of a request to close a session."))
    msg = RequestMessage.build("eval").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "g.V(x)", "bindings", [x: 1], "language", "gremlin-groovy").create()
    writer.write(toJsonV1d0NoTypes(msg, "Sessionless Eval", mapper, "The following `RequestMessage` is an example of a simple sessionless request for a script evaluation with parameters."))
    msg = RequestMessage.build("eval").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "social.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "aliases", [g: "social"]).create()
    writer.write(toJsonV1d0NoTypes(msg, "Sessionless Eval Aliased", mapper, "The following `RequestMessage` is an example of a sessionless request for a script evaluation with an alias that binds the `TraversalSource` of \"g\" to \"social\"."))

    writer.write("\n")
    writer.write("ResponseMessage\n")
    writer.write("~~~~~~~~~~~~~~~\n\n")
    msg = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
            code(org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.AUTHENTICATE).create()
    writer.write(toJsonV1d0NoTypes(msg, "Authentication Challenge", mapper, "When authentication is enabled, an initial request to the server will result in an authentication challenge. The typical response message will appear as follows, but handling it could be different dependending on the SASL implementation (e.g. multiple challenges maybe requested in some cases, but not in the default provided by Gremlin Server)."))
    msg = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
            code(org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.SUCCESS).
            result(Arrays.asList(graph.vertices().next())).create()
    writer.write(toJsonV1d0NoTypes(msg, "Standard Result", mapper, "The following `ResponseMessage` is a typical example of the typical successful response Gremlin Server will return when returning results from a script."))
}

createStaticTraversalMetrics = {
    // based on g.V().hasLabel("person").out().out().tree().profile().next()
    def traversalMutableMetrics = new ArrayList<>()
    def m7 = new MutableMetrics("7.0.0()", "TinkerGraphStep(vertex,[~label.eq(person)])")
    m7.setDuration(100, TimeUnit.MILLISECONDS)
    m7.setCount("traverserCount", 4)
    m7.setCount("elementCount", 4)
    m7.setAnnotation("percentDur", 25.0d)
    traversalMutableMetrics.add(m7)

    def m2 = new MutableMetrics("2.0.0()", "VertexStep(OUT,vertex)")
    m2.setDuration(100, TimeUnit.MILLISECONDS)
    m2.setCount("traverserCount", 13)
    m2.setCount("elementCount", 13)
    m2.setAnnotation("percentDur", 25.0d)
    traversalMutableMetrics.add(m2)

    def m3 = new MutableMetrics("3.0.0()", "VertexStep(OUT,vertex)")
    m3.setDuration(100, TimeUnit.MILLISECONDS)
    m3.setCount("traverserCount", 7)
    m3.setCount("elementCount", 7)
    m3.setAnnotation("percentDur", 25.0d)
    traversalMutableMetrics.add(m3)

    def m4 = new MutableMetrics("4.0.0()", "TreeStep")
    m4.setDuration(100, TimeUnit.MILLISECONDS)
    m4.setCount("traverserCount", 1)
    m4.setCount("elementCount", 1)
    m4.setAnnotation("percentDur", 25.0d)
    traversalMutableMetrics.add(m4)

    return new DefaultTraversalMetrics(4000, traversalMutableMetrics)
}

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV1d0.instance()).
        addCustomModule(new AbstractGraphSONMessageSerializerV1d0.GremlinServerModule()).
        version(GraphSONVersion.V1_0).create().createMapper()

v1GraphSONFile = new File("dev-docs/out-graphson-1d0.txt")
if (v1GraphSONFile.exists()) v1GraphSONFile.delete()
new File("dev-docs/out-graphson-1d0.txt").withWriter { writeSupportedV1Objects(it, mapper) }

toJsonV2d0PartialTypes = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v2d0-partial")
}

toJsonV2d0NoTypes = { o, type, mapper, comment = "" ->
    toJson(o, type, mapper, comment, "v2d0-no-types")
}

writeSupportedV2Objects = { writer, mapper, toJsonFunction ->
    writer.write("Core\n")
    writer.write("~~~~\n\n")
    writer.write(toJsonFunction(File, "Class", mapper))
    writer.write(toJsonFunction(new Date(1481750076295L), "Date", mapper))
    writer.write(toJsonFunction(100.00d, "Double", mapper))
    writer.write(toJsonFunction(100.00f, "Float", mapper))
    writer.write(toJsonFunction(100, "Integer", mapper))
    writer.write(toJsonFunction(100L, "Long", mapper))
    writer.write(toJsonFunction(new java.sql.Timestamp(1481750076295L), "Timestamp", mapper))
    writer.write(toJsonFunction(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786"), "UUID", mapper))

    writer.write("\n")
    writer.write("Graph Structure\n")
    writer.write("~~~~~~~~~~~~~~~\n\n")
    writer.write(toJsonFunction(graph.edges().next(), "Edge", mapper))
    writer.write(toJsonFunction(g.V().out().out().path().next(), "Path", mapper))
    writer.write(toJsonFunction(graph.edges().next().properties().next(), "Property", mapper))
    writer.write(toJsonFunction(new org.apache.tinkerpop.gremlin.structure.util.star.DirectionalStarGraph(org.apache.tinkerpop.gremlin.structure.util.star.StarGraph.of(graph.vertices().next()), Direction.BOTH), "StarGraph", mapper))
    writer.write(toJsonFunction(graph, "TinkerGraph", mapper, "`TinkerGraph` has a custom serializer that is registered as part of the `TinkerIoRegistry`."))
    writer.write(toJsonFunction(g.V(1).out().out().tree().next(), "Tree", mapper))
    writer.write(toJsonFunction(graph.vertices().next(), "Vertex", mapper))
    writer.write(toJsonFunction(graph.vertices().next().properties().next(), "VertexProperty", mapper))

    writer.write("\n")
    writer.write("Graph Process\n")
    writer.write("~~~~~~~~~~~~~\n\n")
    writer.write(toJsonFunction(SackFunctions.Barrier.normSack, "Barrier", mapper))
    writer.write(toJsonFunction(new Bytecode.Binding("x", 1), "Binding", mapper, "A \"Binding\" refers to a `Bytecode.Binding`."))
    writer.write(toJsonFunction(g.V().hasLabel('person').out().in().tree().asAdmin().getBytecode(), "Bytecode", mapper, "The following `Bytecode` example represents the traversal of `g.V().hasLabel('person').out().in().tree()`. Obviously the serialized `Bytecode` woudl be quite different for the endless variations of commands that could be used together in the Gremlin language."))
    writer.write(toJsonFunction(VertexProperty.Cardinality.list, "Cardinality", mapper))
    writer.write(toJsonFunction(Column.keys, "Column", mapper))
    writer.write(toJsonFunction(Direction.OUT, "Direction", mapper))
    writer.write(toJsonFunction(Operator.sum, "Operator", mapper))
    writer.write(toJsonFunction(Order.incr, "Order", mapper))
    writer.write(toJsonFunction(Pop.all, "Pop", mapper))
    writer.write(toJsonFunction(org.apache.tinkerpop.gremlin.util.function.Lambda.function("{ it.get() }"), "Lambda", mapper))
    def tm = createStaticTraversalMetrics()
    def metrics = new MutableMetrics(tm.getMetrics("7.0.0()"))
    metrics.addNested(new MutableMetrics(tm.getMetrics("3.0.0()")))
    writer.write(toJsonFunction(metrics, "Metrics", mapper))
    writer.write(toJsonFunction(P.gt(0), "P", mapper))
    writer.write(toJsonFunction(P.gt(0).and(P.lt(10)), "P and", mapper))
    writer.write(toJsonFunction(P.gt(0).or(P.within(-1, -10, -100)), "P or", mapper))
    writer.write(toJsonFunction(Scope.local, "Scope", mapper))
    writer.write(toJsonFunction(T.label, "T", mapper))
    writer.write(toJsonFunction(createStaticTraversalMetrics(), "TraversalMetrics", mapper))
    writer.write(toJsonFunction(g.V().hasLabel('person').nextTraverser(), "Traverser", mapper))

    writer.write("\n")
    writer.write("RequestMessage\n")
    writer.write("~~~~~~~~~~~~~~\n\n")
    def msg = null
    msg = RequestMessage.build("authentication").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("saslMechanism", "PLAIN", "sasl", "AHN0ZXBocGhlbgBwYXNzd29yZA==").create()
    writer.write(toJsonFunction(msg, "Authentication Response", mapper, "The following `RequestMessage` is an example of the response that should be made to a SASL-based authentication challenge."))
    msg = RequestMessage.build("eval").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "g.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    writer.write(toJsonFunction(msg, "Session Eval", mapper, "The following `RequestMessage` is an example of a simple session request for a script evaluation with parameters."))
    msg = RequestMessage.build("eval").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "social.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "aliases", [g: "social"], "session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    writer.write(toJsonFunction(msg, "Session Eval Aliased", mapper, "The following `RequestMessage` is an example of a session request for a script evaluation with an alias that binds the `TraversalSource` of \"g\" to \"social\"."))
    msg = RequestMessage.build("close").processor("session").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("session", UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).create()
    writer.write(toJsonFunction(msg, "Session Close", mapper, "The following `RequestMessage` is an example of a request to close a session."))
    msg = RequestMessage.build("eval").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "g.V(x)", "bindings", [x: 1], "language", "gremlin-groovy").create()
    writer.write(toJsonFunction(msg, "Sessionless Eval", mapper, "The following `RequestMessage` is an example of a simple sessionless request for a script evaluation with parameters."))
    msg = RequestMessage.build("eval").
            overrideRequestId(UUID.fromString("cb682578-9d92-4499-9ebc-5c6aa73c5397")).
            add("gremlin", "social.V(x)", "bindings", [x: 1], "language", "gremlin-groovy", "aliases", [g: "social"]).create()
    writer.write(toJsonFunction(msg, "Sessionless Eval Aliased", mapper, "The following `RequestMessage` is an example of a sessionless request for a script evaluation with an alias that binds the `TraversalSource` of \"g\" to \"social\"."))

    writer.write("\n")
    writer.write("ResponseMessage\n")
    writer.write("~~~~~~~~~~~~~~~\n\n")
    msg = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
            code(org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.AUTHENTICATE).create()
    writer.write(toJsonFunction(msg, "Authentication Challenge", mapper, "When authentication is enabled, an initial request to the server will result in an authentication challenge. The typical response message will appear as follows, but handling it could be different dependending on the SASL implementation (e.g. multiple challenges maybe requested in some cases, but not in the default provided by Gremlin Server)."))
    msg = ResponseMessage.build(UUID.fromString("41d2e28a-20a4-4ab0-b379-d810dede3786")).
            code(org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode.SUCCESS).
            result(Arrays.asList(graph.vertices().next())).create()
    writer.write(toJsonFunction(msg, "Standard Result", mapper, "The following `ResponseMessage` is a typical example of the typical successful response Gremlin Server will return when returning results from a script."))

    writer.write("\n")
    writer.write("Extended\n")
    writer.write("~~~~~~~~\n\n")
    writer.write("""Note that the "extended" types require the addition of the separate `GraphSONXModuleV2d0` module as follows:\n
[source,java]
----
mapper = GraphSONMapper.build().
                        typeInfo(TypeInfo.PARTIAL_TYPES).
                        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
                        version(GraphSONVersion.V2_0).create().createMapper()
----\n
""")
    writer.write(toJsonFunction(new java.math.BigDecimal(new java.math.BigInteger("123456789987654321123456789987654321")), "BigDecimal", mapper))
    writer.write(toJsonFunction(new java.math.BigInteger("123456789987654321123456789987654321"), "BigInteger", mapper))
    writer.write(toJsonFunction(new Byte("1"), "Byte", mapper))
    writer.write(toJsonFunction(java.nio.ByteBuffer.wrap("some bytes for you".getBytes()), "ByteBuffer", mapper))
    writer.write(toJsonFunction("x".charAt(0), "Char", mapper))
    writer.write(toJsonFunction(Duration.ofDays(5), "Duration", mapper,"The following example is a `Duration` of five days."))
    writer.write(toJsonFunction(java.net.InetAddress.getByName("localhost"), "InetAddress", mapper))
    writer.write(toJsonFunction(Instant.parse("2016-12-14T16:39:19.349Z"), "Instant", mapper))
    writer.write(toJsonFunction(LocalDate.of(2016, 1, 1), "LocalDate", mapper))
    writer.write(toJsonFunction(LocalDateTime.of(2016, 1, 1, 12, 30), "LocalDateTime", mapper))
    writer.write(toJsonFunction(LocalTime.of(12, 30, 45), "LocalTime", mapper))
    writer.write(toJsonFunction(MonthDay.of(1, 1), "MonthDay", mapper))
    writer.write(toJsonFunction(OffsetDateTime.parse("2007-12-03T10:15:30+01:00"), "OffsetDateTime", mapper))
    writer.write(toJsonFunction(OffsetTime.parse("10:15:30+01:00"), "OffsetTime", mapper))
    writer.write(toJsonFunction(Period.of(1, 6, 15), "Period", mapper, "The following example is a `Period` of one year, six months and fifteen days."))
    writer.write(toJsonFunction(new Short("100"), "Short", mapper))
    writer.write(toJsonFunction(Year.of(2016), "Year", mapper, "The following example is of the `Year` \"2016\"."))
    writer.write(toJsonFunction(YearMonth.of(2016, 6), "YearMonth", mapper, "The following example is a `YearMonth` of \"June 2016\""))
    writer.write(toJsonFunction(ZonedDateTime.of(2016, 12, 23, 12, 12, 24, 36, ZoneId.of("GMT+2")), "ZonedDateTime", mapper))
    writer.write(toJsonFunction(ZoneOffset.ofHoursMinutesSeconds(3, 6, 9), "ZoneOffset", mapper, "The following example is a `ZoneOffset` of three hours, six minutes, and nine seconds."))
}

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV2d0.instance()).
        typeInfo(TypeInfo.PARTIAL_TYPES).
        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
        addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.GremlinServerModule()).
        version(GraphSONVersion.V2_0).create().createMapper()

file = new File("dev-docs/out-graphson-2d0-partial.txt")
file.withWriter { writeSupportedV2Objects(it, mapper, toJsonV2d0PartialTypes) }

mapper = GraphSONMapper.build().
        addRegistry(TinkerIoRegistryV2d0.instance()).
        typeInfo(TypeInfo.NO_TYPES).
        addCustomModule(GraphSONXModuleV2d0.build().create(false)).
        addCustomModule(new org.apache.tinkerpop.gremlin.driver.ser.AbstractGraphSONMessageSerializerV2d0.GremlinServerModule()).
        version(GraphSONVersion.V2_0).create().createMapper()

file = new File("dev-docs/out-graphson-2d0-no-type.txt")
file.withWriter { writeSupportedV2Objects(it, mapper, toJsonV2d0NoTypes) }