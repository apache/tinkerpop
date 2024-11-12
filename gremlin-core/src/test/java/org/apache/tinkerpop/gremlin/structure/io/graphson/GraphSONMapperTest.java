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
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedEdge;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class GraphSONMapperTest {

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"v1", GraphSONMapper.build().version(GraphSONVersion.V1_0).typeInfo(TypeInfo.NO_TYPES).create().createMapper()},
                {"v2", GraphSONMapper.build().version(GraphSONVersion.V2_0).addCustomModule(GraphSONXModuleV2.build()).typeInfo(TypeInfo.NO_TYPES).create().createMapper()},
                {"v2-default", GraphSONMapper.build().version(GraphSONVersion.V2_0).addDefaultXModule(true).typeInfo(TypeInfo.NO_TYPES).create().createMapper()}, // alternate construction of v2
                {"v3", GraphSONMapper.build().version(GraphSONVersion.V3_0).addCustomModule(GraphSONXModuleV3.build()).typeInfo(TypeInfo.NO_TYPES).create().createMapper()},
                {"v4", GraphSONMapper.build().version(GraphSONVersion.V4_0).addCustomModule(GraphSONXModuleV4.build()).typeInfo(TypeInfo.NO_TYPES).create().createMapper()},
        });
    }

    @Parameterized.Parameter(0)
    public String version;

    @Parameterized.Parameter(1)
    public ObjectMapper mapper;

    @Test
    public void shouldHandleVertex() throws Exception {
        final Vertex v = new DetachedVertex(123L, "person", Arrays.asList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("alice").create(),
                DetachedVertexProperty.build().setId(1).setLabel("age").setValue("31").create()));
        final String json = mapper.writeValueAsString(v);

        // v2 untyped seems to serialize the VertexProperty label. not changing that since it's already been
        // introduced a long while back. v3 dips back to v1 style - since it has never existed prior to 3.7.0
        // we can make this change
        if (version.startsWith("v1") || version.startsWith("v3"))
            assertEquals("{\"id\":123,\"label\":\"person\",\"type\":\"vertex\",\"properties\":{\"name\":[{\"id\":1,\"value\":\"alice\"}],\"age\":[{\"id\":1,\"value\":\"31\"}]}}", json);
        else if (version.startsWith("v2"))
            assertEquals("{\"id\":123,\"label\":\"person\",\"properties\":{\"name\":[{\"id\":1,\"value\":\"alice\",\"label\":\"name\"}],\"age\":[{\"id\":1,\"value\":\"31\",\"label\":\"age\"}]}}", json);
        else if (version.startsWith("v4"))
            assertEquals("{\"id\":123,\"label\":[\"person\"],\"type\":\"vertex\",\"properties\":{\"name\":[{\"id\":1,\"value\":\"alice\"}],\"age\":[{\"id\":1,\"value\":\"31\"}]}}", json);
        else
            throw new IllegalStateException("Version not accounted for in asserts");
    }

    @Test
    public void shouldHandleEdge() throws Exception {
        final Edge e = new DetachedEdge(123L, "knows", new HashMap<String,Object>() {{
            put("weight", 0.5d);
        }}, 1L, "person", 2L, "person");
        final String json = mapper.writeValueAsString(e);

        // v2 untyped seems to serialize the VertexProperty label. not changing that since it's already been
        // introduced a long while back. v3 dips back to v1 style - since it has never existed prior to 3.7.0
        // we can make this change
        if (version.startsWith("v1") || version.startsWith("v3"))
            assertEquals("{\"id\":123,\"label\":\"knows\",\"type\":\"edge\",\"inVLabel\":\"person\",\"outVLabel\":\"person\",\"inV\":2,\"outV\":1,\"properties\":{\"weight\":0.5}}", json);
        else if (version.startsWith("v2"))
            assertEquals("{\"id\":123,\"label\":\"knows\",\"inVLabel\":\"person\",\"outVLabel\":\"person\",\"inV\":2,\"outV\":1,\"properties\":{\"weight\":{\"key\":\"weight\",\"value\":0.5}}}", json);
        else if (version.startsWith("v4"))
            assertEquals("{\"id\":123,\"label\":[\"knows\"],\"type\":\"edge\",\"inV\":{\"id\":2,\"label\":[\"person\"]},\"outV\":{\"id\":1,\"label\":[\"person\"]},\"properties\":{\"weight\":[0.5]}}", json);
        else
            throw new IllegalStateException("Version not accounted for in asserts");
    }

    @Test
    public void shouldHandleProperty() throws Exception {
        final Property p = new DetachedProperty("k", 123);
        final String json = mapper.writeValueAsString(p);
        assertEquals("{\"key\":\"k\",\"value\":123}", json);
    }

    @Test
    public void shouldHandleVertexProperty() throws Exception {
        final DetachedVertex v = new DetachedVertex(321L, "person", Collections.emptyMap());
        final VertexProperty p = new DetachedVertexProperty(123L, "name", "alice",
                new HashMap<String,Object>() {{
                    put("current", true);
                }}, v);
        final String json = mapper.writeValueAsString(p);
        if (version.startsWith("v4")) {
            assertEquals("{\"id\":123,\"value\":\"alice\",\"label\":[\"name\"],\"properties\":{\"current\":true}}", json);
        } else {
            assertEquals("{\"id\":123,\"value\":\"alice\",\"label\":\"name\",\"properties\":{\"current\":true}}", json);
        }
    }

    @Test
    public void shouldHandleVertexPropertyNoMeta() throws Exception {
        final DetachedVertex v = new DetachedVertex(321L, "person", Collections.emptyMap());
        final VertexProperty p = new DetachedVertexProperty(123L, "name", "alice", Collections.emptyMap(), v);
        final String json = mapper.writeValueAsString(p);
        if (version.startsWith("v4")) {
            assertEquals("{\"id\":123,\"value\":\"alice\",\"label\":[\"name\"]}", json);
        } else {
            assertEquals("{\"id\":123,\"value\":\"alice\",\"label\":\"name\"}", json);
        }
    }

    @Test
    public void shouldHandlePath() throws Exception {
        final Vertex v = new DetachedVertex(123L, "person", Arrays.asList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("alice").create(),
                DetachedVertexProperty.build().setId(1).setLabel("age").setValue("31").create()));
        final Path p = ImmutablePath.make().extend(v, new HashSet<>(Collections.singletonList("a"))).
                extend(123L, new HashSet<>(Collections.singletonList("b"))).
                extend("alice", new HashSet<>(Collections.singletonList("c")));
        final String json = mapper.writeValueAsString(p);

        if (version.startsWith("v1") || version.startsWith("v3"))
            assertEquals("{\"labels\":[[\"a\"],[\"b\"],[\"c\"]],\"objects\":[{\"id\":123,\"label\":\"person\",\"type\":\"vertex\",\"properties\":{\"name\":[{\"id\":1,\"value\":\"alice\"}],\"age\":[{\"id\":1,\"value\":\"31\"}]}},123,\"alice\"]}", json);
        else if (version.startsWith("v4"))
            assertEquals("{\"labels\":[[\"a\"],[\"b\"],[\"c\"]],\"objects\":[{\"id\":123,\"label\":[\"person\"],\"type\":\"vertex\",\"properties\":{\"name\":[{\"id\":1,\"value\":\"alice\"}],\"age\":[{\"id\":1,\"value\":\"31\"}]}},123,\"alice\"]}", json);
        else
            assertEquals("{\"labels\":[[\"a\"],[\"b\"],[\"c\"]],\"objects\":[{\"id\":123,\"label\":\"person\",\"properties\":{\"name\":[{\"id\":1,\"value\":\"alice\",\"label\":\"name\"}],\"age\":[{\"id\":1,\"value\":\"31\",\"label\":\"age\"}]}},123,\"alice\"]}", json);
    }

    @Test
    public void shouldHandleTraversalExplanation() throws Exception {
        assumeThat(version, not(startsWith("v4")));

        final TraversalExplanation te = __().out().outV().outE().explain();
        final String json = mapper.writeValueAsString(te);
        assertEquals("{\"original\":[\"InjectStep([])\",\"VertexStep(OUT,vertex)\",\"EdgeVertexStep(OUT)\",\"VertexStep(OUT,edge)\"],\"intermediate\":[],\"final\":[\"InjectStep([])\",\"VertexStep(OUT,vertex)\",\"EdgeVertexStep(OUT)\",\"VertexStep(OUT,edge)\"]}", json);
    }

    @Test
    public void shouldHandleDuration()throws Exception  {
        final Duration o = Duration.ZERO;
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleInstant()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleLocalDate()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final LocalDate o = LocalDate.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleLocalDateTime()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final LocalDateTime o = LocalDateTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleLocalTime()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final LocalTime o = LocalTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleMonthDay()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final MonthDay o = MonthDay.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleOffsetDateTime()throws Exception  {
        final OffsetDateTime o = OffsetDateTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleOffsetTime()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final OffsetTime o = OffsetTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandlePeriod()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final Period o = Period.ofDays(3);
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleYear()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final Year o = Year.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleYearMonth()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final YearMonth o = YearMonth.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleZonedDateTime()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final ZonedDateTime o = ZonedDateTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleZoneOffset()throws Exception  {
        assumeThat(version, not(startsWith("v4")));

        final ZoneOffset o = ZonedDateTime.now().getOffset();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }
}
