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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.io.graphbinary.GraphBinaryCompatibilityTest;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV4;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractUntypedCompatibilityTest extends AbstractCompatibilityTest {

    @Test
    public void shouldReadWriteEdge() throws Exception {
        final String resourceName = "traversal-edge";

        final Edge resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, Edge.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(Collections.singletonList(resource.label()), fromStatic.get("label"));
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(Collections.singletonList(resource.inVertex().label()), ((Map) fromStatic.get("inV")).get("label"));
        assertEquals(Collections.singletonList(resource.outVertex().label()), ((Map) fromStatic.get("outV")).get("label"));
        assertEquals(resource.inVertex().id(), ((Map) fromStatic.get("inV")).get("id"));
        assertEquals(resource.outVertex().id(), ((Map) fromStatic.get("outV")).get("id"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(Collections.singletonList(resource.label()), recycled.get("label"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(Collections.singletonList(resource.inVertex().label()), ((Map) recycled.get("inV")).get("label"));
        assertEquals(Collections.singletonList(resource.outVertex().label()), ((Map) recycled.get("outV")).get("label"));
        assertEquals(resource.inVertex().id(), ((Map) recycled.get("inV")).get("id"));
        assertEquals(resource.outVertex().id(), ((Map) recycled.get("outV")).get("id"));

        // deal with incompatibilities
        if (getCompatibility().equals("v1-no-types") || getCompatibility().equals("v3-no-types")) {
            assertEquals("edge", fromStatic.get("type"));
            assertEquals(IteratorUtils.count(resource.properties()), ((Map) fromStatic.get("properties")).size());
            assertEquals(resource.value("since"), ((Map) fromStatic.get("properties")).get("since"));
            assertEquals("edge", recycled.get("type"));
            assertEquals(IteratorUtils.count(resource.properties()), ((Map) recycled.get("properties")).size());
            assertEquals(resource.value("since"), ((Map) recycled.get("properties")).get("since"));
        } else if (getCompatibility().equals("v2-no-types")) {
            assertEquals(IteratorUtils.count(resource.properties()), ((Map) fromStatic.get("properties")).size());
            assertEquals(resource.keys().iterator().next(), ((Map) ((Map) fromStatic.get("properties")).get("since")).get("key"));
            assertEquals(resource.value("since"), ((Map) ((Map) fromStatic.get("properties")).get("since")).get("value"));
            assertEquals(IteratorUtils.count(resource.properties()), ((Map) recycled.get("properties")).size());
            assertEquals(resource.keys().iterator().next(), ((Map) ((Map) recycled.get("properties")).get("since")).get("key"));
            assertEquals(resource.value("since"), ((Map) ((Map) recycled.get("properties")).get("since")).get("value"));
        }
    }

    @Test
    public void shouldReadWritePath() throws Exception {
        final String resourceName = "traversal-path";

        final Path resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, Path.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.labels().size(), ((List) fromStatic.get("labels")).size());
        assertEquals(resource.labels().get(0).size(), ((List) ((List) fromStatic.get("labels")).get(0)).size());
        assertEquals(resource.labels().get(1).size(), ((List) ((List) fromStatic.get("labels")).get(1)).size());
        assertEquals(resource.labels().get(2).size(), ((List) ((List) fromStatic.get("labels")).get(2)).size());
        assertEquals(resource.objects().size(), ((List) fromStatic.get("objects")).size());
        assertEquals(((Vertex) resource.objects().get(0)).id(), ((Map) ((List) fromStatic.get("objects")).get(0)).get("id"));
        assertEquals(Collections.singletonList(((Vertex) resource.objects().get(0)).label()), ((Map) ((List) fromStatic.get("objects")).get(0)).get("label"));
        assertEquals(((Vertex) resource.objects().get(1)).id(), ((Map) ((List) fromStatic.get("objects")).get(1)).get("id"));
        assertEquals(Collections.singletonList(((Vertex) resource.objects().get(1)).label()), ((Map) ((List) fromStatic.get("objects")).get(1)).get("label"));
        assertEquals(((Vertex) resource.objects().get(2)).id(), ((Map) ((List) fromStatic.get("objects")).get(2)).get("id"));
        assertEquals(Collections.singletonList(((Vertex) resource.objects().get(2)).label()), ((Map) ((List) fromStatic.get("objects")).get(2)).get("label"));
        assertEquals(resource.labels().size(), ((List) recycled.get("labels")).size());
        assertEquals(resource.labels().get(0).size(), ((List) ((List) recycled.get("labels")).get(0)).size());
        assertEquals(resource.labels().get(1).size(), ((List) ((List) recycled.get("labels")).get(1)).size());
        assertEquals(resource.labels().get(2).size(), ((List) ((List) recycled.get("labels")).get(2)).size());
        assertEquals(resource.objects().size(), ((List) recycled.get("objects")).size());
        assertEquals(((Vertex) resource.objects().get(0)).id(), ((Map) ((List) recycled.get("objects")).get(0)).get("id"));
        assertEquals(Collections.singletonList(((Vertex) resource.objects().get(0)).label()), ((Map) ((List) recycled.get("objects")).get(0)).get("label"));
        assertEquals(((Vertex) resource.objects().get(1)).id(), ((Map) ((List) recycled.get("objects")).get(1)).get("id"));
        assertEquals(Collections.singletonList(((Vertex) resource.objects().get(1)).label()), ((Map) ((List) recycled.get("objects")).get(1)).get("label"));
        assertEquals(((Vertex) resource.objects().get(2)).id(), ((Map) ((List) recycled.get("objects")).get(2)).get("id"));
        assertEquals(Collections.singletonList(((Vertex) resource.objects().get(2)).label()), ((Map) ((List) recycled.get("objects")).get(2)).get("label"));
    }

    @Test
    public void shouldReadWriteProperty() throws Exception {
        final String resourceName = "edge-property";

        final Property resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, Property.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(2, fromStatic.size());
        assertEquals(resource.key(), fromStatic.get("key"));
        assertEquals(resource.value(), fromStatic.get("value"));
        assertEquals(2, recycled.size());
        assertEquals(resource.key(), recycled.get("key"));
        assertEquals(resource.value(), recycled.get("value"));
    }

    @Test
    public void shouldReadWriteVertex() throws Exception {
        final String resourceName = "traversal-vertex";

        final Vertex resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, Vertex.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(Collections.singletonList(resource.label()), fromStatic.get("label"));
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(Collections.singletonList(resource.label()), recycled.get("label"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(IteratorUtils.count(resource.keys()), ((Map) fromStatic.get("properties")).size());
        assertEquals(resource.value("name"), ((Map) ((List) ((Map) fromStatic.get("properties")).get("name")).get(0)).get("value"));
        assertEquals(IteratorUtils.list(resource.values("location")).get(0), ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(0)).get("value"));
        assertEquals(IteratorUtils.list(resource.values("location")).get(1), ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(1)).get("value"));
        assertEquals(IteratorUtils.list(resource.values("location")).get(2), ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(2)).get("value"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(0)).value("startTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(0)).get("properties")).get("startTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(0)).value("endTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(0)).get("properties")).get("endTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(1)).value("startTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(1)).get("properties")).get("startTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(1)).value("endTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(1)).get("properties")).get("endTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(2)).value("startTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(2)).get("properties")).get("startTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(2)).value("endTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(2)).get("properties")).get("endTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(3)).value("startTime"), ((Map) ((Map) ((List) ((Map) fromStatic.get("properties")).get("location")).get(3)).get("properties")).get("startTime"));
        assertEquals(IteratorUtils.count(resource.keys()), ((Map) recycled.get("properties")).size());
        assertEquals(resource.value("name"), ((Map) ((List) ((Map) recycled.get("properties")).get("name")).get(0)).get("value"));
        assertEquals(IteratorUtils.list(resource.values("location")).get(0), ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(0)).get("value"));
        assertEquals(IteratorUtils.list(resource.values("location")).get(1), ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(1)).get("value"));
        assertEquals(IteratorUtils.list(resource.values("location")).get(2), ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(2)).get("value"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(0)).value("startTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(0)).get("properties")).get("startTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(0)).value("endTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(0)).get("properties")).get("endTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(1)).value("startTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(1)).get("properties")).get("startTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(1)).value("endTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(1)).get("properties")).get("endTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(2)).value("startTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(2)).get("properties")).get("startTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(2)).value("endTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(2)).get("properties")).get("endTime"));
        assertEquals(((VertexProperty) IteratorUtils.list(resource.properties("location")).get(3)).value("startTime"), ((Map) ((Map) ((List) ((Map) recycled.get("properties")).get("location")).get(3)).get("properties")).get("startTime"));

        // deal with incompatibilities
        if (getCompatibility().equals("v1") || getCompatibility().equals("v3")) {
            assertEquals("vertex", fromStatic.get("type"));
        }
    }

    @Test
    public void shouldReadWriteVertexProperty() throws Exception {
        final String resourceName = "traversal-vertexproperty";

        final VertexProperty resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, VertexProperty.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(3, fromStatic.size());
        assertEquals(resource.id().toString(), fromStatic.get("id").toString());
        assertEquals(Collections.singletonList(resource.key()), fromStatic.get("label"));
        assertEquals(resource.value(), fromStatic.get("value"));
        assertEquals(3, recycled.size());
        assertEquals(resource.id().toString(), fromStatic.get("id").toString());
        assertEquals(Collections.singletonList(resource.key()), recycled.get("label"));
        assertEquals(resource.value(), recycled.get("value"));
    }
}
