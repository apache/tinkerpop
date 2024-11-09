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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractUntypedCompatibilityTest extends AbstractCompatibilityTest {

    @Test
    public void shouldReadWriteAuthenticationChallenge() throws Exception {
        final String resourceName = "authenticationchallenge";

        final ResponseMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, ResponseMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(ResponseStatusCode.AUTHENTICATE.getValue(), ((Map) fromStatic.get("status")).get("code"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(ResponseStatusCode.AUTHENTICATE.getValue(), ((Map) recycled.get("status")).get("code"));
    }

    @Test
    public void shouldReadWriteAuthenticationResponse() throws Exception {
        final String resourceName = "authenticationresponse";

        final RequestMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, RequestMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().get("saslMechanism"), ((Map) fromStatic.get("args")).get("saslMechanism"));
        assertEquals(resource.getArgs().get("sasl"), ((Map) fromStatic.get("args")).get("sasl"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().get("saslMechanism"), ((Map) recycled.get("args")).get("saslMechanism"));
        assertEquals(resource.getArgs().get("sasl"), ((Map) recycled.get("args")).get("sasl"));
    }

    @Test
    public void shouldReadWriteEdge() throws Exception {
        final String resourceName = "edge";

        final Edge resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, Edge.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(resource.label(), fromStatic.get("label"));
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(resource.inVertex().label(), fromStatic.get("inVLabel"));
        assertEquals(resource.outVertex().label(), fromStatic.get("outVLabel"));
        assertEquals(resource.inVertex().id(), fromStatic.get("inV"));
        assertEquals(resource.outVertex().id(), fromStatic.get("outV"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(resource.label(), recycled.get("label"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(resource.inVertex().label(), recycled.get("inVLabel"));
        assertEquals(resource.outVertex().label(), recycled.get("outVLabel"));
        assertEquals(resource.inVertex().id(), recycled.get("inV"));
        assertEquals(resource.outVertex().id(), recycled.get("outV"));

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
        final String resourceName = "path";

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
        assertEquals(((Vertex) resource.objects().get(0)).label(), ((Map) ((List) fromStatic.get("objects")).get(0)).get("label"));
        assertEquals(((Vertex) resource.objects().get(1)).id(), ((Map) ((List) fromStatic.get("objects")).get(1)).get("id"));
        assertEquals(((Vertex) resource.objects().get(1)).label(), ((Map) ((List) fromStatic.get("objects")).get(1)).get("label"));
        assertEquals(((Vertex) resource.objects().get(2)).id(), ((Map) ((List) fromStatic.get("objects")).get(2)).get("id"));
        assertEquals(((Vertex) resource.objects().get(2)).label(), ((Map) ((List) fromStatic.get("objects")).get(2)).get("label"));
        assertEquals(resource.labels().size(), ((List) recycled.get("labels")).size());
        assertEquals(resource.labels().get(0).size(), ((List) ((List) recycled.get("labels")).get(0)).size());
        assertEquals(resource.labels().get(1).size(), ((List) ((List) recycled.get("labels")).get(1)).size());
        assertEquals(resource.labels().get(2).size(), ((List) ((List) recycled.get("labels")).get(2)).size());
        assertEquals(resource.objects().size(), ((List) recycled.get("objects")).size());
        assertEquals(((Vertex) resource.objects().get(0)).id(), ((Map) ((List) recycled.get("objects")).get(0)).get("id"));
        assertEquals(((Vertex) resource.objects().get(0)).label(), ((Map) ((List) recycled.get("objects")).get(0)).get("label"));
        assertEquals(((Vertex) resource.objects().get(1)).id(), ((Map) ((List) recycled.get("objects")).get(1)).get("id"));
        assertEquals(((Vertex) resource.objects().get(1)).label(), ((Map) ((List) recycled.get("objects")).get(1)).get("label"));
        assertEquals(((Vertex) resource.objects().get(2)).id(), ((Map) ((List) recycled.get("objects")).get(2)).get("id"));
        assertEquals(((Vertex) resource.objects().get(2)).label(), ((Map) ((List) recycled.get("objects")).get(2)).get("label"));
    }

    @Test
    public void shouldReadWriteProperty() throws Exception {
        final String resourceName = "property";

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
    public void shouldReadWriteSessionClose() throws Exception {
        final String resourceName = "sessionclose";

        final RequestMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, RequestMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) fromStatic.get("args")).size());
        assertEquals(resource.getArgs().get("session").toString(), ((Map) fromStatic.get("args")).get("session"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) recycled.get("args")).size());
        assertEquals(resource.getArgs().get("session").toString(), ((Map) recycled.get("args")).get("session"));
    }

    @Test
    public void shouldReadWriteSessionEval() throws Exception {
        final String resourceName = "sessioneval";

        final RequestMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, RequestMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) fromStatic.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("session").toString(), ((Map) fromStatic.get("args")).get("session"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) fromStatic.get("args")).get("bindings"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) recycled.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("session").toString(), ((Map) recycled.get("args")).get("session"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) recycled.get("args")).get("bindings"));
    }

    @Test
    public void shouldReadWriteSessionEvalAliased() throws Exception {
        final String resourceName = "sessionevalaliased";

        final RequestMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, RequestMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) fromStatic.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("session").toString(), ((Map) fromStatic.get("args")).get("session"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) fromStatic.get("args")).get("bindings"));
        assertEquals(resource.getArgs().get("aliased"), ((Map) fromStatic.get("args")).get("aliased"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) recycled.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("session").toString(), ((Map) recycled.get("args")).get("session"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) recycled.get("args")).get("bindings"));
        assertEquals(resource.getArgs().get("aliased"), ((Map) recycled.get("args")).get("aliased"));
    }

    @Test
    public void shouldReadWriteSessionlessEval() throws Exception {
        final String resourceName = "sessionlesseval";

        final RequestMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, RequestMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) fromStatic.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) fromStatic.get("args")).get("bindings"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) recycled.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) recycled.get("args")).get("bindings"));
    }

    @Test
    public void shouldReadWriteSessionlessEvalAliased() throws Exception {
        final String resourceName = "sessionlessevalaliased";

        final RequestMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, RequestMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getOp(), fromStatic.get("op"));
        assertEquals(resource.getProcessor(), fromStatic.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) fromStatic.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) fromStatic.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) fromStatic.get("args")).get("language"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) fromStatic.get("args")).get("bindings"));
        assertEquals(resource.getArgs().get("aliased"), ((Map) fromStatic.get("args")).get("aliased"));
        assertEquals(resource.getRequestId().toString(), recycled.get("requestId"));
        assertEquals(resource.getOp(), recycled.get("op"));
        assertEquals(resource.getProcessor(), recycled.get("processor"));
        assertEquals(resource.getArgs().size(), ((Map) recycled.get("args")).size());
        assertEquals(resource.getArgs().get("gremlin"), ((Map) recycled.get("args")).get("gremlin"));
        assertEquals(resource.getArgs().get("language"), ((Map) recycled.get("args")).get("language"));
        assertEquals(resource.getArgs().get("bindings"), ((Map) recycled.get("args")).get("bindings"));
        assertEquals(resource.getArgs().get("aliased"), ((Map) recycled.get("args")).get("aliased"));
    }

    @Test
    public void shouldReadWriteStandardResult() throws Exception {
        final String resourceName = "standardresult";

        final ResponseMessage resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, ResponseMessage.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(fromStatic, recycled);
        assertEquals(resource.getRequestId().toString(), fromStatic.get("requestId"));
        assertEquals(resource.getStatus().getCode().getValue(), ((Map) fromStatic.get("status")).get("code"));
        assertEquals(resource.getStatus().getMessage(), ((Map) fromStatic.get("status")).get("message"));
        assertEquals(resource.getStatus().getAttributes(), ((Map) fromStatic.get("status")).get("attributes"));
        assertEquals(resource.getResult().getMeta(), ((Map) fromStatic.get("result")).get("meta"));
        assertEquals(((List) resource.getResult().getData()).size(), ((List) ((Map) fromStatic.get("result")).get("data")).size());
    }

    @Test
    public void shouldReadWriteVertex() throws Exception {
        final String resourceName = "vertex";

        final Vertex resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, Vertex.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(resource.label(), fromStatic.get("label"));
        assertEquals(resource.id(), fromStatic.get("id"));
        assertEquals(resource.id(), recycled.get("id"));
        assertEquals(resource.label(), recycled.get("label"));
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
        final String resourceName = "vertexproperty";

        final VertexProperty resource = findModelEntryObject(resourceName);
        final byte[] bytes = write(resource, VertexProperty.class, resourceName);
        final HashMap fromStatic = read(readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(bytes, HashMap.class);
        assertNotSame(fromStatic, recycled);
        assertEquals(3, fromStatic.size());
        assertEquals(resource.id().toString(), fromStatic.get("id").toString());
        assertEquals(resource.key(), fromStatic.get("label"));
        assertEquals(resource.value(), fromStatic.get("value"));
        assertEquals(3, recycled.size());
        assertEquals(resource.id().toString(), fromStatic.get("id").toString());
        assertEquals(resource.key(), recycled.get("label"));
        assertEquals(resource.value(), recycled.get("value"));
    }
}
