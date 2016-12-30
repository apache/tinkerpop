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

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
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

    public abstract <T> T read(final byte[] bytes, final Class<T> clazz) throws Exception;

    public abstract byte[] write(final Object o, final Class<?> clazz) throws Exception;

    @Test
    public void shouldReadWriteAuthenticationChallenge() throws Exception {
        final String resourceName = "authenticationchallenge";
        assumeCompatibility(resourceName);

        final ResponseMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(resource, ResponseMessage.class), HashMap.class);
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
        assumeCompatibility(resourceName);

        final RequestMessage resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(resource, RequestMessage.class), HashMap.class);
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
        assumeCompatibility(resourceName);

        final Edge resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(resource, Edge.class), HashMap.class);
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
        if (getCompatibility().getConfiguration().equals("v1d0")) {
            assertEquals("edge", fromStatic.get("type"));
            assertEquals(IteratorUtils.count(resource.properties()), ((Map) fromStatic.get("properties")).size());
            assertEquals(resource.value("since"), ((Map) fromStatic.get("properties")).get("since"));
            assertEquals("edge", recycled.get("type"));
            assertEquals(IteratorUtils.count(resource.properties()), ((Map) recycled.get("properties")).size());
            assertEquals(resource.value("since"), ((Map) recycled.get("properties")).get("since"));
        } else if (getCompatibility().getConfiguration().contains("no-types")) {
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
        assumeCompatibility(resourceName);

        final Path resource = findModelEntryObject(resourceName);
        final HashMap fromStatic = read(getCompatibility().readFromResource(resourceName), HashMap.class);
        final HashMap recycled = read(write(resource, Path.class), HashMap.class);
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
}
