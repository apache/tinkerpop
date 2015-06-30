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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResultTest {
    private final Graph g = TinkerFactory.createModern();

    @Test
    public void shouldGetString() {
        final Result result = new Result("string");

        assertEquals("string", result.getString());
        assertEquals("string", result.get(String.class));
    }

    @Test
    public void shouldGetInt() {
        final Result result = new Result(100);

        assertEquals(100, result.getInt());
        assertEquals(100, result.get(Integer.class).intValue());
    }

    @Test
    public void shouldGetByte() {
        final Result result = new Result((byte) 100);

        assertEquals((byte) 100, result.getByte());
        assertEquals((byte) 100, result.get(Byte.class).byteValue());
    }

    @Test
    public void shouldGetShort() {
        final Result result = new Result((short) 100);

        assertEquals((short) 100, result.getShort());
        assertEquals((short) 100, result.get(Short.class).shortValue());
    }

    @Test
    public void shouldGetLong() {
        final Result result = new Result(100l);

        assertEquals((long) 100, result.getLong());
        assertEquals((long) 100, result.get(Long.class).longValue());
    }

    @Test
    public void shouldGetFloat() {
        final Result result = new Result(100.001f);

        assertEquals(100.001f, result.getFloat(), 0.0001f);
        assertEquals(100.001f, result.get(Float.class).floatValue(), 0.0001f);
    }

    @Test
    public void shouldGetDouble() {
        final Result result = new Result(100.001d);

        assertEquals(100.001d, result.getDouble(), 0.0001d);
        assertEquals(100.001d, result.get(Double.class), 0.0001d);
    }

    @Test
    public void shouldGetBoolean() {
        final Result result = new Result(true);

        assertEquals(true, result.getBoolean());
        assertEquals(true, result.get(Boolean.class));
    }

    @Test
    public void shouldGetVertex() {
        final Vertex v = g.vertices(1).next();
        final Result result = new Result(v);

        assertEquals(v, result.getVertex());
        assertEquals(v, result.get(Vertex.class));
        assertEquals(v, result.getElement());
        assertEquals(v, result.get(Element.class));
    }

    @Test
    public void shouldGetVertexProperty() {
        final VertexProperty<String> v = g.vertices(1).next().property("name");
        final Result result = new Result(v);

        assertEquals(v, result.getVertexProperty());
        assertEquals(v, result.get(VertexProperty.class));
        assertEquals(v, result.getElement());
        assertEquals(v, result.get(Element.class));
    }

    @Test
    public void shouldGetEdge() {
        final Edge e = g.edges(11).next();
        final Result result = new Result(e);

        assertEquals(e, result.getEdge());
        assertEquals(e, result.get(Edge.class));
        assertEquals(e, result.getElement());
        assertEquals(e, result.get(Element.class));
    }

    @Test
    public void shouldGetProperty() {
        final Property<Double> p = g.edges(11).next().property("weight");
        final Result result = new Result(p);

        assertEquals(p, result.getProperty());
        assertEquals(p, result.get(Property.class));
    }

    @Test
    public void shouldGetPath() {
        final Path p = g.traversal().V().out().path().next();
        final Result result = new Result(p);

        assertEquals(p, result.getPath());
        assertEquals(p, result.get(Path.class));
    }
}
