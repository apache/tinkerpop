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
package org.apache.tinkerpop.gremlin.process.traversal.lambda;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TokenTraversalTest {
    @Test
    public void shouldWorkOnVertex() {
        final TokenTraversal<Vertex, Integer> t = new TokenTraversal<>(T.id);
        final Vertex v = mock(Vertex.class);
        when(v.id()).thenReturn(100);
        t.addStart(new B_O_Traverser<>(v, 1).asAdmin());
        assertEquals(100, t.next().intValue());
    }

    @Test
    public void shouldWorkOnVertexProperty() {
        final TokenTraversal<VertexProperty, Integer> t = new TokenTraversal<>(T.id);
        final VertexProperty vo = mock(VertexProperty.class);
        when(vo.id()).thenReturn(100);
        t.addStart(new B_O_Traverser<>(vo, 1).asAdmin());
        assertEquals(100, t.next().intValue());
    }

    @Test
    public void shouldWorkOnEdge() {
        final TokenTraversal<Edge, Integer> t = new TokenTraversal<>(T.id);
        final Edge e = mock(Edge.class);
        when(e.id()).thenReturn(100);
        t.addStart(new B_O_Traverser<>(e, 1).asAdmin());
        assertEquals(100, t.next().intValue());
    }

    @Test
    public void shouldWorkOnPropertyKey() {
        final TokenTraversal<Property<String>, String> t = new TokenTraversal<>(T.key);
        final Property<String> p = mock(Property.class);
        when(p.key()).thenReturn("name");
        t.addStart(new B_O_Traverser<>(p, 1).asAdmin());
        assertEquals("name", t.next());
    }

    @Test
    public void shouldWorkOnPropertyValue() {
        final TokenTraversal<Property<String>, String> t = new TokenTraversal<>(T.value);
        final Property<String> p = mock(Property.class);
        when(p.value()).thenReturn("marko");
        t.addStart(new B_O_Traverser<>(p, 1).asAdmin());
        assertEquals("marko", t.next());
    }
}
