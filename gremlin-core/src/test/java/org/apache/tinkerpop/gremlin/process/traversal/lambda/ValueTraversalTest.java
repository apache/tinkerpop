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
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ValueTraversalTest {

    @Test
    public void shouldWorkOnVertex() {
        final ValueTraversal<Vertex, Integer> t = new ValueTraversal<>("age");
        final Vertex v = mock(Vertex.class);
        when(v.property("age")).thenReturn(new DetachedVertexProperty<>(1, "age", 29, null));
        t.addStart(new B_O_Traverser<>(v, 1).asAdmin());
        assertEquals(29, t.next().intValue());
    }

    @Test
    public void shouldWorkOnVertexWithMissingKey() {
        final ValueTraversal<Vertex, Integer> t = new ValueTraversal<>("age");
        final Vertex v = mock(Vertex.class);
        when(v.property("age")).thenReturn(VertexProperty.empty());
        t.addStart(new B_O_Traverser<>(v, 1).asAdmin());
        assertNull(t.next());
    }

    @Test
    public void shouldWorkOnEdge() {
        final ValueTraversal<Edge, Double> t = new ValueTraversal<>("weight");
        final Edge e = mock(Edge.class);
        when(e.property("weight")).thenReturn(new DetachedProperty<>("weight", 1.0d));
        t.addStart(new B_O_Traverser<>(e, 1).asAdmin());
        assertEquals(1.0d, t.next(), 0.00001d);
    }

    @Test
    public void shouldWorkOnEdgeWithMissingKey() {
        final ValueTraversal<Edge, Double> t = new ValueTraversal<>("weight");
        final Edge e = mock(Edge.class);
        when(e.property("weight")).thenReturn(Property.empty());
        t.addStart(new B_O_Traverser<>(e, 1).asAdmin());
        assertNull(t.next());
    }

    @Test
    public void shouldWorkOnVertexProperty() {
        final ValueTraversal<VertexProperty, Integer> t = new ValueTraversal<>("age");
        final VertexProperty vp = mock(VertexProperty.class);
        when(vp.property("age")).thenReturn(new DetachedProperty<>("age", 29));
        t.addStart(new B_O_Traverser<>(vp, 1).asAdmin());
        assertEquals(29, t.next().intValue());
    }

    @Test
    public void shouldWorkOnVertexPropertyWithMissingKey() {
        final ValueTraversal<VertexProperty, Integer> t = new ValueTraversal<>("age");
        final VertexProperty vp = mock(VertexProperty.class);
        when(vp.property("age")).thenReturn(Property.empty());
        t.addStart(new B_O_Traverser<>(vp, 1).asAdmin());
        assertNull(t.next());
    }

    @Test
    public void shouldWorkOnMap() {
        final ValueTraversal<Map<String,Integer>, Integer> t = new ValueTraversal<>("age");
        final Map<String,Integer> m = new HashMap<>();
        m.put("age", 29);
        t.addStart(new B_O_Traverser<>(m, 1).asAdmin());
        assertEquals(29, t.next().intValue());
    }

    @Test
    public void shouldWorkOnMapWithMissingKey() {
        final ValueTraversal<Map<String,Integer>, Integer> t = new ValueTraversal<>("not-age");
        final Map<String,Integer> m = new HashMap<>();
        m.put("age", 29);
        t.addStart(new B_O_Traverser<>(m, 1).asAdmin());
        assertNull(t.next());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenTryingUnsupportedType() {
        final ValueTraversal<Integer, Integer> t = new ValueTraversal<>("age");
        t.addStart(new B_O_Traverser<>(29, 1).asAdmin());
        t.next();
    }
}
