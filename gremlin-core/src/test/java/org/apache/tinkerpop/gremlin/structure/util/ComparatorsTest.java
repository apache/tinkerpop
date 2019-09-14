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
package org.apache.tinkerpop.gremlin.structure.util;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ComparatorsTest {
    @Test
    public void shouldBeUtilityClass() throws Exception {
        TestHelper.assertIsUtilityClass(Comparators.class);
    }

    @Test
    public void shouldSortElement() {
        final Element a = mock(Element.class);
        when(a.id()).thenReturn("Ant");
        final Element b = mock(Element.class);
        when(b.id()).thenReturn("Bat");
        final Element c = mock(Element.class);
        when(c.id()).thenReturn("Cat");

        final List<Element> l = Arrays.asList(c, b, a);
        l.sort(Comparators.ELEMENT_COMPARATOR);

        assertEquals(a.id(), l.get(0).id());
        assertEquals(b.id(), l.get(1).id());
        assertEquals(c.id(), l.get(2).id());
    }

    @Test
    public void shouldSortVertex() {
        final Vertex a = mock(Vertex.class);
        when(a.id()).thenReturn("Ant");
        final Vertex b = mock(Vertex.class);
        when(b.id()).thenReturn("Bat");
        final Vertex c = mock(Vertex.class);
        when(c.id()).thenReturn("Cat");

        final List<Vertex> l = Arrays.asList(c, b, a);
        l.sort(Comparators.VERTEX_COMPARATOR);

        assertEquals(a.id(), l.get(0).id());
        assertEquals(b.id(), l.get(1).id());
        assertEquals(c.id(), l.get(2).id());
    }

    @Test
    public void shouldSortEdge() {
        final Edge a = mock(Edge.class);
        when(a.id()).thenReturn("Ant");
        final Edge b = mock(Edge.class);
        when(b.id()).thenReturn("Bat");
        final Edge c = mock(Edge.class);
        when(c.id()).thenReturn("Cat");

        final List<Edge> l = Arrays.asList(c, b, a);
        l.sort(Comparators.EDGE_COMPARATOR);

        assertEquals(a.id(), l.get(0).id());
        assertEquals(b.id(), l.get(1).id());
        assertEquals(c.id(), l.get(2).id());
    }

    @Test
    public void shouldSortProperty() {
        final Property a = mock(Property.class);
        when(a.key()).thenReturn("Ant");
        final Property b = mock(Property.class);
        when(b.key()).thenReturn("Bat");
        final Property c = mock(Property.class);
        when(c.key()).thenReturn("Cat");

        final List<Property> l = Arrays.asList(c, b, a);
        l.sort(Comparators.PROPERTY_COMPARATOR);

        assertEquals(a.key(), l.get(0).key());
        assertEquals(b.key(), l.get(1).key());
        assertEquals(c.key(), l.get(2).key());
    }
}
