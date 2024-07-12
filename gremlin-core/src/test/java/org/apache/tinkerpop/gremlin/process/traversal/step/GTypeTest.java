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
package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class GTypeTest {

    @Test
    public void shouldReturnTrueForNumericTypes() {
        assertThat(GType.INTEGER.isNumeric(), is(true));
        assertThat(GType.DOUBLE.isNumeric(), is(true));
        assertThat(GType.LONG.isNumeric(), is(true));
        assertThat(GType.BIG_INTEGER.isNumeric(), is(true));
        assertThat(GType.BIG_DECIMAL.isNumeric(), is(true));
    }

    @Test
    public void shouldReturnFalseForNonNumericTypes() {
        assertThat(GType.STRING.isNumeric(), is(false));
        assertThat(GType.BOOLEAN.isNumeric(), is(false));
        assertThat(GType.EDGE.isNumeric(), is(false));
        assertThat(GType.VERTEX.isNumeric(), is(false));
    }

    @Test
    public void shouldReturnCorrectGType() {
        assertEquals(GType.STRING, GType.getType("test"));
        assertEquals(GType.INTEGER, GType.getType(123));
        assertEquals(GType.BOOLEAN, GType.getType(true));
        assertEquals(GType.DOUBLE, GType.getType(123.45));
        assertEquals(GType.LONG, GType.getType(123L));
        assertEquals(GType.MAP, GType.getType(Collections.emptyMap()));
        assertEquals(GType.LIST, GType.getType(Collections.emptyList()));
        assertEquals(GType.SET, GType.getType(Collections.emptySet()));
        assertEquals(GType.VERTEX, GType.getType(mock(Vertex.class)));
        assertEquals(GType.EDGE, GType.getType(mock(Edge.class)));
        assertEquals(GType.PATH, GType.getType(mock(Path.class)));
        assertEquals(GType.PROPERTY, GType.getType(mock(Property.class)));
        assertEquals(GType.BIG_INTEGER, GType.getType(BigInteger.ONE));
        assertEquals(GType.BIG_DECIMAL, GType.getType(BigDecimal.ONE));
    }

    @Test
    public void shouldReturnUnknownForUnmatchedTypes() {
        assertEquals(GType.UNKNOWN, GType.getType(new Object()));
    }
}