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
package org.apache.tinkerpop.gremlin.structure;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TTest {
    @Test
    public void shouldGetLabelEnumFromString() {
        assertEquals(T.label, T.fromString(T.label.getAccessor()));
    }

    @Test
    public void shouldGetIdEnumFromString() {
        assertEquals(T.id, T.fromString(T.id.getAccessor()));
    }

    @Test
    public void shouldGetKeyEnumFromString() {
        assertEquals(T.key, T.fromString(T.key.getAccessor()));
    }

    @Test
    public void shouldGetValueEnumFromString() {
        assertEquals(T.value, T.fromString(T.value.getAccessor()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowIfAccesorIsNotExpected() {
        T.fromString("blah");
    }

    @Test
    public void shouldApplyLabelOnElement() {
        final Element e = mock(Element.class);
        when(e.label()).thenReturn("knows");

        assertEquals("knows", T.label.apply(e));
    }

    @Test
    public void shouldApplyIdOnElement() {
        final Element e = mock(Element.class);
        when(e.id()).thenReturn(100l);

        assertEquals(100l, T.id.apply(e));
    }

    @Test
    public void shouldApplyKeyOnVertexProperty() {
        final VertexProperty e = mock(VertexProperty.class);
        when(e.key()).thenReturn("keyName");

        assertEquals("keyName", T.key.apply(e));
    }

    @Test
    public void shouldApplyValueOnVertexProperty() {
        final VertexProperty e = mock(VertexProperty.class);
        when(e.value()).thenReturn("val");

        assertEquals("val", T.value.apply(e));
    }
}
