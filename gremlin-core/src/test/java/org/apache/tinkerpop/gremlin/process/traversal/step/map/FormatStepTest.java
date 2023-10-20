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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FormatStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(
                __.format("hello")
        );
    }

    private List<String> getVariables(final String format) {
        final FormatStep formatStep =  new FormatStep(__.inject("test").asAdmin(), format);
        return new ArrayList<>(formatStep.getScopeKeys());
    }

    @Test
    public void shouldGetVariablesFromTemplate() {
        assertEquals(Collections.emptyList(), getVariables("Hello world"));
        assertEquals(Collections.emptyList(), getVariables("Hello %{world"));
        assertEquals(Collections.emptyList(), getVariables("Hello {world}"));
        assertEquals(Collections.emptyList(), getVariables("Hello % {world}"));
        assertEquals(Collections.singletonList("world"), getVariables("Hello %{world}"));
        assertEquals(Arrays.asList("Hello", "world"), getVariables("%{Hello} %{world}"));
        assertEquals(Arrays.asList("Hello", "world"), getVariables("%{Hello} %{Hello} %{world}"));
        assertEquals(Arrays.asList("Hello", "hello", "world"), getVariables("%{Hello} %{hello} %{world}"));
    }

    @Test
    public void shouldWorkWithoutVariables() {
        assertEquals("Hello world", __.__("test").format("Hello world").next());
    }

    @Test
    public void shouldWorkWithVertexInput() {
        final VertexProperty mockProperty = mock(VertexProperty.class);
        when(mockProperty.key()).thenReturn("name");
        when(mockProperty.value()).thenReturn("Stephen");
        when(mockProperty.isPresent()).thenReturn(true);

        final Vertex mockVertex = mock(Vertex.class);
        when(mockVertex.property("name")).thenReturn(mockProperty);

        assertEquals("Hello Stephen", __.__(mockVertex).format("Hello %{name}").next());
    }

    @Test
    public void shouldWorkWithMultipleVertexInput() {
        final VertexProperty mockProperty1 = mock(VertexProperty.class);
        when(mockProperty1.key()).thenReturn("name");
        when(mockProperty1.value()).thenReturn("Stephen");
        when(mockProperty1.isPresent()).thenReturn(true);

        final Vertex mockVertex1 = mock(Vertex.class);
        when(mockVertex1.property("name")).thenReturn(mockProperty1);

        final VertexProperty mockProperty2 = mock(VertexProperty.class);
        when(mockProperty2.key()).thenReturn("name");
        when(mockProperty2.value()).thenReturn("Marko");
        when(mockProperty2.isPresent()).thenReturn(true);

        final Vertex mockVertex2 = mock(Vertex.class);
        when(mockVertex2.property("name")).thenReturn(mockProperty2);

        assertEquals(Arrays.asList("Hello Stephen", "Hello Marko"),
                __.__(mockVertex1, mockVertex2).format("Hello %{name}").toList());
    }
    @Test
    public void shouldWorkWithMap() {
        assertEquals("Hello Stephen", __.__(asMap("name", "Stephen")).format("Hello %{name}").next());
    }

    @Test
    public void shouldWorkWithScopeVariables() {
         assertEquals("Hello Stephen", __.__("Stephen").as("name").format("Hello %{name}").next());
    }

    @Test
    public void shouldHandleSameVariableTwice() {
        assertEquals("Hello, Hello Stephen",
                __.__("Hello").as("action").format("%{action}, %{action} Stephen").next());
    }

    @Test
    public void shouldWorkWithMixedInput() {
        final VertexProperty mockProperty1 = mock(VertexProperty.class);
        when(mockProperty1.key()).thenReturn("p1");
        when(mockProperty1.value()).thenReturn("val1");
        when(mockProperty1.isPresent()).thenReturn(true);

        final VertexProperty mockProperty2 = mock(VertexProperty.class);
        when(mockProperty2.key()).thenReturn("p2");
        when(mockProperty2.value()).thenReturn("val2");
        when(mockProperty2.isPresent()).thenReturn(true);

        final Vertex mockVertex = mock(Vertex.class);
        when(mockVertex.property("p1")).thenReturn(mockProperty1);
        when(mockVertex.property("p2")).thenReturn(mockProperty2);

        assertEquals("val1 val2 valA valB",
                __.inject("valA").as("varA").
                        constant("valB").as("varB").
                        constant(mockVertex).format("%{p1} %{p2} %{varA} %{varB}").next());
    }
}