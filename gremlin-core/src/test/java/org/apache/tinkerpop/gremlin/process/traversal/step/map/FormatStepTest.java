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

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.tinkerpop.gremlin.util.CollectionUtil.asMap;
import static org.junit.Assert.assertEquals;

public class FormatStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(
                __.format("hello")
        );
    }

    private List<String> getVariables(final String format) {
        final FormatStep formatStep = new FormatStep(__.inject("test").asAdmin(), format);
        return new ArrayList<>(formatStep.getScopeKeys());
    }

    @Test
    public void shouldGetVariablesFromTemplate() {
        assertEquals(Collections.emptyList(), getVariables(""));
        assertEquals(Collections.emptyList(), getVariables("Hello world"));
        assertEquals(Collections.emptyList(), getVariables("Hello %{world"));
        assertEquals(Collections.emptyList(), getVariables("Hello {world}"));
        assertEquals(Collections.emptyList(), getVariables("Hello % {world}"));
        assertEquals(Collections.emptyList(), getVariables("Hello %% {world}"));
        assertEquals(Collections.emptyList(), getVariables("Hello %%{world}"));
        assertEquals(Collections.emptyList(), getVariables("Hello%%{world}"));
        assertEquals(Collections.emptyList(), getVariables("Hello %{_}"));
        assertEquals(Collections.emptyList(), getVariables("%%{world}"));
        assertEquals(Collections.singletonList(""), getVariables("Hello %{}"));
        assertEquals(Collections.singletonList(" "), getVariables("Hello %{ }"));
        assertEquals(Collections.singletonList("world"), getVariables("Hello %{world}"));
        assertEquals(Collections.singletonList("world"), getVariables("%%{Hello} %{world} %{_}"));
        assertEquals(Arrays.asList("Hello", "world"), getVariables("%{Hello} %{world}"));
        assertEquals(Arrays.asList("Hello", "hello", "world"), getVariables("%{Hello}%{hello}%{world}"));
        assertEquals(Arrays.asList("Hello", "world"), getVariables("%{Hello} %{Hello} %{world}"));
        assertEquals(Arrays.asList("Hello", "hello", "world"), getVariables("%{Hello} %{hello} %{world}"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionWhenFormatStringIsNull() {
        new FormatStep(__.inject("test").asAdmin(), null);
    }

    @Test
    public void shouldWorkWithoutVariables() {
        assertEquals("Hello world", __.__("test").format("Hello world").next());
    }

    @Test
    public void shouldWorkWithVertexInput() {
        final Vertex vertex1 = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        assertEquals("Hello Stephen", __.__(vertex1).format("Hello %{name}").next());
    }

    @Test
    public void shouldWorkWithEmptyTemplate() {
        final Vertex vertex1 = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        assertEquals("", __.__(vertex1).format("").next());
    }

    @Test
    public void shouldWorkWithMultipleVertexInput() {
        final Vertex vertex1 = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        final Vertex vertex2 = new DetachedVertex(11L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(2).setLabel("name").setValue("Marko").create()));

        assertEquals(Arrays.asList("Hello Stephen", "Hello Marko"),
                __.__(vertex1, vertex2).format("Hello %{name}").toList());
    }

    @Test
    public void shouldWorkWithModulator() {
        final Vertex vertex1 = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        final Vertex vertex2 = new DetachedVertex(11L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(2).setLabel("name").setValue("Marko").create()));

        assertEquals(Arrays.asList("Hello Stephen", "Hello Marko"),
                __.__(vertex1, vertex2).format("%{_} %{_}").by(__.constant("Hello")).by(__.values("name")).toList());
    }

    @Test
    public void shouldNotTryToApplyModulatorTraversalToAllVars() {
        final Vertex vertex1 = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        assertEquals(Collections.emptyList(),
                __.__(vertex1).format("%{name} %{missing}").by(__.label()).toList());
    }

    @Test
    public void shouldHandleMissingModulatorValue() {
        final Vertex vertex1 = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        final Vertex vertex2 = new DetachedVertex(11L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(2).setLabel("name").setValue("Marko").create()));

        assertEquals(Arrays.asList("Hello Stephen Hello", "Hello Marko Hello"),
                __.__(vertex1, vertex2).format("%{_} %{_} %{_}").
                        by(__.constant("Hello")).by(__.values("name")).toList());
    }

    @Test
    public void shouldWorkWithMap() {
        assertEquals("Hello 2", __.__(asMap("name", 2)).format("Hello %{name}").next());
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
        final Vertex vertex = new DetachedVertex(10L, "test", Arrays.asList(
                DetachedVertexProperty.build().setId(1).setLabel("p1").setValue("val1").create(),
                DetachedVertexProperty.build().setId(2).setLabel("p2").setValue("val2").create()));

        assertEquals("val1 val2 valA valB",
                __.inject("valA").as("varA").
                        constant("valB").as("varB").
                        constant(vertex).format("%{p1} %{p2} %{varA} %{varB}").next());
    }

    @Test
    public void shouldPrioritizeVertexPropertiesOverScopeVariables() {
        final Vertex vertex = new DetachedVertex(10L, "person", Collections.singletonList(
                DetachedVertexProperty.build().setId(1).setLabel("name").setValue("Stephen").create()));

        assertEquals("Hello Stephen",
                __.__("Marko").as("name").
                        constant(vertex).format("Hello %{name}").next());
    }

    @Test
    public void testScopingInfo() {
        final FormatStep formatStep = new FormatStep(__.identity().asAdmin(), "%{Hello} %{world}");

        final Scoping.ScopingInfo scopingInfo1 = new Scoping.ScopingInfo();
        scopingInfo1.label = "Hello";
        scopingInfo1.pop = Pop.last;

        final Scoping.ScopingInfo scopingInfo2 = new Scoping.ScopingInfo();
        scopingInfo2.label = "world";
        scopingInfo2.pop = Pop.last;

        final HashSet<Scoping.ScopingInfo> scopingInfoSet = new HashSet<>();
        scopingInfoSet.add(scopingInfo1);
        scopingInfoSet.add(scopingInfo2);

        assertEquals(formatStep.getScopingInfo(), scopingInfoSet);

    }
}