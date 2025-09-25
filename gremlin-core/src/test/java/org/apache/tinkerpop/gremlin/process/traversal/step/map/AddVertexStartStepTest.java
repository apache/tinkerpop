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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class AddVertexStartStepTest extends GValueStepTest {

    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                g.addV("knows").property("a", "b"),
                g.addV("created").property("a", "b"),
                g.addV("knows").property("a", "b").property("c", "e"),
                g.addV("knows").property("c", "e"),
                g.addV(GValue.of("label", "knows")).property("a", "b"),
                g.addV(GValue.of("label", "created")).property("a", GValue.of("prop", "b")),
                g.addV(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(g.addV(GValue.of("label", "knows")).property("a", "b"), Set.of("label")),
                Pair.of(g.addV(GValue.of("label", "created")).property("a", GValue.of("prop", "b")), Set.of("label", "prop")),
                Pair.of(g.addV(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")), Set.of("label", "prop1", "prop2"))
        );
    }

    @Test
    public void shouldRemoveElementIdFromAddVertexStartStep() {
        final AddVertexStartStep step = new AddVertexStartStep(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId("startVertex123");
        assertEquals("startVertex123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveExistingPropertyFromAddVertexStartStep() {
        final AddVertexStartStep step = new AddVertexStartStep(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.addProperty("name", "josh");
        step.addProperty("age", 32);

        assertTrue(step.removeProperty("age"));
        assertFalse(step.getProperties().containsKey("age"));
        assertTrue(step.getProperties().containsKey("name"));
        assertFalse(step.removeProperty("age"));
    }

    @Test
    public void getLabelShouldPinVariable() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("person", ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getLabel());
        verifyVariables(traversal, Set.of("label"), Set.of("id", "a"));
    }

    @Test
    public void getLabelAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(GValue.of("label", "person"), ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getLabelWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }
    
    @Test
    public void getLabelFromConcreteStep() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("person", ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getLabel());
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }

    @Test
    public void getElementIdShouldPinVariable() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("1234", ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getElementId());
        verifyVariables(traversal, Set.of("id"), Set.of("label", "a"));
    }

    @Test
    public void getElementIdAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(GValue.of("id", "1234"), ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getElementIdWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }

    @Test
    public void getElementIdFromConcreteStep() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("1234", ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getElementId());
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(List.of(29), ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getProperties().get("age"));
        verifyVariables(traversal, Set.of("a"), Set.of("label", "id"));
    }

    @Test
    public void getPropertiesWithGValuesShouldNotPinVariable() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(List.of(GValue.of("a", 29)), ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0))
                .getPropertiesWithGValues().get("age"));
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }

    @Test
    public void getPropertiesFromConcreteStep() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(List.of(29), ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getProperties().get("age"));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = getAddPersonGValueTraversal();
        Collection<GValue<?>> gValues = ((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getGValues();
        assertEquals(3, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("label", "id", "a")));
    }

    @Test
    public void getGValuesNonShouldReturnEmptyCollection() {
        GraphTraversal.Admin<Vertex, Vertex> traversal = g.addV("person")
                .property(T.id, "1234")
                .property("age", 29)
                .asAdmin();
        assertTrue(((AddVertexStartStepPlaceholder) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    private GraphTraversal.Admin<Vertex, Vertex> getAddPersonGValueTraversal() {
        return g.addV(GValue.of("label", "person"))
                .property(T.id, GValue.of("id", "1234"))
                .property("age", GValue.of("a", 29))
                .asAdmin();
    }
}
