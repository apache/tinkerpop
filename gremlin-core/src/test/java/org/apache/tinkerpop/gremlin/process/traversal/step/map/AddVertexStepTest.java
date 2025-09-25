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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AddVertexStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.addV("knows").property("a", "b"),
                __.addV("created").property("a", "b"),
                __.addV("knows").property("a", "b").property("c", "e"),
                __.addV("knows").property("c", "e"),
                __.addV(GValue.of("label", "knows")).property("a", "b"),
                __.addV(GValue.of("label", "created")).property("a", GValue.of("prop", "b")),
                __.addV(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.addV(GValue.of("label", "knows")).property("a", "b"), Set.of("label")),
                Pair.of(__.addV(GValue.of("label", "created")).property("a", GValue.of("prop", "b")), Set.of("label", "prop")),
                Pair.of(__.addV(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")), Set.of("label", "prop1", "prop2"))
        );
    }


    @Test
    public void shouldRemoveElementIdFromAddVertexStep() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId("vertex123");
        assertEquals("vertex123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveElementIdFromAddVertexStepWhenIdIsNull() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        assertNull(step.getElementId());

        assertFalse(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveElementIdFromAddVertexStepPlaceholder() {
        final AddVertexStepPlaceholder<Object> step = new AddVertexStepPlaceholder<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId("placeholderVertex123");
        assertEquals("placeholderVertex123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveElementIdFromAddVertexStepPlaceholderWithGValue() {
        final AddVertexStepPlaceholder<Object> step = new AddVertexStepPlaceholder<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId(GValue.of("vertexId", "gvalueVertex123"));
        assertEquals("gvalueVertex123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldPreserveOtherPropertiesWhenRemovingElementId() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId("vertex123");
        step.addProperty("name", "marko");
        step.addProperty("age", 29);

        assertEquals("vertex123", step.getElementId());
        assertTrue(step.getProperties().containsKey("name"));
        assertTrue(step.getProperties().containsKey("age"));

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
        assertTrue(step.getProperties().containsKey("name"));
        assertTrue(step.getProperties().containsKey("age"));
        assertEquals("marko", step.getProperties().get("name").get(0));
        assertEquals(29, step.getProperties().get("age").get(0));
    }

    @Test
    public void shouldAllowMultipleCallsToRemoveElementId() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId("vertex123");
        assertEquals("vertex123", step.getElementId());

        assertTrue(step.removeElementId());
        assertFalse(step.removeElementId());
        assertFalse(step.removeElementId());

        // no exception thrown
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveElementIdAndAllowResetting() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.setElementId("originalId");
        assertEquals("originalId", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());

        step.setElementId("newId");
        assertEquals("newId", step.getElementId());
    }

    @Test
    public void shouldRemoveExistingPropertyFromAddVertexStep() {
        // Given: AddVertexStep with properties
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.addProperty("name", "marko");
        step.addProperty("age", 29);

        assertTrue(step.getProperties().containsKey("name"));
        assertTrue(step.getProperties().containsKey("age"));

        assertTrue(step.removeProperty("name"));
        assertFalse(step.getProperties().containsKey("name"));
        assertTrue(step.getProperties().containsKey("age"));
        assertFalse(step.removeProperty("name"));
    }

    @Test
    public void shouldReturnFalseWhenRemovingNonExistentPropertyFromAddVertexStep() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");

        step.addProperty("name", "marko");
        assertFalse(step.removeProperty("nonExistent"));
        assertTrue(step.getProperties().containsKey("name"));
    }

    @Test
    public void shouldRemoveExistingPropertyFromAddVertexStepPlaceholder() {
        final AddVertexStepPlaceholder<Object> step = new AddVertexStepPlaceholder<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.addProperty("name", GValue.of("vadas"));
        step.addProperty("age", GValue.of(27));

        assertTrue(step.removeProperty("name"));
        assertFalse(step.getPropertiesWithGValues().containsKey("name"));
        assertTrue(step.getPropertiesWithGValues().containsKey("age"));
        assertFalse(step.removeProperty("name"));
    }

    @Test
    public void shouldRemovePropertyAndAllowReAdding() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.addProperty("name", "originalValue");
        assertTrue(step.removeProperty("name"));

        step.addProperty("name", "newValue");
        assertTrue(step.getProperties().containsKey("name"));
        assertEquals("newValue", step.getProperties().get("name").get(0));
    }

    @Test
    public void shouldRemovePropertyWithMultipleValues() {
        final AddVertexStep<Object> step = new AddVertexStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "person");
        step.addProperty("skill", "java");
        step.addProperty("skill", "python");
        step.addProperty("skill", "gremlin");

        assertEquals(3, step.getProperties().get("skill").size());
        assertTrue(step.removeProperty("skill"));
        assertFalse(step.getProperties().containsKey("skill"));
    }

    @Test
    public void shouldDefaultTheLabelIfNullString() {
        final Traversal.Admin t = mock(Traversal.Admin.class);
        when(t.getTraverserSetSupplier()).thenReturn(TraverserSetSupplier.instance());
        final AddVertexStartStep starStep = new AddVertexStartStep(t, (String) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
        final AddVertexStep step = new AddVertexStep(t, (String) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
    }

    @Test
    public void shouldDefaultTheLabelIfNullTraversal() {
        final Traversal.Admin t = mock(Traversal.Admin.class);
        when(t.getTraverserSetSupplier()).thenReturn(TraverserSetSupplier.instance());
        final AddVertexStartStep starStep = new AddVertexStartStep(t, (Traversal<?, String>) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
        final AddVertexStep step = new AddVertexStep(t, (String) null);
        assertEquals(Vertex.DEFAULT_LABEL, starStep.getParameters().getRaw().get(T.label).get(0));
    }

    @Test
    public void shouldObtainPopInstructions() {
        // Vertex Step Test
        final AddVertexStep addVertexStep = new AddVertexStep(__.identity().asAdmin(),
                (Traversal.Admin) __.select(Pop.first, "b").select("a").select(Pop.last, "c"));

        final HashSet<PopContaining.PopInstruction> expectedOutput = TestDataBuilder.createPopInstructionSet(
                new Object[]{"b", Pop.first},
                new Object[]{"a", Pop.last},
                new Object[]{"c", Pop.last}
        );

        assertEquals(addVertexStep.getPopInstructions(), expectedOutput);

        // Vertex Start Step Test
        final AddVertexStartStep addVertexStartStep = new AddVertexStartStep(__.identity().asAdmin(),
                __.select(Pop.first, "b").select("a").select(Pop.last, "c"));

        assertEquals(addVertexStartStep.getPopInstructions(), expectedOutput);
    }

    @Test
    public void getLabelShouldPinVariable() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("person", ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getLabel());
        verifyVariables(traversal, Set.of("label"), Set.of("id", "a"));
    }

    @Test
    public void getLabelAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(GValue.of("label", "person"), ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getLabelWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }
    
    @Test
    public void getLabelFromConcreteStep() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("person", ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getLabel());
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }

    @Test
    public void getElementIdShouldPinVariable() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("1234", ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getElementId());
        verifyVariables(traversal, Set.of("id"), Set.of("label", "a"));
    }

    @Test
    public void getElementIdAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(GValue.of("id", "1234"), ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getElementIdWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }

    @Test
    public void getElementIdFromConcreteStep() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals("1234", ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getElementId());
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(List.of(29), ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getProperties().get("age"));
        verifyVariables(traversal, Set.of("a"), Set.of("label", "id"));
    }

    @Test
    public void getPropertiesWithGValuesShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(List.of(GValue.of("a", 29)), ((AddVertexStepPlaceholder) traversal.getSteps().get(0))
                .getPropertiesWithGValues().get("age"));
        verifyVariables(traversal, Set.of(), Set.of("label", "id", "a"));
    }

    @Test
    public void getPropertiesFromConcreteStep() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        assertEquals(List.of(29), ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getProperties().get("age"));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<Object, Vertex> traversal = getAddPersonGValueTraversal();
        Collection<GValue<?>> gValues = ((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getGValues();
        assertEquals(3, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("label", "id", "a")));
    }

    @Test
    public void getGValuesNoneShouldReturnEmptyCollection() {
        GraphTraversal.Admin<Object, Vertex> traversal = __.addV("person")
                .property(T.id, "1234")
                .property("age", 29)
                .asAdmin();
        assertTrue(((AddVertexStepPlaceholder) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    private GraphTraversal.Admin<Object, Vertex> getAddPersonGValueTraversal() {
        return __.addV(GValue.of("label", "person"))
                .property(T.id, GValue.of("id", "1234"))
                .property("age", GValue.of("a", 29))
                .asAdmin();
    }
}
