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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ConstantTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class AddEdgeStepTest extends GValueStepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                __.addE("knows").property("a", "b"),
                __.addE("created").property("a", "b"),
                __.addE("knows").property("a", "b").property("c", "e"),
                __.addE("knows").property("c", "e"),
                __.addE("knows").from(__.V(1)).to(__.V(2)).property("a", "b"),
                __.addE(GValue.of("label", "knows")).property("a", "b"),
                __.addE(GValue.of("label", "created")).property("a", GValue.of("prop", "b")),
                __.addE(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")),
                __.addE(GValue.of("label", "knows")).from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("a",  GValue.of("prop", "b"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(__.addE(GValue.of("label", "knows")).property("a", "b"), Set.of("label")),
                Pair.of(__.addE(GValue.of("label", "created")).property("a", GValue.of("prop", "b")), Set.of("label", "prop")),
                Pair.of(__.addE(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")), Set.of("label", "prop1", "prop2")),
                Pair.of(__.addE(GValue.of("label", "knows")).from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("a",  GValue.of("prop", "b")), Set.of("label", "from", "to", "prop")),
                Pair.of(__.addE("knows").from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("a",  GValue.of("prop", "b")), Set.of("from", "to", "prop"))
        );
    }

    @Test
    public void shouldRemoveElementIdFromAddEdgeStep() {
        final AddEdgeStep<Object> step = new AddEdgeStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        step.setElementId("edge123");
        assertEquals("edge123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveElementIdFromAddEdgeStepWhenIdIsNull() {
        final AddEdgeStep<Object> step = new AddEdgeStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        assertNull(step.getElementId());

        assertFalse(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveElementIdFromAddEdgeStepPlaceholder() {
        final AddEdgeStepPlaceholder<Object> step = new AddEdgeStepPlaceholder<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        step.setElementId("placeholderEdge123");
        assertEquals("placeholderEdge123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveExistingPropertyFromAddEdgeStep() {
        final AddEdgeStep<Object> step = new AddEdgeStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        step.addProperty("weight", 0.8);
        step.addProperty("since", 2010);

        assertTrue(step.getProperties().containsKey("weight"));
        assertTrue(step.getProperties().containsKey("since"));

        assertTrue(step.removeProperty("weight"));
        assertFalse(step.getProperties().containsKey("weight"));
        assertTrue(step.getProperties().containsKey("since"));
        assertFalse(step.removeProperty("weight"));
    }

    @Test
    public void shouldReturnFalseWhenRemovingNonExistentPropertyFromAddEdgeStep() {
        final AddEdgeStep<Object> step = new AddEdgeStep<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        step.addProperty("weight", 0.8);

        assertFalse(step.removeProperty("nonExistent"));
        assertTrue(step.getProperties().containsKey("weight"));
    }

    @Test
    public void shouldRemoveExistingPropertyFromAddEdgeStepPlaceholder() {
        final AddEdgeStepPlaceholder<Object> step = new AddEdgeStepPlaceholder<>(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        step.addProperty("weight", GValue.of(0.5));
        step.addProperty("type", GValue.of("friendship"));

        assertTrue(step.removeProperty("type"));
        assertFalse(step.getPropertiesWithGValues().containsKey("type"));
        assertTrue(step.getPropertiesWithGValues().containsKey("weight"));
        assertFalse(step.removeProperty("type"));
    }


    @Test
    public void shouldObtainPopInstructions() {
        // Edge Step Test
        final AddEdgeStep<Object> addEdgeStep = new AddEdgeStep<>(__.identity().asAdmin(),
                (Traversal.Admin) __.select(Pop.first, "b").select("a"));

        final HashSet<PopContaining.PopInstruction> expectedOutput = TestDataBuilder.createPopInstructionSet(
                new Object[]{"b", Pop.first},
                new Object[]{"a", Pop.last}
        );

        assertEquals(addEdgeStep.getPopInstructions(), expectedOutput);

        // Edge Step Start test
        final AddEdgeStartStep addEdgeStartStep = new AddEdgeStartStep(__.identity().asAdmin(),
                __.select(Pop.first, "b").select("a"));

        assertEquals(addEdgeStartStep.getPopInstructions(), expectedOutput);
    }

    @Test
    public void getLabelShouldPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("likes", ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getLabel());
        verifyVariables(traversal, Set.of("label"), Set.of("from", "to", "id", "r"));
    }

    @Test
    public void getLabelAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("label", "likes"), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getLabelWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getLabelFromConcreteStep() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("likes", ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getLabel());
    }

    @Test
    public void getElementIdShouldPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("1234", ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getElementId());
        verifyVariables(traversal, Set.of("id"), Set.of("label", "from", "to", "r"));
    }

    @Test
    public void getElementIdAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("id", "1234"), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getElementIdWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getElementIdFromConcreteStep() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("1234", ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getElementId());
    }

    @Test
    public void getFromShouldPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(1, ((Vertex) (((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getFrom())).id());
        verifyVariables(traversal, Set.of("from"), Set.of("label", "to", "id", "r"));
    }

    @Test
    public void getFromWithGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("from", 1), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getFromWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getFromFromConcreteStep() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(1, ((Vertex) (((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getFrom())).id());
    }

    @Test
    public void getFromUsingConstantTraversal() {
        GraphTraversal.Admin<Object, Edge> traversal = __.addE(GValue.of("label", "likes"))
                .from(new ConstantTraversal<>(1))
                .to(GValue.of("to", 2))
                .asAdmin();
        assertEquals(1, ((Vertex) (((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getFrom())).id());
        verifyVariables(traversal, Set.of(), Set.of("label", "to"));
    }

    @Test
    public void getToShouldPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(2, ((Vertex) (((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getTo())).id());
        verifyVariables(traversal, Set.of("to"), Set.of("label", "from", "id", "r"));
    }

    @Test
    public void getToWithGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("to", 2), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getToWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getToUsingConstantTraversal() {
        GraphTraversal.Admin<Object, Edge> traversal = __.addE(GValue.of("label", "likes"))
                .to(new ConstantTraversal<>(1))
                .from(GValue.of("from", 2))
                .asAdmin();
        assertEquals(1, ((Vertex) (((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getTo())).id());
        verifyVariables(traversal, Set.of(), Set.of("label", "from"));
    }

    @Test
    public void getToFromConcreteStep() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(2, ((Vertex) (((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).asConcreteStep().getTo())).id());
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(List.of("great"), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0))
                .getProperties().get("rating"));
        verifyVariables(traversal, Set.of("r"), Set.of("label", "from", "to", "id"));
    }

    @Test
    public void getPropertiesWithGValuesShouldNotPinVariable() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(List.of(GValue.of("r", "great")), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0))
                .getPropertiesWithGValues().get("rating"));
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getPropertiesFromConcreteStep() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(List.of("great"), ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0))
                .asConcreteStep().getProperties().get("rating"));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<Object, Edge> traversal = getAddEdgeGValueTraversal();
        Collection<GValue<?>> gValues = ((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getGValues();
        assertEquals(5, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("label", "from", "to", "id", "r")));
    }

    @Test
    public void getGValuesNoneShouldReturnEmptyCollection() {
        GraphTraversal.Admin<Object, Edge> traversal = __.addE("likes")
                .from(__.V(1))
                .to(__.V(2))
                .property(T.id, "1234")
                .property("rating", "great")
                .asAdmin();
        assertTrue(((AddEdgeStepPlaceholder<?>) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    @Test
    public void configuringShouldNotSetProperties() {
        AddEdgeStep<?> step = new AddEdgeStep<>(new DefaultGraphTraversal(), "Edge");
        step.configure("key", "option");
        step.addProperty("prop", "value");
        assertEquals(Map.of("prop", List.of("value")), step.getProperties());
        assertEquals(List.of("option"), step.getParameters().get("key", () -> null));
    }

    private GraphTraversal.Admin<Object, Edge> getAddEdgeGValueTraversal() {
        return __.addE(GValue.of("label", "likes"))
                .from(GValue.of("from", 1))
                .to(GValue.of("to", 2))
                .property(T.id, GValue.of("id", "1234"))
                .property("rating", GValue.of("r", "great"))
                .asAdmin();
    }
}
