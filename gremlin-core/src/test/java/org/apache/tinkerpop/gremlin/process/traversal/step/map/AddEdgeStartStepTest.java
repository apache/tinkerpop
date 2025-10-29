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
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValueStepTest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class AddEdgeStartStepTest extends GValueStepTest {

    private static final GraphTraversalSource g = traversal().with(EmptyGraph.instance());

    @Override
    protected List<Traversal> getTraversals() {
        return Arrays.asList(
                g.addE("knows").property("a", "b"),
                g.addE("created").property("a", "b"),
                g.addE("knows").property("a", "b").property("c", "e"),
                g.addE("knows").property("c", "e"),
                g.addE("knows").from(__.V(1)).to(__.V(2)).property("a", "b"),
                g.addE(GValue.of("label", "knows")).property("a", "b"),
                g.addE(GValue.of("label", "created")).property("a", GValue.of("prop", "b")),
                g.addE(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")),
                g.addE(GValue.of("label", "knows")).from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("a", GValue.of("prop", "b"))
        );
    }

    @Override
    protected List<Pair<Traversal, Set<String>>> getGValueTraversals() {
        return List.of(
                Pair.of(g.addE(GValue.of("label", "knows")).property("a", "b"), Set.of("label")),
                Pair.of(g.addE(GValue.of("label", "created")).property("a", GValue.of("prop", "b")), Set.of("label", "prop")),
                Pair.of(g.addE(GValue.of("label", "knows")).property("a", GValue.of("prop1", "b")).property("c", GValue.of("prop2", "e")), Set.of("label", "prop1", "prop2")),
                Pair.of(g.addE(GValue.of("label", "knows")).from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("a", GValue.of("prop", "b")), Set.of("label", "from", "to", "prop")),
                Pair.of(g.addE("knows").from(GValue.of("from", 1)).to(GValue.of("to", 2)).property("a", GValue.of("prop", "b")), Set.of("from", "to", "prop"))
        );
    }

    @Test
    public void shouldRemoveElementIdFromAddEdgeStartStep() {
        final AddEdgeStartStep step = new AddEdgeStartStep(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "knows");
        step.setElementId("startEdge123");
        assertEquals("startEdge123", step.getElementId());

        assertTrue(step.removeElementId());
        assertNull(step.getElementId());
    }

    @Test
    public void shouldRemoveExistingPropertyFromAddEdgeStartStep() {
        final AddEdgeStartStep step = new AddEdgeStartStep(
                new DefaultGraphTraversal<>(EmptyGraph.instance()).asAdmin(), "created");
        step.addProperty("weight", 1.0);
        step.addProperty("year", 2009);

        assertTrue(step.removeProperty("year"));
        assertFalse(step.getProperties().containsKey("year"));
        assertTrue(step.getProperties().containsKey("weight"));
        assertFalse(step.removeProperty("year"));
    }

    @Test
    public void getLabelShouldPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("likes", ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getLabel());
        verifyVariables(traversal, Set.of("label"), Set.of("from", "to", "id", "r"));
    }

    @Test
    public void getLabelAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("label", "likes"), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getLabelWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getLabelFromConcreteStep() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("likes", ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getLabel());
    }

    @Test
    public void getElementIdShouldPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("1234", ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getElementId());
        verifyVariables(traversal, Set.of("id"), Set.of("label", "from", "to", "r"));
    }

    @Test
    public void getElementIdAsGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("id", "1234"), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getElementIdWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getElementIdFromConcreteStep() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals("1234", ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getElementId());
    }

    @Test
    public void getFromShouldPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(1, ((Vertex) (((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getFrom())).id());
        verifyVariables(traversal, Set.of("from"), Set.of("label", "to", "id", "r"));
    }

    @Test
    public void getFromWithGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("from", 1), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getFromWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getFromFromConcreteStep() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(1, ((Vertex) (((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getFrom())).id());
    }

    @Test
    public void getToShouldPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(2, ((Vertex) (((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getTo())).id());
        verifyVariables(traversal, Set.of("to"), Set.of("label", "from", "id", "r"));
    }

    @Test
    public void getToWithGValueShouldNotPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(GValue.of("to", 2), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getToWithGValue());
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getToFromConcreteStep() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(2, ((Vertex) (((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).asConcreteStep().getTo())).id());
    }

    @Test
    public void getPropertiesShouldPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(List.of("great"), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0))
                .getProperties().get("rating"));
        verifyVariables(traversal, Set.of("r"), Set.of("label", "from", "to", "id"));
    }

    @Test
    public void getPropertiesWithGValuesShouldNotPinVariable() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(List.of(GValue.of("r", "great")), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0))
                .getPropertiesWithGValues().get("rating"));
        verifyVariables(traversal, Set.of(), Set.of("label", "from", "to", "id", "r"));
    }

    @Test
    public void getPropertiesFromConcreteStep() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        assertEquals(List.of("great"), ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0))
                .asConcreteStep().getProperties().get("rating"));
    }

    @Test
    public void getGValuesShouldReturnAllGValues() {
        GraphTraversal.Admin<Edge, Edge> traversal = getAddEdgeGValueTraversal();
        Collection<GValue<?>> gValues = ((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getGValues();
        assertEquals(5, gValues.size());
        assertTrue(gValues.stream().map(GValue::getName).collect(Collectors.toList())
                .containsAll(List.of("label", "from", "to", "id", "r")));
    }

    @Test
    public void getGValuesNoneShouldReturnEmptyCollection() {
        GraphTraversal.Admin<Edge, Edge> traversal = g.addE("likes")
                .from(__.V(1))
                .to(__.V(2))
                .property(T.id, "1234")
                .property("rating", "great")
                .asAdmin();
        assertTrue(((AddEdgeStartStepPlaceholder) traversal.getSteps().get(0)).getGValues().isEmpty());
    }

    @Test
    public void configuringShouldNotSetProperties() {
        AddEdgeStartStep step = new AddEdgeStartStep(new DefaultGraphTraversal(), "Edge");
        step.configure("key", "option");
        step.addProperty("prop", "value");
        assertEquals(Map.of("prop", List.of("value")), step.getProperties());
        assertEquals(List.of("option"), step.getParameters().get("key", () -> null));
    }

    private GraphTraversal.Admin<Edge, Edge> getAddEdgeGValueTraversal() {
        return g.addE(GValue.of("label", "likes"))
                .from(GValue.of("from", 1))
                .to(GValue.of("to", 2))
                .property(T.id, GValue.of("id", "1234"))
                .property("rating", GValue.of("r", "great"))
                .asAdmin();
    }
}
