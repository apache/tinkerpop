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

import java.util.HashSet;

import org.apache.tinkerpop.gremlin.TestDataBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.PopContaining;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.function.TraverserSetSupplier;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AddVertexStepTest {

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
        final AddVertexStartStep starStep = new AddVertexStartStep(t, (Traversal<?,String>) null);
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
}
