/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VertexProgramStepTest {

    // The default seam is identity (returns the graph unchanged); the non-overriding subclasses rely on this to keep
    // binding and generating against the graph they are handed.
    @Test
    public void shouldResolveComputeGraphToSameGraphByDefault() {
        final Graph graph = EmptyGraph.instance();
        final VertexProgramStep step = new VertexProgramStep(__.start().asAdmin()) {
            @Override
            public VertexProgram generateProgram(final Graph graph, final Memory memory) {
                return null;
            }
        };
        assertSame(graph, step.resolveComputeGraph(graph));
    }

    // The graph resolveComputeGraph returns must flow to BOTH getComputer().apply(graph) and generateProgram(graph, ...).
    // Driving processNextStart with the seam returning a distinct instance, both the computer binding and the program
    // generation must observe that same instance, so a reorder that binds the computer to a different graph than the
    // one configured is caught here.
    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void shouldBindComputerAndGenerateProgramToTheSameResolvedGraph() {
        final Graph handed = mock(Graph.class);    // what the traversal exposes
        final Graph resolved = mock(Graph.class);  // what the seam returns -- distinct, so we can tell them apart

        // Computer is final and cannot be mocked; use a real Computer.compute() whose apply(graph) calls graph.compute().
        // The GraphComputer is produced by the graph the computer is applied to, so stubbing compute() ONLY on `resolved`
        // means the chain completes iff apply() bound to `resolved` -- and we verify that call directly.
        final Memory memory = mock(Memory.class);
        when(memory.keys()).thenReturn(Collections.emptySet());
        final ComputerResult result = mock(ComputerResult.class);
        when(result.memory()).thenReturn(memory);
        final GraphComputer graphComputer = mock(GraphComputer.class);
        when(graphComputer.program(any())).thenReturn(graphComputer);
        when(graphComputer.submit()).thenReturn(CompletableFuture.completedFuture(result));
        when(resolved.compute()).thenReturn(graphComputer);
        final Computer computer = Computer.compute(); // graphComputerClass == GraphComputer.class -> apply calls graph.compute()

        // mock the traversal so no strategies/real graph are involved; getGraph() must be present (non-Empty)
        final TraverserGenerator generator = mock(TraverserGenerator.class);
        when(generator.generate(any(), any(), anyLong())).thenReturn(mock(Traverser.Admin.class));
        final Traversal.Admin traversal = mock(Traversal.Admin.class);
        when(traversal.getTraverserSetSupplier()).thenReturn((java.util.function.Supplier) TraverserSet::new);
        when(traversal.getGraph()).thenReturn(Optional.of(handed));
        when(traversal.getTraverserGenerator()).thenReturn(generator);

        final Graph[] generatedAgainst = new Graph[1];
        final VertexProgramStep step = new VertexProgramStep(traversal) {
            @Override
            public Computer getComputer() {
                return computer;
            }

            @Override
            protected Graph resolveComputeGraph(final Graph graph) {
                return resolved;
            }

            @Override
            public VertexProgram generateProgram(final Graph graph, final Memory memory) {
                generatedAgainst[0] = graph;
                return mock(VertexProgram.class);
            }
        };

        step.processNextStart();

        // the computer was applied to the resolved graph (apply() -> graph.compute() on `resolved`, never on `handed`)
        verify(resolved).compute();
        verify(handed, never()).compute();
        // and the program was generated against that same resolved graph
        assertSame("the program must be generated against the graph the seam resolved", resolved, generatedAgainst[0]);
    }
}
