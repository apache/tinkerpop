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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.ProgramPhase;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.MemoryTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ProgramVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class ProgramTest extends AbstractGremlinProcessTest {

    public abstract Traversal<Vertex, Vertex> get_g_V_programXpageRankX();

    public abstract Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_ascX_valueMapXname_rankX();

    public abstract Traversal<Vertex, Map<String, Object>> get_g_V_outXcreatedX_aggregateXxX_byXlangX_groupCount_programXTestProgramX_asXaX_selectXa_xX();

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_programXpageRankX() {
        final Traversal<Vertex, Vertex> traversal = get_g_V_programXpageRankX();
        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            final Vertex vertex = traversal.next();
            counter++;
            assertTrue(vertex.property(PageRankVertexProgram.PAGE_RANK).isPresent());
        }
        assertEquals(6, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_ascX_valueMapXname_rankX() {
        final Traversal<Vertex, Map<String, List<Object>>> traversal = get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_ascX_valueMapXname_rankX();
        printTraversalForm(traversal);
        int counter = 0;
        double lastRank = Double.MIN_VALUE;
        while (traversal.hasNext()) {
            final Map<String, List<Object>> map = traversal.next();
            assertEquals(2, map.size());
            assertEquals(1, map.get("name").size());
            assertEquals(1, map.get("rank").size());
            String name = (String) map.get("name").get(0);
            double rank = (Double) map.get("rank").get(0);
            assertTrue(rank >= lastRank);
            lastRank = rank;
            assertFalse(name.equals("lop") || name.equals("ripple"));
            counter++;
        }
        assertEquals(4, counter);
    }

    @Test
    @LoadGraphWith(MODERN)
    public void g_V_outXcreatedX_aggregateXxX_byXlangX_groupCount_programXTestProgramX_asXaX_selectXa_xX() {
        final Traversal<Vertex, Map<String, Object>> traversal = get_g_V_outXcreatedX_aggregateXxX_byXlangX_groupCount_programXTestProgramX_asXaX_selectXa_xX();
        final List<Map<String, Object>> results = traversal.toList();
        assertFalse(traversal.hasNext());
        assertEquals(6, results.size());
        final BulkSet<String> bulkSet = new BulkSet<>();
        bulkSet.add("java", 4);
        for (int i = 0; i < 4; i++) {
            assertEquals(bulkSet, results.get(i).get("x"));
        }
        final Set<String> strings = new HashSet<>();
        strings.add((String) results.get(0).get("a"));
        strings.add((String) results.get(1).get("a"));
        strings.add((String) results.get(2).get("a"));
        strings.add((String) results.get(3).get("a"));
        strings.add((String) results.get(4).get("a"));
        strings.add((String) results.get(5).get("a"));
        assertEquals(6, strings.size());
        assertTrue(strings.contains("hello"));
        assertTrue(strings.contains("gremlin"));
        assertTrue(strings.contains("lop"));
        assertTrue(strings.contains("ripple"));
        assertTrue(strings.contains("marko-is-my-name"));
        assertTrue(strings.contains("the-v-o-double-g"));
    }


    public static class Traversals extends ProgramTest {

        @Override
        public Traversal<Vertex, Vertex> get_g_V_programXpageRankX() {
            return g.V().program(PageRankVertexProgram.build().create(graph));
        }

        @Override
        public Traversal<Vertex, Map<String, List<Object>>> get_g_V_hasLabelXpersonX_programXpageRank_rankX_order_byXrank_ascX_valueMapXname_rankX() {
            return g.V().hasLabel("person").program(PageRankVertexProgram.build().property("rank").create(graph)).order().by("rank", Order.asc).valueMap("name", "rank");
        }

        @Override
        public Traversal<Vertex, Map<String, Object>> get_g_V_outXcreatedX_aggregateXxX_byXlangX_groupCount_programXTestProgramX_asXaX_selectXa_xX() {
            return g.V().out("created").aggregate("x").by("lang").groupCount().program(new TestProgram()).as("a").select("a", "x");
        }
    }

    /////////////////////

    public static class TestProgram implements VertexProgram {

        private PureTraversal<?, ?> traversal = new PureTraversal<>(EmptyTraversal.instance());
        private TraverserSet<Object> haltedTraversers;
        private Step programStep = EmptyStep.instance();

        private final Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>();

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            VertexProgram.super.loadState(graph, configuration);
            this.traversal = PureTraversal.loadState(configuration, VertexProgramStep.ROOT_TRAVERSAL, graph);
            this.haltedTraversers = TraversalVertexProgram.loadHaltedTraversers(configuration);
            this.programStep = new TraversalMatrix<>(this.traversal.get()).getStepById(configuration.getString(ProgramVertexProgramStep.STEP_ID));
            this.memoryComputeKeys.addAll(MemoryTraversalSideEffects.getMemoryComputeKeys(this.traversal.get()));
            this.memoryComputeKeys.add(MemoryComputeKey.of(TraversalVertexProgram.HALTED_TRAVERSERS, Operator.addAll, false, false));
            this.memoryComputeKeys.add(MemoryComputeKey.of(TraversalVertexProgram.ACTIVE_TRAVERSERS, Operator.addAll, true, true));
        }

        @Override
        public void storeState(final Configuration configuration) {
            VertexProgram.super.storeState(configuration);
            this.traversal.storeState(configuration, VertexProgramStep.ROOT_TRAVERSAL);
            TraversalVertexProgram.storeHaltedTraversers(configuration, this.haltedTraversers);
            configuration.setProperty(ProgramVertexProgramStep.STEP_ID, this.programStep.getId());
        }

        @Override
        public void setup(final Memory memory) {
            MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.SETUP);
            final Map<Vertex, Long> map = (Map<Vertex, Long>) this.haltedTraversers.iterator().next().get();
            assertEquals(2, map.size());
            assertTrue(map.values().contains(3l));
            assertTrue(map.values().contains(1l));
            final IndexedTraverserSet<Object,Vertex> activeTraversers = new IndexedTraverserSet.VertexIndexedTraverserSet();
            map.keySet().forEach(vertex -> activeTraversers.add(this.haltedTraversers.peek().split(vertex, EmptyStep.instance())));
            this.haltedTraversers.clear();
            this.checkSideEffects();
            memory.set(TraversalVertexProgram.ACTIVE_TRAVERSERS, activeTraversers);
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            assertFalse(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS));
            final TraverserGenerator generator = this.traversal.get().getTraverserGenerator();
            MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.EXECUTE);
            this.checkSideEffects();
            final TraverserSet<Vertex> activeTraversers = memory.get(TraversalVertexProgram.ACTIVE_TRAVERSERS);
            if (vertex.label().equals("software")) {
                assertEquals(1, activeTraversers.stream().filter(v -> v.get().equals(vertex)).count());
                if (memory.isInitialIteration()) {
                    assertFalse(vertex.property(TraversalVertexProgram.HALTED_TRAVERSERS).isPresent());
                    vertex.property(
                            TraversalVertexProgram.HALTED_TRAVERSERS,
                            new TraverserSet<>(generator.generate(vertex.value("name"), this.programStep, 1l)));
                } else {
                    assertTrue(vertex.property(TraversalVertexProgram.HALTED_TRAVERSERS).isPresent());
                }
            } else {
                assertFalse(vertex.property(TraversalVertexProgram.HALTED_TRAVERSERS).isPresent());
                assertEquals(0, activeTraversers.stream().filter(v -> v.get().equals(vertex)).count());
                if (!memory.isInitialIteration()) {
                    if (vertex.value("name").equals("marko"))
                        memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(generator.generate("marko-is-my-name", this.programStep, 1l)));
                    else if (vertex.value("name").equals("vadas"))
                        this.traversal.get().getSideEffects().add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(generator.generate("the-v-o-double-g", this.programStep, 1l)));
                }
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            final TraverserGenerator generator = this.traversal.get().getTraverserGenerator();
            MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.TERMINATE);
            checkSideEffects();
            if (memory.isInitialIteration()) {
                assertFalse(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS));
                return false;
            } else {
                ///
                assertTrue(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS));
                final TraverserSet<String> haltedTraversers = memory.get(TraversalVertexProgram.HALTED_TRAVERSERS);
                haltedTraversers.add(generator.generate("hello", this.programStep, 1l));
                haltedTraversers.add(generator.generate("gremlin", this.programStep, 1l));
                memory.set(TraversalVertexProgram.HALTED_TRAVERSERS, haltedTraversers);
                return true;
            }
        }

        @Override
        public void workerIterationStart(final Memory memory) {
            assertNotNull(this.haltedTraversers);
            this.haltedTraversers.clear();
            assertFalse(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS));
            MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.WORKER_ITERATION_START);
            checkSideEffects();
        }

        @Override
        public void workerIterationEnd(final Memory memory) {
            assertFalse(memory.exists(TraversalVertexProgram.HALTED_TRAVERSERS));
            MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.WORKER_ITERATION_END);
            checkSideEffects();
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of(TraversalVertexProgram.HALTED_TRAVERSERS, false));
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return this.memoryComputeKeys;
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public TestProgram clone() {
            try {
                final TestProgram clone = (TestProgram) super.clone();
                clone.traversal = this.traversal.clone();
                clone.programStep = new TraversalMatrix<>(clone.traversal.get()).getStepById(this.programStep.getId());
                return clone;
            } catch (final CloneNotSupportedException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.EDGES;
        }

        ////////

        private void checkSideEffects() {
            assertEquals(0, this.haltedTraversers.size());
            assertTrue(this.haltedTraversers.isEmpty());
            final TraversalSideEffects sideEffects = this.traversal.get().getSideEffects();
            assertTrue(sideEffects instanceof MemoryTraversalSideEffects);
            assertEquals(1, sideEffects.keys().size());
            assertFalse(sideEffects.exists(TraversalVertexProgram.HALTED_TRAVERSERS));
            assertTrue(sideEffects.exists("x"));
            final BulkSet<String> bulkSet = sideEffects.get("x");
            assertEquals(4, bulkSet.size());
            assertEquals(4, bulkSet.get("java"));
        }
    }
}
