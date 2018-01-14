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
package org.apache.tinkerpop.gremlin.process.computer;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyPath;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeNoException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@ExceptionCoverage(exceptionClass = GraphComputer.Exceptions.class, methods = {
        "providedKeyIsNotAMemoryComputeKey",
        "computerHasNoVertexProgramNorMapReducers",
        "computerHasAlreadyBeenSubmittedAVertexProgram",
        "providedKeyIsNotAnElementComputeKey",
        "incidentAndAdjacentElementsCanNotBeAccessedInMapReduce",
        "adjacentVertexLabelsCanNotBeRead",
        "adjacentVertexPropertiesCanNotBeReadOrUpdated",
        "adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated",
        "resultGraphPersistCombinationNotSupported",
        "vertexPropertiesCanNotBeUpdatedInMapReduce",
        "computerRequiresMoreWorkersThanSupported",
        "vertexFilterAccessesIncidentEdges",
        "edgeFilterAccessesAdjacentVertices",
        "graphFilterNotSupported"
})
@ExceptionCoverage(exceptionClass = Memory.Exceptions.class, methods = {
        "memoryKeyCanNotBeEmpty",
        "memoryKeyCanNotBeNull",
        "memoryValueCanNotBeNull",
        "memoryIsCurrentlyImmutable",
        "memoryDoesNotExist",
        "memorySetOnlyDuringVertexProgramSetUpAndTerminate",
        "memoryAddOnlyDuringVertexProgramExecute",
        "adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated"
})
@ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
        "graphDoesNotSupportProvidedGraphComputer"
})
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
public class GraphComputerTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveStandardStringRepresentation() {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        assertEquals(StringFactory.graphComputerString(computer), computer.toString());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowWithNoVertexProgramNorMapReducers() throws Exception {
        try {
            graphProvider.getGraphComputer(graph).submit().get();
            fail("Should throw an IllegalStateException when there is no vertex program nor map reducers");
        } catch (Exception ex) {
            validateException(GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers(), ex);
        }
    }


    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowBadGraphComputers() {
        try {
            graph.compute(BadGraphComputer.class);
            fail("Providing a bad graph computer class should fail");
        } catch (Exception ex) {
            validateException(Graph.Exceptions.graphDoesNotSupportProvidedGraphComputer(BadGraphComputer.class), ex);
        }
        if (!new BadGraphComputer().features().supportsGraphFilter()) {
            try {
                new BadGraphComputer().vertices(__.hasLabel("software"));
                fail("Should throw an unsupported operation exception");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.graphFilterNotSupported().getMessage(), e.getMessage());
            }
            try {
                new BadGraphComputer().edges(__.bothE());
                fail("Should throw an unsupported operation exception");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.graphFilterNotSupported().getMessage(), e.getMessage());
            }
        } else {
            fail("Should not support graph filter: " + BadGraphComputer.class);
        }
    }

    public static class BadGraphComputer implements GraphComputer {

        @Override
        public GraphComputer result(final ResultGraph resultGraph) {
            return null;
        }

        @Override
        public GraphComputer persist(final Persist persist) {
            return null;
        }

        @Override
        public GraphComputer program(final VertexProgram vertexProgram) {
            return null;
        }

        @Override
        public GraphComputer mapReduce(final MapReduce mapReduce) {
            return null;
        }

        @Override
        public GraphComputer workers(final int workers) {
            return null;
        }

        @Override
        public GraphComputer vertices(final Traversal<Vertex, Vertex> vertexFilter) {
            throw GraphComputer.Exceptions.graphFilterNotSupported();
        }

        @Override
        public GraphComputer edges(final Traversal<Vertex, Edge> edgeFilter) {
            throw GraphComputer.Exceptions.graphFilterNotSupported();
        }

        @Override
        public GraphComputer configure(final String key, final Object value) {
            return null;
        }

        @Override
        public Future<ComputerResult> submit() {
            return null;
        }

        public GraphComputer.Features features() {
            return new Features() {
                @Override
                public boolean supportsGraphFilter() {
                    return false;
                }
            };
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveImmutableComputeResultMemory() throws Exception {
        final ComputerResult results = graphProvider.getGraphComputer(graph).program(new VertexProgramB()).submit().get();

        try {
            results.memory().set("set", "test");
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }

        try {
            results.memory().add("incr", 1);
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }

        try {
            results.memory().add("and", true);
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }

        try {
            results.memory().add("or", false);
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }
    }

    public static class VertexProgramB extends StaticVertexProgram {
        @Override
        public void setup(final Memory memory) {
            assertEquals(0, memory.getIteration());
            assertTrue(memory.isInitialIteration());
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            assertEquals(0, memory.getIteration());
            assertTrue(memory.isInitialIteration());
        }

        @Override
        public boolean terminate(final Memory memory) {
            assertEquals(0, memory.getIteration());
            assertTrue(memory.isInitialIteration());
            return true;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(
                    MemoryComputeKey.of("set", Operator.assign, true, false),
                    MemoryComputeKey.of("incr", Operator.sum, true, false),
                    MemoryComputeKey.of("and", Operator.and, true, false),
                    MemoryComputeKey.of("or", Operator.or, true, false)));
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    /////////////////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowNullMemoryKeys() throws Exception {
        try {
            graphProvider.getGraphComputer(graph).program(new VertexProgramC()).submit().get();
            fail("Providing null memory key should fail");
        } catch (Exception ex) {
            // validateException(Memory.Exceptions.memoryKeyCanNotBeNull(), ex);
        }
    }

    public static class VertexProgramC extends StaticVertexProgram {
        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {

        }

        @Override
        public boolean terminate(final Memory memory) {
            return true;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.singleton(MemoryComputeKey.of(null, Operator.or, true, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowEmptyMemoryKeys() throws Exception {
        try {
            graphProvider.getGraphComputer(graph).program(new VertexProgramD()).submit().get();
            fail("Providing empty memory key should fail");
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryKeyCanNotBeEmpty(), ex);
        }
    }

    public static class VertexProgramD extends StaticVertexProgram {
        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {

        }

        @Override
        public boolean terminate(final Memory memory) {
            return true;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.singleton(MemoryComputeKey.of("", Operator.or, true, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    ////////////////////////////////////////////

    ////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldHandleUndeclaredMemoryKeysCorrectly() throws Exception {
        graphProvider.getGraphComputer(graph).program(new VertexProgramE()).submit().get();
    }

    public static class VertexProgramE extends StaticVertexProgram {
        @Override
        public void setup(final Memory memory) {
            try {
                memory.get("a");
                fail("The memory key does not exist and should fail");
            } catch (Exception e) {
                validateException(Memory.Exceptions.memoryDoesNotExist("a"), e);
            }
            try {
                memory.set("a", true);
                fail("Setting a memory key that wasn't declared should fail");
            } catch (Exception e) {
                validateException(GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey("a"), e);
            }
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            try {
                memory.get("a");
                fail("The memory key does not exist and should fail");
            } catch (Exception e) {
                validateException(Memory.Exceptions.memoryDoesNotExist("a"), e);
            }
            try {
                memory.add("a", true);
                fail("Setting a memory key that wasn't declared should fail");
            } catch (Exception e) {
                validateException(GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey("a"), e);
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            try {
                memory.get("a");
                fail("The memory key does not exist and should fail");
            } catch (Exception e) {
                validateException(Memory.Exceptions.memoryDoesNotExist("a"), e);
            }
            try {
                memory.set("a", true);
                // fail("Setting a memory key that wasn't declared should fail");
            } catch (Exception e) {
                validateException(GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey("a"), e);
            }
            return true;
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    ////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowTheSameComputerToExecutedTwice() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph).program(new VertexProgramA());
        computer.submit().get(); // this should work as its the first run of the graph computer

        try {
            computer.submit(); // this should fail as the computer has already been executed
            fail("Using the same graph computer to compute again should not be possible");
        } catch (IllegalStateException e) {

        } catch (Exception e) {
            fail("Should yield an illegal state exception for graph computer being executed twice");
        }

        // test no rerun of graph computer
        try {
            computer.submit(); // this should fail as the computer has already been executed even through new program submitted
            fail("Using the same graph computer to compute again should not be possible");
        } catch (IllegalStateException e) {

        } catch (Exception e) {
            fail("Should yield an illegal state exception for graph computer being executed twice");
        }
    }

    public static class VertexProgramA extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {

        }

        @Override
        public boolean terminate(final Memory memory) {
            return true;
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.ORIGINAL;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveConsistentMemoryVertexPropertiesAndExceptions() throws Exception {
        ComputerResult results = graphProvider.getGraphComputer(graph).program(new VertexProgramF()).submit().get();
        assertEquals(1, results.memory().getIteration());
        assertEquals(2, results.memory().asMap().size());
        assertEquals(2, results.memory().keys().size());
        assertTrue(results.memory().keys().contains("a"));
        assertTrue(results.memory().keys().contains("b"));
        assertTrue(results.memory().getRuntime() >= 0);

        assertEquals(12, results.memory().<Integer>get("a").intValue());   // 2 iterations
        assertEquals(28, results.memory().<Integer>get("b").intValue());
        try {
            results.memory().get("BAD");
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(Memory.Exceptions.memoryDoesNotExist("BAD").getMessage(), e.getMessage());
        }
        assertEquals(Long.valueOf(0), results.graph().traversal().V().count().next()); // persist new/nothing.

        results.graph().traversal().V().forEachRemaining(v -> {
            assertTrue(v.property("nameLengthCounter").isPresent());
            assertEquals(Integer.valueOf(v.<String>value("name").length() * 2), Integer.valueOf(v.<Integer>value("nameLengthCounter")));
        });
    }

    public static class VertexProgramF extends StaticVertexProgram<Object> {

        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger<Object> messenger, final Memory memory) {
            try {
                vertex.property(VertexProperty.Cardinality.single, "blah", "blah");
                fail("Should throw an IllegalArgumentException");
            } catch (final IllegalArgumentException e) {
                assertEquals(GraphComputer.Exceptions.providedKeyIsNotAnElementComputeKey("blah").getMessage(), e.getMessage());
            } catch (final Exception e) {
                fail("Should throw an IllegalArgumentException: " + e);
            }

            memory.add("a", 1);
            if (memory.isInitialIteration()) {
                vertex.property(VertexProperty.Cardinality.single, "nameLengthCounter", vertex.<String>value("name").length());
                memory.add("b", vertex.<String>value("name").length());
            } else {
                vertex.property(VertexProperty.Cardinality.single, "nameLengthCounter", vertex.<String>value("name").length() + vertex.<Integer>value("nameLengthCounter"));
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() == 1;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of("nameLengthCounter", false));
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(
                    MemoryComputeKey.of("a", Operator.sum, true, false),
                    MemoryComputeKey.of("b", Operator.sum, true, false)));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldAndOrIncrCorrectlyThroughSubStages() throws Exception {
        ComputerResult results = graphProvider.getGraphComputer(graph).program(new VertexProgramG()).submit().get();
        assertEquals(2, results.memory().getIteration());
        assertEquals(6, results.memory().asMap().size());
        assertEquals(6, results.memory().keys().size());
        assertTrue(results.memory().keys().contains("a"));
        assertTrue(results.memory().keys().contains("b"));
        assertTrue(results.memory().keys().contains("c"));
        assertTrue(results.memory().keys().contains("d"));
        assertTrue(results.memory().keys().contains("e"));
        assertTrue(results.memory().keys().contains("f"));

        assertEquals(Long.valueOf(18), results.memory().get("a"));
        assertEquals(Long.valueOf(0), results.memory().get("b"));
        assertFalse(results.memory().get("c"));
        assertTrue(results.memory().get("d"));
        assertTrue(results.memory().get("e"));
        assertEquals(3, results.memory().<Integer>get("f").intValue());
    }

    public static class VertexProgramG extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {
            memory.set("a", 0l);
            memory.set("b", 0l);
            memory.set("c", true);
            memory.set("d", false);
            memory.set("e", true);
            memory.set("f", memory.getIteration());
            try {
                memory.add("a", 0l);
                fail("Should only allow Memory.set() during VertexProgram.setup()");
            } catch (final Exception e) {
                validateException(Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute("a"), e);
            }
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            // test current step values
            assertEquals(Long.valueOf(6 * memory.getIteration()), memory.get("a"));
            assertEquals(Long.valueOf(0), memory.get("b"));
            if (memory.isInitialIteration()) {
                assertTrue(memory.get("c"));
                assertFalse(memory.get("d"));
            } else {
                assertFalse(memory.get("c"));
                assertTrue(memory.get("d"));
            }
            assertTrue(memory.get("e"));
            assertEquals(memory.getIteration(), memory.<Integer>get("f").intValue());

            // update current step values
            memory.add("a", 1l);
            memory.add("b", 1l);
            memory.add("c", false);
            memory.add("d", true);
            memory.add("e", false);
            memory.add("f", memory.getIteration() + 1);

            // test current step values, should be the same as previous prior to update
            assertEquals(Long.valueOf(6 * memory.getIteration()), memory.get("a"));
            assertEquals(Long.valueOf(0), memory.get("b"));
            if (memory.isInitialIteration()) {
                assertTrue(memory.get("c"));
                assertFalse(memory.get("d"));
            } else {
                assertFalse(memory.get("c"));
                assertTrue(memory.get("d"));
            }
            assertTrue(memory.get("e"));
            assertEquals(memory.getIteration(), memory.<Integer>get("f").intValue());
            try {
                memory.set("a", 0l);
                fail("Should only allow Memory.add() during VertexProgram.execute()");
            } catch (final Exception e) {
                validateException(Memory.Exceptions.memorySetOnlyDuringVertexProgramSetUpAndTerminate("a"), e);
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            assertEquals(Long.valueOf(6 * (memory.getIteration() + 1)), memory.get("a"));
            assertEquals(Long.valueOf(6), memory.get("b"));
            assertFalse(memory.get("c"));
            assertTrue(memory.get("d"));
            assertFalse(memory.get("e"));
            assertEquals(memory.getIteration() + 1, memory.<Integer>get("f").intValue());
            memory.set("b", 0l);
            memory.set("e", true);
            try {
                memory.add("a", 0l);
                fail("Should only allow Memory.set() during VertexProgram.terminate()");
            } catch (final Exception e) {
                validateException(Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute("a"), e);
            }
            return memory.getIteration() > 1;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(
                    MemoryComputeKey.of("a", Operator.sum, true, false),
                    MemoryComputeKey.of("b", Operator.sum, true, false),
                    MemoryComputeKey.of("c", Operator.and, true, false),
                    MemoryComputeKey.of("d", Operator.or, true, false),
                    MemoryComputeKey.of("e", Operator.and, true, false),
                    MemoryComputeKey.of("f", Operator.assign, true, false)));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldAllowMapReduceWithNoVertexProgram() throws Exception {
        final ComputerResult results = graphProvider.getGraphComputer(graph).mapReduce(new MapReduceA()).submit().get();
        assertEquals(123, results.memory().<Integer>get("ageSum").intValue());
    }

    private static class MapReduceA extends StaticMapReduce<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer> {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP) || stage.equals(Stage.REDUCE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Integer> emitter) {
            vertex.<Integer>property("age").ifPresent(emitter::emit);
        }

        @Override
        public void reduce(NullObject key, Iterator<Integer> values, ReduceEmitter<NullObject, Integer> emitter) {
            int sum = 0;
            while (values.hasNext()) {
                sum = sum + values.next();
            }
            emitter.emit(sum);
        }

        @Override
        public Integer generateFinalResult(Iterator<KeyValue<NullObject, Integer>> keyValues) {
            return keyValues.next().getValue();
        }

        @Override
        public String getMemoryKey() {
            return "ageSum";
        }
    }
    /////////////////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportMultipleMapReduceJobs() throws Exception {
        final ComputerResult results = graphProvider.getGraphComputer(graph)
                .program(new VertexProgramH())
                .mapReduce(new MapReduceH1())
                .mapReduce(new MapReduceH2()).submit().get();
        assertEquals(60, results.memory().<Integer>get("a").intValue());
        assertEquals(1, results.memory().<Integer>get("b").intValue());
    }

    public static class VertexProgramH extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(Vertex vertex, Messenger messenger, Memory memory) {
            vertex.property(VertexProperty.Cardinality.single, "counter", memory.isInitialIteration() ? 1 : vertex.<Integer>value("counter") + 1);
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() > 8;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of("counter", false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }

    private static class MapReduceH1 extends StaticMapReduce<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer> {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP) || stage.equals(Stage.REDUCE);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Integer> emitter) {
            vertex.<Integer>property("counter").ifPresent(emitter::emit);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Integer> values, final ReduceEmitter<NullObject, Integer> emitter) {
            int sum = 0;
            while (values.hasNext()) {
                sum = sum + values.next();
            }
            emitter.emit(sum);
        }

        @Override
        public Integer generateFinalResult(final Iterator<KeyValue<NullObject, Integer>> keyValues) {
            return keyValues.next().getValue();
        }

        @Override
        public String getMemoryKey() {
            return "a";
        }
    }

    private static class MapReduceH2 extends StaticMapReduce<Integer, Integer, Integer, Integer, Integer> {

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<Integer, Integer> emitter) {
            vertex.<Integer>property("age").ifPresent(age -> emitter.emit(age, age));
        }

        @Override
        public void combine(Integer key, Iterator<Integer> values, ReduceEmitter<Integer, Integer> emitter) {
            values.forEachRemaining(i -> emitter.emit(i, 1));
        }

        @Override
        public void reduce(Integer key, Iterator<Integer> values, ReduceEmitter<Integer, Integer> emitter) {
            values.forEachRemaining(i -> emitter.emit(i, 1));
        }

        @Override
        public Integer generateFinalResult(Iterator<KeyValue<Integer, Integer>> keyValues) {
            return keyValues.next().getValue();
        }

        @Override
        public String getMemoryKey() {
            return "b";
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldSortReduceOutput() throws Exception {
        final ComputerResult results = graphProvider.getGraphComputer(graph).mapReduce(new MapReduceB()).submit().get();
        final List<Integer> nameLengths = results.memory().get("nameLengths");
        assertEquals(6, nameLengths.size());
        for (int i = 1; i < nameLengths.size(); i++) {
            assertTrue(nameLengths.get(i) <= nameLengths.get(i - 1));
        }
    }

    public static class MapReduceB extends StaticMapReduce<Integer, Integer, Integer, Integer, List<Integer>> {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.REDUCE) || stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<Integer, Integer> emitter) {
            emitter.emit(vertex.<String>value("name").length(), vertex.<String>value("name").length());
        }

        @Override
        public void reduce(Integer key, Iterator<Integer> values, ReduceEmitter<Integer, Integer> emitter) {
            values.forEachRemaining(id -> emitter.emit(id, id));
        }

        @Override
        public Optional<Comparator<Integer>> getReduceKeySort() {
            return Optional.of(Comparator.<Integer>reverseOrder());
        }

        @Override
        public String getMemoryKey() {
            return "nameLengths";
        }

        @Override
        public List<Integer> generateFinalResult(final Iterator<KeyValue<Integer, Integer>> keyValues) {
            final List<Integer> list = new ArrayList<>();
            keyValues.forEachRemaining(nameLength -> list.add(nameLength.getKey()));
            return list;
        }
    }

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldSortMapOutput() throws Exception {
        final ComputerResult results = graphProvider.getGraphComputer(graph).mapReduce(new MapReduceBB()).submit().get();
        final List<Integer> nameLengths = results.memory().get("nameLengths");
        assertEquals(6, nameLengths.size());
        for (int i = 1; i < nameLengths.size(); i++) {
            assertTrue(nameLengths.get(i) <= nameLengths.get(i - 1));
        }
    }

    public static class MapReduceBB extends StaticMapReduce<Integer, Integer, Integer, Integer, List<Integer>> {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<Integer, Integer> emitter) {
            emitter.emit(vertex.<String>value("name").length(), vertex.<String>value("name").length());
        }

        @Override
        public Optional<Comparator<Integer>> getMapKeySort() {
            return Optional.of(Comparator.<Integer>reverseOrder());
        }

        @Override
        public String getMemoryKey() {
            return "nameLengths";
        }

        @Override
        public List<Integer> generateFinalResult(final Iterator<KeyValue<Integer, Integer>> keyValues) {
            final List<Integer> list = new ArrayList<>();
            keyValues.forEachRemaining(nameLength -> list.add(nameLength.getKey()));
            return list;
        }
    }


    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldOnlyAllowReadingVertexPropertiesInMapReduce() throws Exception {
        graphProvider.getGraphComputer(graph).mapReduce(new MapReduceC()).submit().get();
    }

    public static class MapReduceC extends StaticMapReduce<MapReduce.NullObject, MapReduce.NullObject, MapReduce.NullObject, MapReduce.NullObject, MapReduce.NullObject> {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<MapReduce.NullObject, MapReduce.NullObject> emitter) {
            try {
                vertex.edges(Direction.OUT);
                fail("Edges should not be accessible in MapReduce.map()");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce().getMessage(), e.getMessage());
            }
            try {
                vertex.edges(Direction.IN);
                fail("Edges should not be accessible in MapReduce.map()");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce().getMessage(), e.getMessage());
            }
            try {
                vertex.edges(Direction.BOTH);
                fail("Edges should not be accessible in MapReduce.map()");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.incidentAndAdjacentElementsCanNotBeAccessedInMapReduce().getMessage(), e.getMessage());
            }
            ////
            try {
                vertex.property("name", "bob");
                fail("Vertex properties should be immutable in MapReduce.map()");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce().getMessage(), e.getMessage());
            }
            try {
                vertex.property("name").property("test", 1);
                fail("Vertex properties should be immutable in MapReduce.map()");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.vertexPropertiesCanNotBeUpdatedInMapReduce().getMessage(), e.getMessage());
            }

        }

        @Override
        public String getMemoryKey() {
            return "nothing";
        }

        @Override
        public MapReduce.NullObject generateFinalResult(final Iterator<KeyValue<MapReduce.NullObject, MapReduce.NullObject>> keyValues) {
            return MapReduce.NullObject.instance();
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldOnlyAllowIDAccessOfAdjacentVertices() throws Exception {
        graphProvider.getGraphComputer(graph).program(new VertexProgramI()).submit().get();
    }

    public static class VertexProgramI extends StaticVertexProgram<MapReduce.NullObject> {

        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(Vertex vertex, Messenger messenger, Memory memory) {
            vertex.vertices(Direction.OUT).forEachRemaining(Vertex::id);
            vertex.vertices(Direction.IN).forEachRemaining(Vertex::id);
            vertex.vertices(Direction.BOTH).forEachRemaining(Vertex::id);
            if (vertex.vertices(Direction.OUT).hasNext()) {
                try {
                    vertex.vertices(Direction.OUT).forEachRemaining(Vertex::label);
                    fail("Adjacent vertex labels should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexLabelsCanNotBeRead().getMessage(), e.getMessage());
                }
            }
            if (vertex.vertices(Direction.IN).hasNext()) {
                try {
                    vertex.vertices(Direction.IN).forEachRemaining(Vertex::label);
                    fail("Adjacent vertex labels should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexLabelsCanNotBeRead().getMessage(), e.getMessage());
                }
            }
            if (vertex.vertices(Direction.BOTH).hasNext()) {
                try {
                    vertex.vertices(Direction.BOTH).forEachRemaining(Vertex::label);
                    fail("Adjacent vertex labels should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexLabelsCanNotBeRead().getMessage(), e.getMessage());
                }
            }
            ////////////////////
            if (vertex.vertices(Direction.OUT).hasNext()) {
                try {
                    vertex.vertices(Direction.OUT).forEachRemaining(v -> v.property("name"));
                    fail("Adjacent vertex properties should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated().getMessage(), e.getMessage());
                }
            }
            if (vertex.vertices(Direction.IN).hasNext()) {
                try {
                    vertex.vertices(Direction.IN).forEachRemaining(v -> v.property("name"));
                    fail("Adjacent vertex properties should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated().getMessage(), e.getMessage());
                }
            }
            if (vertex.vertices(Direction.BOTH).hasNext()) {
                try {
                    vertex.vertices(Direction.BOTH).forEachRemaining(v -> v.property("name"));
                    fail("Adjacent vertex properties should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexPropertiesCanNotBeReadOrUpdated().getMessage(), e.getMessage());
                }
            }
            ////////////////////
            if (vertex.vertices(Direction.BOTH).hasNext()) {
                try {
                    vertex.vertices(Direction.BOTH).forEachRemaining(v -> v.edges(Direction.BOTH));
                    fail("Adjacent vertex edges should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated().getMessage(), e.getMessage());
                }
            }
            if (vertex.vertices(Direction.BOTH).hasNext()) {
                try {
                    vertex.vertices(Direction.BOTH).forEachRemaining(v -> v.vertices(Direction.BOTH));
                    fail("Adjacent vertex vertices should not be accessible in VertexProgram.execute()");
                } catch (UnsupportedOperationException e) {
                    assertEquals(GraphComputer.Exceptions.adjacentVertexEdgesAndVerticesCanNotBeReadOrUpdated().getMessage(), e.getMessage());
                }
            }

        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() > 1;
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldStartAndEndWorkersForVertexProgramAndMapReduce() throws Exception {
        MapReduceI.WORKER_START.clear();
        MapReduceI.WORKER_END.clear();
        assertEquals(3, graphProvider.getGraphComputer(graph).program(new VertexProgramJ()).mapReduce(new MapReduceI()).submit().get().memory().<Integer>get("a").intValue());
        if (MapReduceI.WORKER_START.size() == 2) {
            assertEquals(2, MapReduceI.WORKER_START.size());
            assertTrue(MapReduceI.WORKER_START.contains(MapReduce.Stage.MAP) && MapReduceI.WORKER_START.contains(MapReduce.Stage.REDUCE));
        } else {
            assertEquals(3, MapReduceI.WORKER_START.size());
            assertTrue(MapReduceI.WORKER_START.contains(MapReduce.Stage.MAP) && MapReduceI.WORKER_START.contains(MapReduce.Stage.COMBINE) && MapReduceI.WORKER_START.contains(MapReduce.Stage.REDUCE));
        }
        if (MapReduceI.WORKER_END.size() == 2) {
            assertEquals(2, MapReduceI.WORKER_END.size());
            assertTrue(MapReduceI.WORKER_END.contains(MapReduce.Stage.MAP) && MapReduceI.WORKER_END.contains(MapReduce.Stage.REDUCE));
        } else {
            assertEquals(3, MapReduceI.WORKER_END.size());
            assertTrue(MapReduceI.WORKER_END.contains(MapReduce.Stage.MAP) && MapReduceI.WORKER_END.contains(MapReduce.Stage.COMBINE) && MapReduceI.WORKER_END.contains(MapReduce.Stage.REDUCE));
        }
    }

    public static class VertexProgramJ extends StaticVertexProgram {


        @Override
        public void setup(final Memory memory) {
            memory.set("test", memory.getIteration());
        }

        @Override
        public void workerIterationStart(final Memory memory) {
            assertEquals(memory.getIteration() * 6, memory.<Integer>get("test").intValue());
            try {
                memory.add("test", memory.getIteration());
                fail("Should throw an immutable memory exception");
            } catch (IllegalStateException e) {
                assertEquals(Memory.Exceptions.memoryIsCurrentlyImmutable().getMessage(), e.getMessage());
            }
        }

        @Override
        public void execute(Vertex vertex, Messenger messenger, Memory memory) {
            assertEquals(memory.getIteration() * 6, memory.<Integer>get("test").intValue());
            memory.add("test", 1);
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() > 3;
        }

        @Override
        public void workerIterationEnd(final Memory memory) {
            assertEquals(memory.getIteration() * 6, memory.<Integer>get("test").intValue());
            try {
                memory.set("test", memory.getIteration());
                fail("Should throw an immutable memory exception");
            } catch (IllegalStateException e) {
                assertEquals(Memory.Exceptions.memoryIsCurrentlyImmutable().getMessage(), e.getMessage());
            }
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.singleton(MemoryComputeKey.of("test", Operator.sum, true, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }
    }

    private static class MapReduceI extends StaticMapReduce<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer> {

        private static final Set<Stage> WORKER_START = new ConcurrentSkipListSet<>();
        private static final Set<Stage> WORKER_END = new ConcurrentSkipListSet<>();

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void workerStart(final Stage stage) {
            WORKER_START.add(stage);
            if (!stage.equals(Stage.MAP))
                assertFalse(WORKER_END.isEmpty());
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Integer> emitter) {
            emitter.emit(1);
            assertEquals(1, WORKER_START.size());
            assertTrue(WORKER_START.contains(Stage.MAP));
        }

        @Override
        public void combine(final NullObject key, final Iterator<Integer> values, final ReduceEmitter<NullObject, Integer> emitter) {
            emitter.emit(2);
            assertEquals(2, WORKER_START.size());
            assertTrue(WORKER_START.contains(Stage.MAP) && WORKER_START.contains(Stage.COMBINE));
            assertFalse(WORKER_END.isEmpty());
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Integer> values, final ReduceEmitter<NullObject, Integer> emitter) {
            emitter.emit(3);
            if (WORKER_START.size() == 2) {
                assertEquals(2, WORKER_START.size());
                assertTrue(WORKER_START.contains(Stage.MAP) && WORKER_START.contains(Stage.REDUCE));
            } else {
                assertEquals(3, WORKER_START.size());
                assertTrue(WORKER_START.contains(Stage.MAP) && WORKER_START.contains(Stage.COMBINE) && WORKER_START.contains(Stage.REDUCE));
            }
            assertFalse(WORKER_END.isEmpty());
        }

        @Override
        public void workerEnd(final Stage stage) {
            assertFalse(WORKER_START.isEmpty());
            if (!stage.equals(Stage.MAP))
                assertFalse(WORKER_END.isEmpty());
            WORKER_END.add(stage);
        }

        @Override
        public Integer generateFinalResult(final Iterator<KeyValue<NullObject, Integer>> keyValues) {
            assertEquals(3, keyValues.next().getValue().intValue());
            return 3;
        }

        @Override
        public String getMemoryKey() {
            return "a";
        }
    }

    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith
    public void shouldSupportPersistResultGraphPairsStatedInFeatures() throws Exception {
        for (final GraphComputer.ResultGraph resultGraph : GraphComputer.ResultGraph.values()) {
            for (final GraphComputer.Persist persist : GraphComputer.Persist.values()) {
                final GraphComputer computer = graphProvider.getGraphComputer(graph);
                if (computer.features().supportsResultGraphPersistCombination(resultGraph, persist)) {
                    computer.program(new VertexProgramK()).result(resultGraph).persist(persist).submit().get();
                } else {
                    try {
                        computer.program(new VertexProgramK()).result(resultGraph).persist(persist).submit().get();
                        fail("The GraphComputer " + computer + " states that it does support the following resultGraph/persist pair: " + resultGraph + ":" + persist);
                    } catch (final IllegalArgumentException e) {
                        assertEquals(GraphComputer.Exceptions.resultGraphPersistCombinationNotSupported(resultGraph, persist).getMessage(), e.getMessage());
                    }
                }
            }
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldProcessResultGraphNewWithPersistNothing() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        if (computer.features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.NOTHING)) {
            final ComputerResult result = computer.program(new VertexProgramK()).result(GraphComputer.ResultGraph.NEW).persist(GraphComputer.Persist.NOTHING).submit().get();
            assertEquals(Long.valueOf(0l), result.graph().traversal().V().count().next());
            assertEquals(Long.valueOf(0l), result.graph().traversal().E().count().next());
            assertEquals(Long.valueOf(0l), result.graph().traversal().V().values().count().next());
            assertEquals(Long.valueOf(0l), result.graph().traversal().E().values().count().next());
            assertEquals(0, result.graph().traversal().V().values("money").sum().next());
            ///
            assertEquals(Long.valueOf(6l), graph.traversal().V().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().count().next());
            assertEquals(Long.valueOf(12l), graph.traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().values().count().next());
            assertEquals(0, graph.traversal().V().values("money").sum().next());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldProcessResultGraphNewWithPersistVertexProperties() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        if (computer.features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = computer.program(new VertexProgramK()).result(GraphComputer.ResultGraph.NEW).persist(GraphComputer.Persist.VERTEX_PROPERTIES).submit().get();
            assertEquals(Long.valueOf(6l), result.graph().traversal().V().count().next());
            assertEquals(Long.valueOf(0l), result.graph().traversal().E().count().next());
            assertEquals(Long.valueOf(18l), result.graph().traversal().V().values().count().next());
            assertEquals(Long.valueOf(0l), result.graph().traversal().E().values().count().next());
            assertEquals(28l, result.graph().traversal().V().values("money").sum().next());
            ///
            assertEquals(Long.valueOf(6l), graph.traversal().V().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().count().next());
            assertEquals(Long.valueOf(12l), graph.traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().values().count().next());
            assertEquals(0, graph.traversal().V().values("money").sum().next());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldProcessResultGraphNewWithPersistEdges() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        if (computer.features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.NEW, GraphComputer.Persist.EDGES)) {
            final ComputerResult result = computer.program(new VertexProgramK()).result(GraphComputer.ResultGraph.NEW).persist(GraphComputer.Persist.EDGES).submit().get();
            assertEquals(Long.valueOf(6l), result.graph().traversal().V().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().count().next());
            assertEquals(Long.valueOf(18l), result.graph().traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().values().count().next());
            assertEquals(28l, result.graph().traversal().V().values("money").sum().next());
            ///
            assertEquals(Long.valueOf(6l), graph.traversal().V().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().count().next());
            assertEquals(Long.valueOf(12l), graph.traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().values().count().next());
            assertEquals(0, graph.traversal().V().values("money").sum().next());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldProcessResultGraphOriginalWithPersistNothing() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        if (computer.features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.ORIGINAL, GraphComputer.Persist.NOTHING)) {
            final ComputerResult result = computer.program(new VertexProgramK()).result(GraphComputer.ResultGraph.ORIGINAL).persist(GraphComputer.Persist.NOTHING).submit().get();
            assertEquals(Long.valueOf(6l), result.graph().traversal().V().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().count().next());
            assertEquals(Long.valueOf(12l), result.graph().traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().values().count().next());
            assertEquals(0, result.graph().traversal().V().values("money").sum().next());
            ///
            assertEquals(Long.valueOf(6l), graph.traversal().V().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().count().next());
            assertEquals(Long.valueOf(12l), graph.traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().values().count().next());
            assertEquals(0, graph.traversal().V().values("money").sum().next());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldProcessResultGraphOriginalWithPersistVertexProperties() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        if (computer.features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.ORIGINAL, GraphComputer.Persist.VERTEX_PROPERTIES)) {
            final ComputerResult result = computer.program(new VertexProgramK()).result(GraphComputer.ResultGraph.ORIGINAL).persist(GraphComputer.Persist.VERTEX_PROPERTIES).submit().get();
            assertEquals(Long.valueOf(6l), result.graph().traversal().V().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().count().next());
            assertEquals(Long.valueOf(18l), result.graph().traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().values().count().next());
            assertEquals(28l, result.graph().traversal().V().values("money").sum().next());
            ///
            assertEquals(Long.valueOf(6l), graph.traversal().V().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().count().next());
            assertEquals(Long.valueOf(18l), graph.traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().values().count().next());
            assertEquals(28l, graph.traversal().V().values("money").sum().next());
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldProcessResultGraphOriginalWithPersistEdges() throws Exception {
        final GraphComputer computer = graphProvider.getGraphComputer(graph);
        if (computer.features().supportsResultGraphPersistCombination(GraphComputer.ResultGraph.ORIGINAL, GraphComputer.Persist.EDGES)) {
            final ComputerResult result = computer.program(new VertexProgramK()).result(GraphComputer.ResultGraph.ORIGINAL).persist(GraphComputer.Persist.EDGES).submit().get();
            assertEquals(Long.valueOf(6l), result.graph().traversal().V().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().count().next());
            assertEquals(Long.valueOf(18l), result.graph().traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), result.graph().traversal().E().values().count().next());
            assertEquals(28l, result.graph().traversal().V().values("money").sum().next());
            ///
            assertEquals(Long.valueOf(6l), graph.traversal().V().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().count().next());
            assertEquals(Long.valueOf(18l), graph.traversal().V().values().count().next());
            assertEquals(Long.valueOf(6l), graph.traversal().E().values().count().next());
            assertEquals(28l, graph.traversal().V().values("money").sum().next());
        }
    }

    public static class VertexProgramK extends StaticVertexProgram {


        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            vertex.property("money", vertex.<String>value("name").length());
        }

        @Override
        public boolean terminate(final Memory memory) {
            return true;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of("money", false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.EDGES;
        }
    }

    /////////////////////////////////////////////

    @Test
    @LoadGraphWith(GRATEFUL)
    public void shouldSupportWorkerCount() throws Exception {
        int maxWorkers = graphProvider.getGraphComputer(graph).features().getMaxWorkers();
        if (maxWorkers != Integer.MAX_VALUE) {
            for (int i = maxWorkers + 1; i < maxWorkers + 10; i++) {
                try {
                    graphProvider.getGraphComputer(graph).program(new VertexProgramL()).workers(i).submit().get();
                    fail("Should throw a GraphComputer.Exceptions.computerRequiresMoreWorkersThanSupported() exception");
                } catch (final IllegalArgumentException e) {
                    assertTrue(e.getMessage().contains("computer requires more workers"));
                }
            }
        }
        if (maxWorkers > 25) maxWorkers = 25;
        for (int i = 1; i <= maxWorkers; i++) {
            ComputerResult result = graphProvider.getGraphComputer(graph).program(new VertexProgramL()).workers(i).submit().get();
            assertEquals(Integer.valueOf(i).longValue(), (long) result.memory().get("workerCount"));
        }
    }

    public static class VertexProgramL implements VertexProgram {

        boolean announced = false;

        @Override
        public void setup(final Memory memory) {
            memory.set("workerCount", 0l);
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            try {
                Thread.sleep(1);
            } catch (Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
            if (!this.announced) {
                memory.add("workerCount", 1l);
                this.announced = true;
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return true;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.singleton(MemoryComputeKey.of("workerCount", Operator.sum, true, false));
        }

        /*public void workerIterationStart(final Memory memory) {
            assertEquals(0l, (long) memory.get("workerCount"));
        }

        public void workerIterationEnd(final Memory memory) {
            assertEquals(1l, (long) memory.get("workerCount"));
        }*/

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }

        @Override
        @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
        public VertexProgramL clone() {
            return new VertexProgramL();
        }

        @Override
        public void storeState(final Configuration configuration) {
            VertexProgram.super.storeState(configuration);
        }
    }

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportMultipleScopes() throws ExecutionException, InterruptedException {
        final ComputerResult result = graphProvider.getGraphComputer(graph).program(new MultiScopeVertexProgram()).submit().get();
        assertEquals(result.graph().traversal().V().has("name", "josh").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 0L);
        assertEquals(result.graph().traversal().V().has("name", "lop").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 1L);
        assertEquals(result.graph().traversal().V().has("name", "ripple").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 1L);
        assertEquals(result.graph().traversal().V().has("name", "marko").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 2L);
    }

    public static class MultiScopeVertexProgram extends StaticVertexProgram<Long> {

        private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE);
        private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE);

        private static final String MEMORY_KEY = "count";


        @Override
        public void setup(final Memory memory) {
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of(MEMORY_KEY, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            HashSet<MessageScope> scopes = new HashSet<>();
            scopes.add(countMessageScopeIn);
            scopes.add(countMessageScopeOut);
            return scopes;
        }

        @Override
        public void execute(Vertex vertex, Messenger<Long> messenger, Memory memory) {
            switch (memory.getIteration()) {
                case 0:
                    if (vertex.value("name").equals("josh")) {
                        messenger.sendMessage(this.countMessageScopeIn, 2L);
                        messenger.sendMessage(this.countMessageScopeOut, 1L);
                    }
                    break;
                case 1:
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    vertex.property(MEMORY_KEY, edgeCount);
                    break;
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() == 1;
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }
    }

    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportMultipleScopesWithEdgeFunction() throws ExecutionException, InterruptedException {
        final ComputerResult result = graphProvider.getGraphComputer(graph).program(new MultiScopeVertexWithEdgeFunctionProgram()).submit().get();
        assertEquals(result.graph().traversal().V().has("name", "josh").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 0L);
        assertEquals(result.graph().traversal().V().has("name", "lop").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 4L);
        assertEquals(result.graph().traversal().V().has("name", "ripple").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 10L);
        assertEquals(result.graph().traversal().V().has("name", "marko").next().property(MultiScopeVertexProgram.MEMORY_KEY).value(), 20L);
    }

    public static class MultiScopeVertexWithEdgeFunctionProgram extends StaticVertexProgram<Long> {

        private final MessageScope.Local<Long> countMessageScopeIn = MessageScope.Local.of(__::inE, (m,e) -> m * Math.round(((Double) e.values("weight").next()) * 10));
        private final MessageScope.Local<Long> countMessageScopeOut = MessageScope.Local.of(__::outE, (m,e) -> m * Math.round(((Double) e.values("weight").next()) * 10));

        private static final String MEMORY_KEY = "count";


        @Override
        public void setup(final Memory memory) {
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of(MEMORY_KEY, false));
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            HashSet<MessageScope> scopes = new HashSet<>();
            scopes.add(countMessageScopeIn);
            scopes.add(countMessageScopeOut);
            return scopes;
        }

        @Override
        public void execute(Vertex vertex, Messenger<Long> messenger, Memory memory) {
            switch (memory.getIteration()) {
                case 0:
                    if (vertex.value("name").equals("josh")) {
                        messenger.sendMessage(this.countMessageScopeIn, 2L);
                        messenger.sendMessage(this.countMessageScopeOut, 1L);
                    }
                    break;
                case 1:
                    long edgeCount = IteratorUtils.reduce(messenger.receiveMessages(), 0L, (a, b) -> a + b);
                    vertex.property(MEMORY_KEY, edgeCount);
                    break;
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() == 1;
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }
    }

    /////////////////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportGraphFilter() throws Exception {
        // if the graph computer does not support graph filter, then make sure its exception handling is correct
        if (!graphProvider.getGraphComputer(graph).features().supportsGraphFilter()) {
            try {
                graphProvider.getGraphComputer(graph).vertices(__.hasLabel("software"));
                fail("Should throw an unsupported operation exception");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.graphFilterNotSupported().getMessage(), e.getMessage());
            }
            try {
                graphProvider.getGraphComputer(graph).edges(__.<Vertex>outE().limit(10));
                fail("Should throw an unsupported operation exception");
            } catch (final UnsupportedOperationException e) {
                assertEquals(GraphComputer.Exceptions.graphFilterNotSupported().getMessage(), e.getMessage());
            }
            return;
        }
        /// VERTEX PROGRAM
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("software")).program(new VertexProgramM(VertexProgramM.SOFTWARE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).program(new VertexProgramM(VertexProgramM.PEOPLE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.bothE("knows")).program(new VertexProgramM(VertexProgramM.KNOWS_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).edges(__.bothE("knows")).program(new VertexProgramM(VertexProgramM.PEOPLE_KNOWS_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).edges(__.<Vertex>bothE("knows").has("weight", P.gt(0.5f))).program(new VertexProgramM(VertexProgramM.PEOPLE_KNOWS_WELL_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.<Vertex>bothE().limit(0)).program(new VertexProgramM(VertexProgramM.VERTICES_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.<Vertex>outE().limit(1)).program(new VertexProgramM(VertexProgramM.ONE_OUT_EDGE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(outE()).program(new VertexProgramM(VertexProgramM.OUT_EDGES_ONLY)).submit().get();

        /// VERTEX PROGRAM + MAP REDUCE
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("software")).program(new VertexProgramM(VertexProgramM.SOFTWARE_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.SOFTWARE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).program(new VertexProgramM(VertexProgramM.PEOPLE_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.PEOPLE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.bothE("knows")).program(new VertexProgramM(VertexProgramM.KNOWS_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.KNOWS_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).edges(__.bothE("knows")).program(new VertexProgramM(VertexProgramM.PEOPLE_KNOWS_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.PEOPLE_KNOWS_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).edges(__.<Vertex>bothE("knows").has("weight", P.gt(0.5f))).program(new VertexProgramM(VertexProgramM.PEOPLE_KNOWS_WELL_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.PEOPLE_KNOWS_WELL_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.<Vertex>bothE().limit(0)).program(new VertexProgramM(VertexProgramM.VERTICES_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.VERTICES_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.<Vertex>outE().limit(1)).program(new VertexProgramM(VertexProgramM.ONE_OUT_EDGE_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.ONE_OUT_EDGE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(outE()).program(new VertexProgramM(VertexProgramM.OUT_EDGES_ONLY)).mapReduce(new MapReduceJ(VertexProgramM.OUT_EDGES_ONLY)).submit().get();

        /// MAP REDUCE ONLY
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("software")).mapReduce(new MapReduceJ(VertexProgramM.SOFTWARE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).mapReduce(new MapReduceJ(VertexProgramM.PEOPLE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.bothE("knows")).mapReduce(new MapReduceJ(VertexProgramM.KNOWS_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).edges(__.bothE("knows")).mapReduce(new MapReduceJ(VertexProgramM.PEOPLE_KNOWS_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).vertices(__.hasLabel("person")).edges(__.<Vertex>bothE("knows").has("weight", P.gt(0.5f))).mapReduce(new MapReduceJ(VertexProgramM.PEOPLE_KNOWS_WELL_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.<Vertex>bothE().limit(0)).mapReduce(new MapReduceJ(VertexProgramM.VERTICES_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(__.<Vertex>outE().limit(1)).mapReduce(new MapReduceJ(VertexProgramM.ONE_OUT_EDGE_ONLY)).submit().get();
        graphProvider.getGraphComputer(graph).edges(outE()).mapReduce(new MapReduceJ(VertexProgramM.OUT_EDGES_ONLY)).submit().get();

        // EXCEPTION HANDLING
        try {
            graphProvider.getGraphComputer(graph).vertices(__.out());
            fail();
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), GraphComputer.Exceptions.vertexFilterAccessesIncidentEdges(__.out()).getMessage());
        }
        try {
            graphProvider.getGraphComputer(graph).edges(__.<Vertex>out().outE());
            fail();
        } catch (final IllegalArgumentException e) {
            assertEquals(e.getMessage(), GraphComputer.Exceptions.edgeFilterAccessesAdjacentVertices(__.<Vertex>out().outE()).getMessage());
        }
    }

    public static class VertexProgramM implements VertexProgram {

        public static final String SOFTWARE_ONLY = "softwareOnly";
        public static final String PEOPLE_ONLY = "peopleOnly";
        public static final String KNOWS_ONLY = "knowsOnly";
        public static final String PEOPLE_KNOWS_ONLY = "peopleKnowsOnly";
        public static final String PEOPLE_KNOWS_WELL_ONLY = "peopleKnowsWellOnly";
        public static final String VERTICES_ONLY = "verticesOnly";
        public static final String ONE_OUT_EDGE_ONLY = "oneOutEdgeOnly";
        public static final String OUT_EDGES_ONLY = "outEdgesOnly";

        private String state;

        public VertexProgramM() {

        }

        public VertexProgramM(final String state) {
            this.state = state;
        }

        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            switch (this.state) {
                case SOFTWARE_ONLY: {
                    assertEquals("software", vertex.label());
                    assertFalse(vertex.edges(Direction.OUT).hasNext());
                    assertTrue(vertex.edges(Direction.IN).hasNext());
                    assertTrue(vertex.edges(Direction.IN, "created").hasNext());
                    assertFalse(vertex.edges(Direction.IN, "knows").hasNext());
                    break;
                }
                case PEOPLE_ONLY: {
                    assertEquals("person", vertex.label());
                    assertFalse(vertex.edges(Direction.IN, "created").hasNext());
                    assertTrue(IteratorUtils.count(vertex.edges(Direction.BOTH)) > 0);
                    break;
                }
                case KNOWS_ONLY: {
                    assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH, "created")));
                    if (vertex.value("name").equals("marko"))
                        assertEquals(2, IteratorUtils.count(vertex.edges(Direction.BOTH, "knows")));
                    else if (vertex.value("name").equals("vadas"))
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.IN, "knows")));
                    else if (vertex.value("name").equals("josh"))
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.IN, "knows")));
                    else {
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH, "knows")));
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    }
                    break;
                }
                case PEOPLE_KNOWS_ONLY: {
                    assertEquals("person", vertex.label());
                    assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH, "created")));
                    if (vertex.value("name").equals("marko"))
                        assertEquals(2, IteratorUtils.count(vertex.edges(Direction.BOTH, "knows")));
                    else if (vertex.value("name").equals("vadas"))
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.IN, "knows")));
                    else if (vertex.value("name").equals("josh"))
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.IN, "knows")));
                    else {
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH, "knows")));
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    }
                    break;
                }
                case PEOPLE_KNOWS_WELL_ONLY: {
                    assertEquals("person", vertex.label());
                    assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH, "created")));
                    if (vertex.value("name").equals("marko")) {
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.BOTH, "knows")));
                        assertEquals(1.0, vertex.edges(Direction.OUT, "knows").next().value("weight"), 0.001);
                    } else if (vertex.value("name").equals("vadas"))
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.IN, "knows")));
                    else if (vertex.value("name").equals("josh")) {
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.IN, "knows")));
                        assertEquals(1.0, vertex.edges(Direction.IN, "knows").next().value("weight"), 0.001);
                    } else {
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH, "knows")));
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    }
                    break;
                }
                case VERTICES_ONLY: {
                    assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    break;
                }
                case ONE_OUT_EDGE_ONLY: {
                    if (vertex.label().equals("software") || vertex.value("name").equals("vadas"))
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    else {
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.OUT)));
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.IN)));
                        assertEquals(1, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    }
                    break;
                }
                case OUT_EDGES_ONLY: {
                    if (vertex.label().equals("software") || vertex.value("name").equals("vadas"))
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    else {
                        assertTrue(IteratorUtils.count(vertex.edges(Direction.OUT)) > 0);
                        assertEquals(0, IteratorUtils.count(vertex.edges(Direction.IN)));
                        assertEquals(IteratorUtils.count(vertex.edges(Direction.OUT)), IteratorUtils.count(vertex.edges(Direction.BOTH)));
                    }
                    break;
                }
                default:
                    throw new IllegalStateException("This is an illegal state for this test case: " + this.state);
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return true;
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.NOTHING;
        }

        @Override
        @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
        public VertexProgramM clone() {
            return new VertexProgramM(this.state);
        }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            this.state = configuration.getString("state");
        }

        @Override
        public void storeState(final Configuration configuration) {
            configuration.setProperty("state", this.state);
            VertexProgram.super.storeState(configuration);
        }

    }

    private static class MapReduceJ implements MapReduce<MapReduce.NullObject, Integer, MapReduce.NullObject, Integer, Integer> {

        private String state;

        public MapReduceJ() {
        }

        public MapReduceJ(final String state) {
            this.state = state;
        }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            this.state = configuration.getString("state");
        }

        @Override
        public void storeState(final Configuration configuration) {
            configuration.setProperty("state", this.state);
            MapReduce.super.storeState(configuration);
        }

        @Override
        @SuppressWarnings("CloneDoesntCallSuperClone,CloneDoesntDeclareCloneNotSupportedException")
        public MapReduceJ clone() {
            return new MapReduceJ(this.state);
        }

        @Override
        public boolean doStage(final Stage stage) {
            return true;
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<NullObject, Integer> emitter) {
            emitter.emit(1);
            switch (this.state) {
                case VertexProgramM.SOFTWARE_ONLY: {
                    assertEquals("software", vertex.label());
                    break;
                }
                case VertexProgramM.PEOPLE_ONLY: {
                    assertEquals("person", vertex.label());
                    break;
                }
                case VertexProgramM.KNOWS_ONLY: {
                    assertTrue(vertex.label().equals("person") || vertex.label().equals("software"));
                    break;
                }
                case VertexProgramM.PEOPLE_KNOWS_ONLY: {
                    assertEquals("person", vertex.label());
                    break;
                }
                case VertexProgramM.PEOPLE_KNOWS_WELL_ONLY: {
                    assertEquals("person", vertex.label());
                    break;
                }
                case VertexProgramM.VERTICES_ONLY: {
                    assertTrue(vertex.label().equals("person") || vertex.label().equals("software"));
                    break;
                }
                case VertexProgramM.ONE_OUT_EDGE_ONLY: {
                    assertTrue(vertex.label().equals("person") || vertex.label().equals("software"));
                    break;
                }
                case VertexProgramM.OUT_EDGES_ONLY: {
                    assertTrue(vertex.label().equals("person") || vertex.label().equals("software"));
                    break;
                }
                default:
                    throw new IllegalStateException("This is an illegal state for this test case: " + this.state);
            }
        }

        @Override
        public void combine(final NullObject key, final Iterator<Integer> values, final ReduceEmitter<NullObject, Integer> emitter) {
            this.reduce(key, values, emitter);
        }

        @Override
        public void reduce(final NullObject key, final Iterator<Integer> values, final ReduceEmitter<NullObject, Integer> emitter) {
            int count = 0;
            while (values.hasNext()) {
                count = count + values.next();
            }
            emitter.emit(count);
        }

        @Override
        public Integer generateFinalResult(final Iterator<KeyValue<NullObject, Integer>> keyValues) {
            int counter = keyValues.next().getValue();
            assertFalse(keyValues.hasNext());

            switch (this.state) {
                case VertexProgramM.SOFTWARE_ONLY: {
                    assertEquals(2, counter);
                    break;
                }
                case VertexProgramM.PEOPLE_ONLY: {
                    assertEquals(4, counter);
                    break;
                }
                case VertexProgramM.KNOWS_ONLY: {
                    assertEquals(6, counter);
                    break;
                }
                case VertexProgramM.PEOPLE_KNOWS_ONLY: {
                    assertEquals(4, counter);
                    break;
                }
                case VertexProgramM.PEOPLE_KNOWS_WELL_ONLY: {
                    assertEquals(4, counter);
                    break;
                }
                case VertexProgramM.VERTICES_ONLY: {
                    assertEquals(6, counter);
                    break;
                }
                case VertexProgramM.ONE_OUT_EDGE_ONLY: {
                    assertEquals(6, counter);
                    break;
                }
                case VertexProgramM.OUT_EDGES_ONLY: {
                    assertEquals(6, counter);
                    break;
                }
                default:
                    throw new IllegalStateException("This is an illegal state for this test case: " + this.state);
            }
            return counter;
        }

        @Override
        public String getMemoryKey() {
            return "a";
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportJobChaining() throws Exception {
        final ComputerResult result1 = graphProvider.getGraphComputer(graph)
                .program(PageRankVertexProgram.build().iterations(5).create(graph)).persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW).submit().get();
        final Graph graph1 = result1.graph();
        final Memory memory1 = result1.memory();
        assertEquals(5, memory1.getIteration());
        assertEquals(6, graph1.traversal().V().count().next().intValue());
        assertEquals(6, graph1.traversal().E().count().next().intValue());
        assertEquals(6, graph1.traversal().V().values(PageRankVertexProgram.PAGE_RANK).count().next().intValue());
        assertEquals(18, graph1.traversal().V().values().count().next().intValue());
        //
        final ComputerResult result2 = graph1.compute(graphProvider.getGraphComputer(graph1).getClass())
                .program(PeerPressureVertexProgram.build().maxIterations(4).create(graph1)).persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW).submit().get();
        final Graph graph2 = result2.graph();
        final Memory memory2 = result2.memory();
        assertTrue(memory2.getIteration() <= 4);
        assertEquals(6, graph2.traversal().V().count().next().intValue());
        assertEquals(6, graph2.traversal().E().count().next().intValue());
        assertEquals(6, graph2.traversal().V().values(PeerPressureVertexProgram.CLUSTER).count().next().intValue());
        assertEquals(6, graph2.traversal().V().values(PageRankVertexProgram.PAGE_RANK).count().next().intValue());
        assertEquals(24, graph2.traversal().V().values().count().next().intValue());
        //
        final ComputerResult result3 = graph2.compute(graphProvider.getGraphComputer(graph2).getClass())
                .program(TraversalVertexProgram.build().traversal(g.V().groupCount("m").by(__.values(PageRankVertexProgram.PAGE_RANK).count()).label().asAdmin()).create(graph2)).persist(GraphComputer.Persist.EDGES).result(GraphComputer.ResultGraph.NEW).submit().get();
        final Graph graph3 = result3.graph();
        final Memory memory3 = result3.memory();
        assertTrue(memory3.keys().contains("m"));
        assertTrue(memory3.keys().contains(TraversalVertexProgram.HALTED_TRAVERSERS));
        assertEquals(1, memory3.<Map<Long, Long>>get("m").size());
        assertEquals(6, memory3.<Map<Long, Long>>get("m").get(1l).intValue());
        List<Traverser<String>> traversers = IteratorUtils.list(memory3.<TraverserSet>get(TraversalVertexProgram.HALTED_TRAVERSERS).iterator());
        assertEquals(6l, traversers.stream().map(Traverser::bulk).reduce((a, b) -> a + b).get().longValue());
        assertEquals(4l, traversers.stream().filter(s -> s.get().equals("person")).map(Traverser::bulk).reduce((a, b) -> a + b).get().longValue());
        assertEquals(2l, traversers.stream().filter(s -> s.get().equals("software")).map(Traverser::bulk).reduce((a, b) -> a + b).get().longValue());
        assertEquals(6, graph3.traversal().V().count().next().intValue());
        assertEquals(6, graph3.traversal().E().count().next().intValue());
        assertEquals(0, graph3.traversal().V().values(TraversalVertexProgram.HALTED_TRAVERSERS).count().next().intValue());
        assertEquals(6, graph3.traversal().V().values(PeerPressureVertexProgram.CLUSTER).count().next().intValue());
        assertEquals(6, graph3.traversal().V().values(PageRankVertexProgram.PAGE_RANK).count().next().intValue());
        assertEquals(24, graph3.traversal().V().values().count().next().intValue()); // no halted traversers

        // TODO: add a test the shows DAG behavior -- splitting another TraversalVertexProgram off of the PeerPressureVertexProgram job.
    }

    ///////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportPreExistingComputeKeys() throws Exception {
        final ComputerResult result = graphProvider.getGraphComputer(graph).program(new VertexProgramN()).submit().get();
        result.graph().vertices().forEachRemaining(vertex -> {
            if (vertex.label().equals("person")) {
                if (vertex.value("name").equals("marko"))
                    assertEquals(32, vertex.<Integer>value("age").intValue());
                else if (vertex.value("name").equals("peter"))
                    assertEquals(38, vertex.<Integer>value("age").intValue());
                else if (vertex.value("name").equals("vadas"))
                    assertEquals(30, vertex.<Integer>value("age").intValue());
                else if (vertex.value("name").equals("josh"))
                    assertEquals(35, vertex.<Integer>value("age").intValue());
                else
                    throw new IllegalStateException("This vertex should not have been accessed: " + vertex);
            }
        });
    }

    private static class VertexProgramN extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {

        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            if (vertex.label().equals("person"))
                vertex.property(VertexProperty.Cardinality.single, "age", vertex.<Integer>value("age") + 1);
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() > 1;
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return Collections.singleton(VertexComputeKey.of("age", false));
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }
    }

    ///////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportTransientKeys() throws Exception {
        final ComputerResult result = graphProvider.getGraphComputer(graph).program(new VertexProgramO()).mapReduce(new MapReduceK()).submit().get();
        result.graph().vertices().forEachRemaining(vertex -> {
            assertFalse(vertex.property("v1").isPresent());
            assertFalse(vertex.property("v2").isPresent());
            assertTrue(vertex.property("v3").isPresent());
            assertEquals("shouldExist", vertex.value("v3"));
            assertTrue(vertex.property("name").isPresent());
            if (vertex.label().equals("software"))
                assertTrue(vertex.property("lang").isPresent());
            else
                assertTrue(vertex.property("age").isPresent());
            assertEquals(3, IteratorUtils.count(vertex.properties()));
            assertEquals(0, IteratorUtils.count(vertex.properties("v1")));
            assertEquals(0, IteratorUtils.count(vertex.properties("v2")));
            assertEquals(1, IteratorUtils.count(vertex.properties("v3")));
            assertEquals(1, IteratorUtils.count(vertex.properties("name")));
        });
        assertEquals(6l, result.graph().traversal().V().properties("name").count().next().longValue());
        assertEquals(0l, result.graph().traversal().V().properties("v1").count().next().longValue());
        assertEquals(0l, result.graph().traversal().V().properties("v2").count().next().longValue());
        assertEquals(6l, result.graph().traversal().V().properties("v3").count().next().longValue());
        assertEquals(6l, result.graph().traversal().V().<String>values("name").dedup().count().next().longValue());
        assertEquals(1l, result.graph().traversal().V().<String>values("v3").dedup().count().next().longValue());
        assertEquals("shouldExist", result.graph().traversal().V().<String>values("v3").dedup().next());
        ///
        assertFalse(result.memory().exists("m1"));
        assertFalse(result.memory().exists("m2"));
        assertTrue(result.memory().exists("m3"));
        assertEquals(24l, result.memory().<Long>get("m3").longValue());
        assertEquals(2, result.memory().keys().size());  // mapReduceK
    }

    private static class VertexProgramO extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {
            assertFalse(memory.exists("m1"));
            assertFalse(memory.exists("m2"));
            assertFalse(memory.exists("m3"));
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            if (memory.isInitialIteration()) {
                assertFalse(vertex.property("v1").isPresent());
                assertFalse(vertex.property("v2").isPresent());
                assertFalse(vertex.property("v3").isPresent());
                vertex.property("v1", "shouldNotExist");
                vertex.property("v2", "shouldNotExist");
                vertex.property("v3", "shouldExist");
                assertTrue(vertex.property("v1").isPresent());
                assertTrue(vertex.property("v2").isPresent());
                assertTrue(vertex.property("v3").isPresent());
                assertEquals("shouldNotExist", vertex.value("v1"));
                assertEquals("shouldNotExist", vertex.value("v2"));
                assertEquals("shouldExist", vertex.value("v3"));
                //
                assertFalse(memory.exists("m1"));
                assertFalse(memory.exists("m2"));
                assertFalse(memory.exists("m3"));
                memory.add("m1", false);
                memory.add("m2", true);
                memory.add("m3", 2l);
                // should still not exist as this pulls from the master memory
                assertFalse(memory.exists("m1"));
                assertFalse(memory.exists("m2"));
                assertFalse(memory.exists("m3"));

            } else {
                assertTrue(vertex.property("v1").isPresent());
                assertTrue(vertex.property("v2").isPresent());
                assertTrue(vertex.property("v3").isPresent());
                assertEquals("shouldNotExist", vertex.value("v1"));
                assertEquals("shouldNotExist", vertex.value("v2"));
                assertEquals("shouldExist", vertex.value("v3"));
                //
                assertTrue(memory.exists("m1"));
                assertTrue(memory.exists("m2"));
                assertTrue(memory.exists("m3"));
                assertFalse(memory.get("m1"));
                assertTrue(memory.get("m2"));
                assertEquals(12l, memory.<Long>get("m3").longValue());
                memory.add("m1", true);
                memory.add("m2", true);
                memory.add("m3", 2l);
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            assertTrue(memory.exists("m1"));
            assertTrue(memory.exists("m2"));
            assertTrue(memory.exists("m3"));
            if (memory.isInitialIteration()) {
                assertFalse(memory.get("m1"));
                assertTrue(memory.get("m2"));
                assertEquals(12l, memory.<Long>get("m3").longValue());
                return false;
            } else {
                assertTrue(memory.get("m1"));
                assertTrue(memory.get("m2"));
                assertEquals(24l, memory.<Long>get("m3").longValue());
                return true;
            }
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(
                    MemoryComputeKey.of("m1", Operator.or, true, true),
                    MemoryComputeKey.of("m2", Operator.and, true, true),
                    MemoryComputeKey.of("m3", Operator.sum, true, false)));
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return new HashSet<>(Arrays.asList(
                    VertexComputeKey.of("v1", true),
                    VertexComputeKey.of("v2", true),
                    VertexComputeKey.of("v3", false)));
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }
    }

    public static class MapReduceK extends StaticMapReduce {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter emitter) {
            assertFalse(vertex.property("v1").isPresent());
            assertFalse(vertex.property("v2").isPresent());
            assertTrue(vertex.property("v3").isPresent());
            assertTrue(vertex.property("name").isPresent());
            assertEquals(3, IteratorUtils.count(vertex.properties()));
            assertEquals(3, IteratorUtils.count(vertex.values()));
        }

        @Override
        public String getMemoryKey() {
            return "mapReduceK";
        }

        @Override
        public Object generateFinalResult(final Iterator keyValues) {
            return "anObject";
        }
    }

    ///////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSupportBroadcastKeys() throws Exception {
        final ComputerResult result = graphProvider.getGraphComputer(graph).program(new VertexProgramP()).submit().get();
        assertTrue(result.memory().exists("m1"));
        assertFalse(result.memory().exists("m2"));
        assertFalse(result.memory().exists("m3"));
        assertTrue(result.memory().exists("m4"));
        assertTrue(result.memory().get("m1"));
        assertEquals(-18, result.memory().<Integer>get("m4").intValue());
        assertEquals(2, result.memory().keys().size());
    }

    private static class VertexProgramP extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {
            assertFalse(memory.exists("m1"));  // or
            assertFalse(memory.exists("m2"));  // and
            assertFalse(memory.exists("m3"));  // long
            assertFalse(memory.exists("m4"));  // int
            memory.set("m1", false);
            memory.set("m2", true);
            memory.set("m3", 0l);
            memory.set("m4", 0);
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            if (memory.isInitialIteration()) {
                assertFalse(memory.exists("m1"));
                assertTrue(memory.exists("m2"));
                assertTrue(memory.get("m2"));
                assertFalse(memory.exists("m3"));
                assertTrue(memory.exists("m4"));
                assertEquals(0, memory.<Integer>get("m4").intValue());
                memory.add("m1", false);
                memory.add("m2", true);
                memory.add("m3", 1l);
                memory.add("m4", -1);
            } else {
                assertFalse(memory.exists("m1")); // no broadcast
                assertTrue(memory.exists("m2"));
                assertFalse(memory.exists("m3")); // no broadcast
                assertTrue(memory.exists("m4"));
                try {
                    assertFalse(memory.get("m1"));
                    fail();
                } catch (final Exception e) {
                    validateException(Memory.Exceptions.memoryDoesNotExist("m1"), e);
                }
                assertTrue(memory.get("m2"));
                try {
                    assertEquals(6l, memory.<Long>get("m3").longValue());
                    fail();
                } catch (final Exception e) {
                    validateException(Memory.Exceptions.memoryDoesNotExist("m3"), e);
                }
                assertEquals(-6l, memory.<Integer>get("m4").intValue());
                ///
                memory.add("m1", true);
                memory.add("m2", true);
                memory.add("m3", 2l);
                memory.add("m4", -2);
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            assertTrue(memory.exists("m1"));
            assertTrue(memory.exists("m2"));
            assertTrue(memory.exists("m3"));
            assertTrue(memory.exists("m4"));
            if (memory.isInitialIteration()) {
                assertFalse(memory.get("m1"));
                assertTrue(memory.get("m2"));
                assertEquals(6l, memory.<Long>get("m3").longValue());
                assertEquals(-6, memory.<Integer>get("m4").intValue());
                return false;
            } else {
                assertTrue(memory.get("m1"));
                assertTrue(memory.get("m2"));
                assertEquals(18l, memory.<Long>get("m3").longValue());
                assertEquals(-18, memory.<Integer>get("m4").intValue());
                return true;
            }
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(
                    MemoryComputeKey.of("m1", Operator.or, false, false),
                    MemoryComputeKey.of("m2", Operator.and, true, true),
                    MemoryComputeKey.of("m3", Operator.sum, false, true),
                    MemoryComputeKey.of("m4", Operator.sum, true, false)));
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }
    }

    ///////////////////////////////////

    @Test
    @LoadGraphWith(MODERN)
    public void shouldSucceedWithProperTraverserRequirements() throws Exception {

        final VertexProgramQ vp = VertexProgramQ.build().property("pl").create();
        final Map<String, List<Integer>> expected = new HashMap<>();
        expected.put("vadas", Collections.singletonList(2));
        expected.put("lop", Arrays.asList(2, 2, 2, 3));
        expected.put("josh", Collections.singletonList(2));
        expected.put("ripple", Arrays.asList(2, 3));

        try {
            g.V().repeat(__.out()).emit().program(vp).dedup()
                    .valueMap("name", "pl").forEachRemaining((Map<String, Object> map) -> {

                final String name = (String) ((List) map.get("name")).get(0);
                final List<Integer> pathLengths = (List<Integer>) map.get("pl");
                assertTrue(expected.containsKey(name));
                final List<Integer> expectedPathLengths = expected.remove(name);
                assertTrue(expectedPathLengths.containsAll(pathLengths));
                assertTrue(pathLengths.containsAll(expectedPathLengths));
            });

            assertTrue(expected.isEmpty());
        } catch (VerificationException ex) {
            assumeNoException(ex);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFailWithImproperTraverserRequirements() throws Exception {
        final VertexProgramQ vp = VertexProgramQ.build().property("pl").useTraverserRequirements(false).create();
        try {
            g.V().repeat(__.out()).emit().program(vp).dedup()
                    .forEachRemaining((Vertex v) -> assertFalse(v.property("pl").isPresent()));
        } catch (VerificationException ex) {
            assumeNoException(ex);
        }
    }

    private static class VertexProgramQ implements VertexProgram<Object> {

        private static final String VERTEX_PROGRAM_Q_CFG_PREFIX = "gremlin.vertexProgramQ";
        private static final String PROPERTY_CFG_KEY = VERTEX_PROGRAM_Q_CFG_PREFIX + ".property";
        private static final String LENGTHS_KEY = VERTEX_PROGRAM_Q_CFG_PREFIX + ".lengths";
        private static final String USE_TRAVERSER_REQUIREMENTS_CFG_KEY = VERTEX_PROGRAM_Q_CFG_PREFIX + ".useTraverserRequirements";

        private final static Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = Collections.singleton(
                MemoryComputeKey.of(LENGTHS_KEY, Operator.addAll, true, true)
        );

        private final Set<VertexComputeKey> elementComputeKeys;
        private Configuration configuration;
        private String propertyKey;
        private Set<TraverserRequirement> traverserRequirements;

        private VertexProgramQ() {
            elementComputeKeys = new HashSet<>();
        }

        public static Builder build() {
            return new Builder();
        }

        static class Builder extends AbstractVertexProgramBuilder<Builder> {

            private Builder() {
                super(VertexProgramQ.class);
            }

            @SuppressWarnings("unchecked")
            @Override
            public VertexProgramQ create(final Graph graph) {
                if (graph != null) {
                    ConfigurationUtils.append(graph.configuration().subset(VERTEX_PROGRAM_Q_CFG_PREFIX), configuration);
                }
                return (VertexProgramQ) VertexProgram.createVertexProgram(graph, configuration);
            }

            public VertexProgramQ create() {
                return create(null);
            }

            public Builder property(final String name) {
                configuration.setProperty(PROPERTY_CFG_KEY, name);
                return this;
            }

            /**
             * This is only configurable for the purpose of testing. In a real-world VP this would be a bad pattern.
             */
            public Builder useTraverserRequirements(final boolean value) {
                configuration.setProperty(USE_TRAVERSER_REQUIREMENTS_CFG_KEY, value);
                return this;
            }
        }

        @Override
        public void storeState(final Configuration config) {
            VertexProgram.super.storeState(config);
            if (configuration != null) {
                ConfigurationUtils.copy(configuration, config);
            }
        }


        @Override
        public void loadState(final Graph graph, final Configuration config) {
            configuration = new BaseConfiguration();
            if (config != null) {
                ConfigurationUtils.copy(config, configuration);
            }
            propertyKey = configuration.getString(PROPERTY_CFG_KEY);
            traverserRequirements = configuration.getBoolean(USE_TRAVERSER_REQUIREMENTS_CFG_KEY, true)
                    ? Collections.singleton(TraverserRequirement.PATH) : Collections.emptySet();
            elementComputeKeys.add(VertexComputeKey.of(propertyKey, false));
        }

        @Override
        public void setup(final Memory memory) {
        }

        @Override
        public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
            if (memory.isInitialIteration()) {
                final Property<TraverserSet> haltedTraversers = vertex.property(TraversalVertexProgram.HALTED_TRAVERSERS);
                if (!haltedTraversers.isPresent()) return;
                final Iterator iterator = haltedTraversers.value().iterator();
                if (iterator.hasNext()) {
                    while (iterator.hasNext()) {
                        final Traverser t = (Traverser) iterator.next();
                        if (!(t.path() instanceof EmptyPath)) {
                            final int pathLength = t.path().size();
                            final List<Pair<Vertex, Integer>> memoryValue = new LinkedList<>();
                            memoryValue.add(Pair.with(vertex, pathLength));
                            memory.add(LENGTHS_KEY, memoryValue);
                        }
                    }
                }
            } else {
                if (memory.exists(LENGTHS_KEY)) {
                    final List<Pair<Vertex, Integer>> lengths = memory.get(LENGTHS_KEY);
                    for (final Pair<Vertex, Integer> pair : lengths) {
                        if (pair.getValue0().equals(vertex)) {
                            vertex.property(VertexProperty.Cardinality.list, propertyKey, pair.getValue1());
                        }
                    }
                }
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return !memory.isInitialIteration();
        }

        @Override
        public Set<MessageScope> getMessageScopes(final Memory memory) {
            return Collections.emptySet();
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return elementComputeKeys;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return MEMORY_COMPUTE_KEYS;
        }

        @SuppressWarnings({"CloneDoesntDeclareCloneNotSupportedException", "CloneDoesntCallSuperClone"})
        @Override
        public VertexProgram<Object> clone() {
            return this;
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public Set<TraverserRequirement> getTraverserRequirements() {
            return this.traverserRequirements;
        }

        @Override
        public Features getFeatures() {
            return new Features() {
                @Override
                public boolean requiresVertexPropertyAddition() {
                    return true;
                }
            };
        }
    }

    ///////////////////////////////////

    @Test
    public void testMessagePassingIn() throws Exception {
        runTest(Direction.BOTH).forEachRemaining(v -> {
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains(VertexProgramR.PROPERTY_IN));
            assertTrue(v.keys().contains(VertexProgramR.PROPERTY_OUT));
            assertEquals(1, IteratorUtils.count(v.values(VertexProgramR.PROPERTY_IN)));
            assertEquals(1, IteratorUtils.count(v.values(VertexProgramR.PROPERTY_OUT)));
            final String in = v.value(VertexProgramR.PROPERTY_IN);
            if (in.equals("a"))
                assertEquals("ab", v.value(VertexProgramR.PROPERTY_OUT).toString());
            else if (in.equals("b"))
                assertEquals("", v.value(VertexProgramR.PROPERTY_OUT).toString());
            else
                throw new IllegalStateException("This vertex should not exist: " + VertexProgramR.PROPERTY_IN
                        + "=" + String.valueOf(in));
        });
    }

    @Test
    public void testMessagePassingOut() throws Exception {
        runTest(Direction.OUT).forEachRemaining(v -> {
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains(VertexProgramR.PROPERTY_IN));
            assertTrue(v.keys().contains(VertexProgramR.PROPERTY_OUT));
            assertEquals(1, IteratorUtils.count(v.values(VertexProgramR.PROPERTY_IN)));
            assertEquals(1, IteratorUtils.count(v.values(VertexProgramR.PROPERTY_OUT)));
            final String in = v.value(VertexProgramR.PROPERTY_IN);
            if (in.equals("a"))
                assertEquals("a", v.value(VertexProgramR.PROPERTY_OUT).toString());
            else if (in.equals("b"))
                assertEquals("a", v.value(VertexProgramR.PROPERTY_OUT).toString());
            else
                throw new IllegalStateException("This vertex should not exist: " + VertexProgramR.PROPERTY_IN
                        + "=" + String.valueOf(in));
        });
    }

    @Test
    public void testMessagePassingBoth() throws Exception {
        runTest(Direction.BOTH).forEachRemaining(v -> {
            assertEquals(2, v.keys().size());
            assertTrue(v.keys().contains(VertexProgramR.PROPERTY_IN));
            assertTrue(v.keys().contains(VertexProgramR.PROPERTY_OUT));
            assertEquals(1, IteratorUtils.count(v.values(VertexProgramR.PROPERTY_IN)));
            assertEquals(1, IteratorUtils.count(v.values(VertexProgramR.PROPERTY_OUT)));
            final String in = v.value(VertexProgramR.PROPERTY_IN);
            if (in.equals("a"))
                assertEquals("aab", v.value(VertexProgramR.PROPERTY_OUT).toString());
            else if (in.equals("b"))
                assertEquals("a", v.value(VertexProgramR.PROPERTY_OUT).toString());
            else
                throw new IllegalStateException("This vertex should not exist: " + VertexProgramR.PROPERTY_IN
                        + "=" + String.valueOf(in));
        });
    }

    private GraphTraversal<Vertex, Vertex> runTest(Direction direction) throws Exception {
        final Vertex a = graph.addVertex(VertexProgramR.PROPERTY_IN, "a");
        final Vertex b = graph.addVertex(VertexProgramR.PROPERTY_IN, "b");
        a.addEdge("edge", b);
        a.addEdge("edge", a);
        final VertexProgramR svp = VertexProgramR.build().direction(direction).create();
        final ComputerResult result = graphProvider.getGraphComputer(graph).program(svp).submit().get();
        return result.graph().traversal().V();
    }

    private static class VertexProgramR implements VertexProgram<String> {
        private static final String SIMPLE_VERTEX_PROGRAM_CFG_PREFIX = "gremlin.simpleVertexProgram";
        private static final String PROPERTY_OUT = "propertyout";
        private static final String PROPERTY_IN = "propertyin";
        private static final String DIRECTION_CFG_KEY = SIMPLE_VERTEX_PROGRAM_CFG_PREFIX + ".direction";

        private final MessageScope.Local<String> inMessageScope = MessageScope.Local.of(__::inE);
        private final MessageScope.Local<String> outMessageScope = MessageScope.Local.of(__::outE);
        private final MessageScope.Local<String> bothMessageScope = MessageScope.Local.of(__::bothE);
        private MessageScope.Local<String> messageScope;
        private final Set<VertexComputeKey> vertexComputeKeys = new HashSet<>(Arrays.asList(
                VertexComputeKey.of(VertexProgramR.PROPERTY_OUT, false),
                VertexComputeKey.of(VertexProgramR.PROPERTY_IN, false)));

        /**
         * Clones this vertex program.
         *
         * @return a clone of this vertex program
         */
        public VertexProgramR clone() { return this; }

        @Override
        public void loadState(final Graph graph, final Configuration configuration) {
            Direction direction = Direction.valueOf(configuration.getString(DIRECTION_CFG_KEY));
            switch (direction) {
                case IN:
                    this.messageScope = this.inMessageScope;
                    break;
                case OUT:
                    this.messageScope = this.outMessageScope;
                    break;
                case BOTH:
                    this.messageScope = this.bothMessageScope;
                    break;
                default:
                    throw new IllegalStateException("Should not reach this point!");
            }
        }

        @Override
        public void setup(Memory memory) {
        }

        @Override
        public void execute(Vertex vertex, Messenger<String> messenger, Memory memory) {
            if (memory.isInitialIteration()) {
                messenger.sendMessage(this.messageScope, vertex.value(PROPERTY_IN).toString());
            } else {
                char[] composite = IteratorUtils.reduce(messenger.receiveMessages(), "", (a, b) -> a + b).toCharArray();
                Arrays.sort(composite);
                vertex.property(PROPERTY_OUT, new String(composite));
            }
        }

        @Override
        public boolean terminate(Memory memory) {
            return !memory.isInitialIteration();
        }

        @Override
        public Set<MessageScope> getMessageScopes(Memory memory) {
            return Collections.singleton(this.messageScope);
        }

        @Override
        public GraphComputer.ResultGraph getPreferredResultGraph() {
            return GraphComputer.ResultGraph.NEW;
        }

        @Override
        public GraphComputer.Persist getPreferredPersist() {
            return GraphComputer.Persist.VERTEX_PROPERTIES;
        }

        @Override
        public Set<VertexComputeKey> getVertexComputeKeys() {
            return this.vertexComputeKeys;
        }

        @Override
        public Set<MemoryComputeKey> getMemoryComputeKeys() {
            return Collections.emptySet();
        }

        public static Builder build() {
            return new Builder();
        }

        static class Builder extends AbstractVertexProgramBuilder<Builder> {

            private Builder() {
                super(VertexProgramR.class);
            }

            @SuppressWarnings("unchecked")
            @Override
            public VertexProgramR create(final Graph graph) {
                if (graph != null) {
                    ConfigurationUtils.append(graph.configuration().subset(SIMPLE_VERTEX_PROGRAM_CFG_PREFIX), configuration);
                }
                return (VertexProgramR) VertexProgram.createVertexProgram(graph, configuration);
            }

            public VertexProgramR create() {
                return create(null);
            }

            public Builder direction(final Direction direction) {
                configuration.setProperty(DIRECTION_CFG_KEY, direction.toString());
                return this;
            }
        }
    }
}
