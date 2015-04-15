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

import org.apache.tinkerpop.gremlin.ExceptionCoverage;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.StreamFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Future;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@ExceptionCoverage(exceptionClass = GraphComputer.Exceptions.class, methods = {
        "providedKeyIsNotAMemoryComputeKey",
        "computerHasNoVertexProgramNorMapReducers",
        "computerHasAlreadyBeenSubmittedAVertexProgram",
        "providedKeyIsNotAnElementComputeKey",
        "isolationNotSupported",
        "incidentAndAdjacentElementsCanNotBeAccessedInMapReduce",
        "resultGraphPersistCombinationNotSupported" // TODO: NOT TRUE!
})
@ExceptionCoverage(exceptionClass = Graph.Exceptions.class, methods = {
        "graphDoesNotSupportProvidedGraphComputer",
        "onlyOneOrNoGraphComputerClass"
})
@SuppressWarnings("ThrowableResultOfMethodCallIgnored")
@UseEngine(TraversalEngine.Type.COMPUTER)
public class GraphComputerTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveStandardStringRepresentation() {
        final GraphComputer computer = graph.compute(graphComputerClass.get());
        assertEquals(StringFactory.graphComputerString(computer), computer.toString());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowWithNoVertexProgramNorMapReducers() throws Exception {
        try {
            graph.compute(graphComputerClass.get()).submit().get();
            fail("Should throw an IllegalStateException when there is no vertex program nor map reducers");
        } catch (Exception ex) {
            validateException(GraphComputer.Exceptions.computerHasNoVertexProgramNorMapReducers(), ex);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFailIfIsolationIsNotSupported() {
        final GraphComputer computer = graph.compute(graphComputerClass.get());
        ;
        if (!computer.features().supportsIsolation(GraphComputer.Isolation.BSP)) {
            try {
                computer.isolation(GraphComputer.Isolation.BSP);
                fail("GraphComputer.isolation() should throw an exception if the isolation is not supported");
            } catch (Exception ex) {
                validateException(GraphComputer.Exceptions.isolationNotSupported(GraphComputer.Isolation.BSP), ex);
            }
        }
        if (!computer.features().supportsIsolation(GraphComputer.Isolation.DIRTY_BSP)) {
            try {
                computer.isolation(GraphComputer.Isolation.DIRTY_BSP);
                fail("GraphComputer.isolation() should throw an exception if the isolation is not supported");
            } catch (Exception ex) {
                validateException(GraphComputer.Exceptions.isolationNotSupported(GraphComputer.Isolation.DIRTY_BSP), ex);
            }
        }
        assertEquals(StringFactory.graphComputerString(computer), computer.toString());
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
    }

    public static class BadGraphComputer implements GraphComputer {
        @Override
        public GraphComputer isolation(final Isolation isolation) {
            return null;
        }

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
        public Future<ComputerResult> submit() {
            return null;
        }

    }
    /////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldHaveImmutableComputeResultMemory() throws Exception {
        final ComputerResult results = graph.compute(graphComputerClass.get()).program(new VertexProgramB()).submit().get();

        try {
            results.memory().set("set", "test");
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }

        try {
            results.memory().incr("incr", 1);
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }

        try {
            results.memory().and("and", true);
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }

        try {
            results.memory().or("or", false);
        } catch (Exception ex) {
            validateException(Memory.Exceptions.memoryIsCurrentlyImmutable(), ex);
        }
    }

    public static class VertexProgramB extends StaticVertexProgram {
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
        public Set<String> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList("set", "incr", "and", "or"));
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
            graph.compute(graphComputerClass.get()).program(new VertexProgramC()).submit().get();
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
        public Set<String> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(null));
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
            graph.compute(graphComputerClass.get()).program(new VertexProgramD()).submit().get();
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
        public Set<String> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList(""));
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
    public void shouldNotAllowSettingUndeclaredMemoryKeys() throws Exception {
        try {
            graph.compute(graphComputerClass.get()).program(new VertexProgramE()).submit().get();
            fail("Setting a memory key that wasn't declared should fail");
        } catch (Exception ex) {
            // TODO: validateException(GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey("a"), ex.getCause());
        }
    }

    public static class VertexProgramE extends StaticVertexProgram {
        @Override
        public void setup(final Memory memory) {
            memory.set("a", true);
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
    ////////////////////////////////////////////

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldNotAllowTheSameComputerToExecutedTwice() throws Exception {
        final GraphComputer computer = graph.compute(graphComputerClass.get()).program(new VertexProgramA());
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
        ComputerResult results = graph.compute(graphComputerClass.get()).program(new VertexProgramF()).submit().get();
        assertEquals(1, results.memory().getIteration());
        assertEquals(2, results.memory().asMap().size());
        assertEquals(2, results.memory().keys().size());
        assertTrue(results.memory().keys().contains("a"));
        assertTrue(results.memory().keys().contains("b"));
        assertTrue(results.memory().getRuntime() >= 0);

        assertEquals(Long.valueOf(12), results.memory().<Long>get("a"));   // 2 iterations
        assertEquals(Long.valueOf(28), results.memory().<Long>get("b"));
        try {
            results.memory().get("BAD");
            fail("Should throw an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertEquals(Memory.Exceptions.memoryDoesNotExist("BAD").getMessage(), e.getMessage());
        }
        assertEquals(Long.valueOf(6), results.graph().traversal().V().count().next());

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

            memory.incr("a", 1);
            if (memory.isInitialIteration()) {
                vertex.property(VertexProperty.Cardinality.single, "nameLengthCounter", vertex.<String>value("name").length());
                memory.incr("b", vertex.<String>value("name").length());
            } else {
                vertex.property(VertexProperty.Cardinality.single, "nameLengthCounter", vertex.<String>value("name").length() + vertex.<Integer>value("nameLengthCounter"));
            }
        }

        @Override
        public boolean terminate(final Memory memory) {
            return memory.getIteration() == 1;
        }

        @Override
        public Set<String> getElementComputeKeys() {
            return new HashSet<>(Arrays.asList("nameLengthCounter"));
        }

        @Override
        public Set<String> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList("a", "b"));
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

    /////////////////////////////////////////////
    @Test
    @LoadGraphWith(MODERN)
    public void shouldAndOrIncrCorrectlyThroughSubStages() throws Exception {
        ComputerResult results = graph.compute(graphComputerClass.get()).program(new VertexProgramG()).submit().get();
        assertEquals(2, results.memory().getIteration());
        assertEquals(5, results.memory().asMap().size());
        assertEquals(5, results.memory().keys().size());
        assertTrue(results.memory().keys().contains("a"));
        assertTrue(results.memory().keys().contains("b"));
        assertTrue(results.memory().keys().contains("c"));
        assertTrue(results.memory().keys().contains("d"));
        assertTrue(results.memory().keys().contains("e"));

        assertEquals(Long.valueOf(18), results.memory().get("a"));
        assertEquals(Long.valueOf(0), results.memory().get("b"));
        assertFalse(results.memory().get("c"));
        assertTrue(results.memory().get("d"));
        assertTrue(results.memory().get("e"));
    }

    public static class VertexProgramG extends StaticVertexProgram {

        @Override
        public void setup(final Memory memory) {
            memory.set("a", 0l);
            memory.set("b", 0l);
            memory.set("c", true);
            memory.set("d", false);
            memory.set("e", true);
        }

        @Override
        public void execute(Vertex vertex, Messenger messenger, Memory memory) {
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

            // update current step values
            memory.incr("a", 1l);
            memory.incr("b", 1l);
            memory.and("c", false);
            memory.or("d", true);
            memory.and("e", false);

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
        }

        @Override
        public boolean terminate(Memory memory) {
            assertEquals(Long.valueOf(6 * (memory.getIteration() + 1)), memory.get("a"));
            assertEquals(Long.valueOf(6), memory.get("b"));
            assertFalse(memory.get("c"));
            assertTrue(memory.get("d"));
            assertFalse(memory.get("e"));
            memory.set("b", 0l);
            memory.set("e", true);
            return memory.getIteration() > 1;
        }

        @Override
        public Set<String> getElementComputeKeys() {
            return Collections.emptySet();
        }

        @Override
        public Set<String> getMemoryComputeKeys() {
            return new HashSet<>(Arrays.asList("a", "b", "c", "d", "e"));
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
        final ComputerResult results = graph.compute(graphComputerClass.get()).mapReduce(new MapReduceA()).submit().get();
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
            while(values.hasNext()) {
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
        final ComputerResult results = graph.compute(graphComputerClass.get())
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
        public Set<String> getElementComputeKeys() {
            return new HashSet<>(Arrays.asList("counter"));
        }

        @Override
        public Set<String> getMemoryComputeKeys() {
            return Collections.emptySet();
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
            while(values.hasNext()) {
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
        final ComputerResult results = graph.compute(graphComputerClass.get()).mapReduce(new MapReduceB()).submit().get();
        final List<Long> ids = results.memory().get("ids");
        assertEquals(6, ids.size());
        for (int i = 1; i < ids.size(); i++) {
            assertTrue(ids.get(i) < ids.get(i - 1));
        }
    }

    public static class MapReduceB extends StaticMapReduce<Long, Long, Long, Long, List<Long>> {

        @Override
        public boolean doStage(final Stage stage) {
            return stage.equals(Stage.REDUCE) || stage.equals(Stage.MAP);
        }

        @Override
        public void map(final Vertex vertex, final MapEmitter<Long, Long> emitter) {
            emitter.emit(Long.valueOf(vertex.id().toString()), Long.valueOf(vertex.id().toString()));
        }

        @Override
        public void reduce(Long key, Iterator<Long> values, ReduceEmitter<Long, Long> emitter) {
            values.forEachRemaining(id -> emitter.emit(id, id));
        }

        @Override
        public Optional<Comparator<Long>> getReduceKeySort() {
            return Optional.of(Comparator.<Long>reverseOrder());
        }

        @Override
        public String getMemoryKey() {
            return "ids";
        }

        @Override
        public List<Long> generateFinalResult(final Iterator<KeyValue<Long, Long>> keyValues) {
            final List<Long> list = new ArrayList<>();
            keyValues.forEachRemaining(id -> list.add(id.getKey()));
            return list;
        }
    }
    /////////////////////////////////////////////

    @Test
    @Ignore("Because of Graph.engine()")
    @LoadGraphWith(MODERN)
    public void shouldSupportStringTraversalVertexProgramExecution() throws Exception {

    }

    @Test
    @Ignore
    @LoadGraphWith(MODERN)
    public void shouldNotAllowEdgeAccessInMapReduce() throws Exception {

    }

}
