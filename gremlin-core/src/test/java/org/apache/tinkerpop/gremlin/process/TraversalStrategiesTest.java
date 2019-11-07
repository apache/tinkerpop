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
package org.apache.tinkerpop.gremlin.process;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */
public class TraversalStrategiesTest {

    @Test
    public void shouldAllowUserManipulationOfGlobalCache() {
        ///////////
        // GRAPH //
        ///////////
        TestGraph graph = new TestGraph();
        TraversalStrategies strategies = graph.traversal().getStrategies();
        assertFalse(TraversalStrategies.GlobalCache.getStrategies(Graph.class).toList().isEmpty());
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(Graph.class).iterator().hasNext());
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(Graph.class)) {
            assertTrue(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(TestGraphComputer.class)) {
            assertFalse(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        assertTrue(strategies.getStrategy(StrategyA.class).isPresent());
        assertTrue(strategies.getStrategy(StrategyB.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyC.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyD.class).isPresent());
        strategies.addStrategies(new StrategyD());
        strategies.removeStrategies(StrategyA.class);
        assertFalse(strategies.getStrategy(StrategyA.class).isPresent());
        assertTrue(strategies.getStrategy(StrategyD.class).isPresent());
        ///
        graph = new TestGraph();
        strategies = graph.traversal().getStrategies();
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(Graph.class)) {
            assertTrue(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(TestGraphComputer.class)) {
            assertFalse(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        assertFalse(strategies.getStrategy(StrategyA.class).isPresent());
        assertTrue(strategies.getStrategy(StrategyB.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyC.class).isPresent());
        assertTrue(strategies.getStrategy(StrategyD.class).isPresent());
        //////////////////////
        /// GRAPH COMPUTER ///
        //////////////////////
        strategies = TraversalStrategies.GlobalCache.getStrategies(TestGraphComputer.class);
        assertFalse(TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).toList().isEmpty());
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class)) {
            assertTrue(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(TestGraph.class)) {
            assertFalse(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        assertFalse(strategies.getStrategy(StrategyA.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyB.class).isPresent());
        assertTrue(strategies.getStrategy(StrategyC.class).isPresent());
        strategies.addStrategies(new StrategyE());
        strategies.removeStrategies(StrategyC.class);
        //
        strategies = TraversalStrategies.GlobalCache.getStrategies(TestGraphComputer.class);
        assertFalse(TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).toList().isEmpty());
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class)) {
            assertTrue(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        for (final TraversalStrategy strategy : TraversalStrategies.GlobalCache.getStrategies(TestGraph.class)) {
            assertFalse(strategies.getStrategy(strategy.getClass()).isPresent());
        }
        assertFalse(strategies.getStrategy(StrategyA.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyB.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyC.class).isPresent());
        assertFalse(strategies.getStrategy(StrategyD.class).isPresent());
        assertTrue(strategies.getStrategy(StrategyE.class).isPresent());
    }

    public static class TestGraphComputer implements GraphComputer {

        static {
            TraversalStrategies.GlobalCache.registerStrategies(TestGraphComputer.class,
                    TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone().addStrategies(new StrategyC()));
        }

        @Override
        public GraphComputer result(ResultGraph resultGraph) {
            return this;
        }

        @Override
        public GraphComputer persist(Persist persist) {
            return this;
        }

        @Override
        public GraphComputer program(VertexProgram vertexProgram) {
            return this;
        }

        @Override
        public GraphComputer mapReduce(MapReduce mapReduce) {
            return this;
        }

        @Override
        public GraphComputer workers(int workers) {
            return this;
        }

        @Override
        public GraphComputer vertices(Traversal<Vertex, Vertex> vertexFilter) throws IllegalArgumentException {
            return this;
        }

        @Override
        public GraphComputer edges(Traversal<Vertex, Edge> edgeFilter) throws IllegalArgumentException {
            return this;
        }

        @Override
        public Future<ComputerResult> submit() {
            return new CompletableFuture<>();
        }
    }

    public static class TestGraph implements Graph {

        static {
            TraversalStrategies.GlobalCache.registerStrategies(TestGraph.class,
                    TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(new StrategyA(), new StrategyB()));
        }

        @Override
        public Vertex addVertex(Object... keyValues) {
            return null;
        }

        @Override
        public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
            return (C) new TestGraphComputer();
        }

        @Override
        public GraphComputer compute() throws IllegalArgumentException {
            return new TestGraphComputer();
        }

        @Override
        public Iterator<Vertex> vertices(Object... vertexIds) {
            return Collections.emptyIterator();
        }

        @Override
        public Iterator<Edge> edges(Object... edgeIds) {
            return Collections.emptyIterator();
        }

        @Override
        public Transaction tx() {
            return null;
        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public Variables variables() {
            return null;
        }

        @Override
        public Configuration configuration() {
            return new BaseConfiguration();
        }
    }

    /**
     * Tests that {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies#sortStrategies(java.util.Set)}
     * works as advertised. This class defines a bunch of dummy strategies which define an order. It is verified
     * that the right order is being returned.
     */
    @Test
    public void testTraversalStrategySorting() {
        TraversalStrategy
                a = new StrategyA(),
                b = new StrategyB(),
                c = new StrategyC(),
                d = new StrategyD(),
                e = new StrategyE(),
                k = new StrategyK(),
                l = new StrategyL(),
                m = new StrategyM(),
                n = new StrategyN(),
                o = new StrategyO();

        Set<TraversalStrategy<?>> s;

        //Dependency well defined
        s = new LinkedHashSet<>((Collection) Arrays.asList(b, a));
        s = TraversalStrategies.sortStrategies(s);
        Iterator<TraversalStrategy<?>> it = s.iterator();
        assertEquals(2, s.size());
        assertEquals(a, it.next());
        assertEquals(b, it.next());

        //No dependency
        s = new LinkedHashSet<>((Collection) Arrays.asList(c, a));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());

        //Dependency well defined
        s = new LinkedHashSet<>((Collection)Arrays.asList(c, a, b));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(3, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(c, it.next());

        //Circular dependency => throws exception
        s = new LinkedHashSet<>((Collection)Arrays.asList(c, k, a, b));
        try {
            TraversalStrategies.sortStrategies(s);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }

        //Dependency well defined
        s = new LinkedHashSet<>((Collection) Arrays.asList(d, c, a, e, b));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(5, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(d, it.next());
        assertEquals(c, it.next());
        assertEquals(e, it.next());

        //Circular dependency => throws exception
        s = new LinkedHashSet<>((Collection) Arrays.asList(d, c, k, a, e, b));
        try {
            TraversalStrategies.sortStrategies(s);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }

        //Lots of strategies
        s = new LinkedHashSet<>((Collection) Arrays.asList(b, l, m, n, o, a));
        s = TraversalStrategies.sortStrategies(s);
        List<TraversalStrategy<?>> list = new ArrayList<>(s);
        assertTrue(list.indexOf(a) < list.indexOf(b));

        // sort and then add more
        s = new LinkedHashSet<>(new LinkedHashSet<>((Collection) Arrays.asList(b, a, c)));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(3, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(c, it.next());
        s.add(d);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(4, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(d, it.next());
        assertEquals(c, it.next());
        s.add(e);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(5, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(d, it.next());
        assertEquals(c, it.next());
        assertEquals(e, it.next());

    }


    public static class StrategyA extends DummyStrategy {

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPost() {
            return Stream.of(StrategyB.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyB extends DummyStrategy {

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPrior() {
            return Stream.of(StrategyA.class).collect(Collectors.toSet());
        }

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPost() {
            return Stream.of(StrategyC.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyC extends DummyStrategy {

    }

    public static class StrategyD extends DummyStrategy {

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPrior() {
            return Stream.of(StrategyB.class).collect(Collectors.toSet());
        }

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPost() {
            return Stream.of(StrategyC.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyE extends DummyStrategy {

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPrior() {
            return Stream.of(StrategyC.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyK extends DummyStrategy {

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPrior() {
            return Stream.of(StrategyC.class).collect(Collectors.toSet());
        }

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPost() {
            return Stream.of(StrategyA.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyL extends DummyStrategy {

    }


    public static class StrategyM extends DummyStrategy {

    }

    public static class StrategyN extends DummyStrategy {

    }

    public static class StrategyO extends DummyStrategy {

    }


    private static class DummyStrategy<S extends TraversalStrategy> extends AbstractTraversalStrategy<S> {

        @Override
        public void apply(Traversal.Admin<?, ?> traversal) {
            //Do nothing
        }
    }

    ////////////////////////////////////////////
    ////////////////////////////////////////////
    ////////////////////////////////////////////

    @Test
    public void testTraversalStrategySortingWithCategories() {
        TraversalStrategy
                a = new StrategyADecoration(),
                b = new StrategyBDecoration(),
                c = new StrategyCOptimization(),
                d = new StrategyDOptimization(),
                e = new StrategyEFinalization(),
                k = new StrategyKVerification();

        Set<TraversalStrategy<?>> s;

        //in category sorting
        s = new LinkedHashSet<>((Collection) Arrays.asList(b, a));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());
        Iterator<TraversalStrategy<?>> it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());

        //mixed category sorting
        s = new LinkedHashSet<>((Collection) Arrays.asList(a, e, b, d));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(4, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(d, it.next());
        assertEquals(e, it.next());

        //full reverse sorting
        s = new LinkedHashSet<>((Collection) Arrays.asList(k, e, d, c, b, a));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(6, s.size());
        it = s.iterator();
        assertEquals(a, it.next());
        assertEquals(b, it.next());
        assertEquals(c, it.next());
        assertEquals(d, it.next());
        assertEquals(e, it.next());
        assertEquals(k, it.next());
    }

    public static class StrategyADecoration extends DummyStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

        @Override
        public Set<Class<? extends DecorationStrategy>> applyPost() {
            return Stream.of(StrategyBDecoration.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyBDecoration extends DummyStrategy<TraversalStrategy.DecorationStrategy> implements TraversalStrategy.DecorationStrategy {

    }

    public static class StrategyCOptimization extends DummyStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

        @Override
        public Set<Class<? extends OptimizationStrategy>> applyPost() {
            return Stream.of(StrategyDOptimization.class).collect(Collectors.toSet());
        }
    }

    public static class StrategyDOptimization extends DummyStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

        @Override
        public Set<Class<? extends OptimizationStrategy>> applyPrior() {
            return Stream.of(StrategyCOptimization.class).collect(Collectors.toSet());
        }

    }

    public static class StrategyEFinalization extends DummyStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {

    }

    public static class StrategyKVerification extends DummyStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {


    }

}
