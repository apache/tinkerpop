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
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.finalization.MatchAlgorithmStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Before;
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

import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.denyStrategy;
import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.getRegisteredStrategyClass;
import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.getRegisteredStrategyClassByFullName;
import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.registerStrategy;
import static org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies.GlobalCache.unregisterStrategy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (marko@markorodriguez.com)
 */
public class TraversalStrategiesTest {

    @Before
    public void setup() {
        TraversalStrategies.GlobalCache.registerStrategies(TestGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().addStrategies(new StrategyA(), new StrategyB()));
        TraversalStrategies.GlobalCache.registerStrategies(TestGraphComputer.class,
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone().addStrategies(new StrategyC()));
    }

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

    @Test
    public void shouldRegisterBuiltInStrategiesByName() {
        assertEquals(OptionsStrategy.class, getRegisteredStrategyClass(OptionsStrategy.class.getSimpleName()).get());
        assertEquals(MatchAlgorithmStrategy.class,
                getRegisteredStrategyClass(MatchAlgorithmStrategy.class.getSimpleName()).get());
        assertEquals(ReadOnlyStrategy.class, getRegisteredStrategyClass(ReadOnlyStrategy.class.getSimpleName()).get());
        assertEquals(CountStrategy.class, getRegisteredStrategyClass(CountStrategy.class.getSimpleName()).get());
    }

    @Test
    public void shouldRegisterGraphAndGraphComputerStrategiesByName() {
        assertEquals(StrategyA.class, getRegisteredStrategyClass(StrategyA.class.getSimpleName()).get());
        assertEquals(StrategyB.class, getRegisteredStrategyClass(StrategyB.class.getSimpleName()).get());
        assertEquals(StrategyC.class, getRegisteredStrategyClass(StrategyC.class.getSimpleName()).get());
    }

    @Test
    public void shouldRegisterAndUnregisterStrategyByNameWithoutChangingDefaults() {
        unregisterStrategy(StrategyD.class);
        assertFalse(getRegisteredStrategyClass(StrategyD.class.getSimpleName()).isPresent());

        try {
            registerStrategy(StrategyD.class);
            assertEquals(StrategyD.class, getRegisteredStrategyClass(StrategyD.class.getSimpleName()).get());

            // registerStrategy() only adds the class to GLOBAL_REGISTRY and must not alter graph defaults
            assertFalse(TraversalStrategies.GlobalCache.getStrategies(Graph.class).
                    getStrategy(StrategyD.class).isPresent());
        } finally {
            unregisterStrategy(StrategyD.class);
        }

        assertFalse(getRegisteredStrategyClass(StrategyD.class.getSimpleName()).isPresent());
    }

    @Test
    public void shouldOverwriteStrategyRegisteredWithSameSimpleName() {
        final String strategyName = FirstStrategyNamespace.DuplicateStrategy.class.getSimpleName();
        unregisterStrategy(FirstStrategyNamespace.DuplicateStrategy.class);

        try {
            registerStrategy(FirstStrategyNamespace.DuplicateStrategy.class);
            assertEquals(FirstStrategyNamespace.DuplicateStrategy.class,
                    getRegisteredStrategyClass(strategyName).get());

            registerStrategy(SecondStrategyNamespace.DuplicateStrategy.class);
            assertEquals(SecondStrategyNamespace.DuplicateStrategy.class,
                    getRegisteredStrategyClass(strategyName).get());
        } finally {
            unregisterStrategy(SecondStrategyNamespace.DuplicateStrategy.class);
        }

        assertFalse(getRegisteredStrategyClass(strategyName).isPresent());
    }

    @Test
    public void shouldNotResolveInvalidStrategyNames() {
        assertFalse(getRegisteredStrategyClass("UnknownStrategy").isPresent());
        assertFalse(getRegisteredStrategyClass(ReadOnlyStrategy.class.getName()).isPresent());
        assertFalse(getRegisteredStrategyClass("readonlystrategy").isPresent());
        assertFalse(getRegisteredStrategyClass("").isPresent());
        assertFalse(getRegisteredStrategyClass(null).isPresent());
    }

    @Test
    public void shouldIgnoreUnregisterOfAbsentStrategy() {
        unregisterStrategy(StrategyD.class);
        unregisterStrategy(AbsentStrategy.class);

        try {
            registerStrategy(StrategyD.class);
            unregisterStrategy(AbsentStrategy.class);

            assertEquals(StrategyD.class, getRegisteredStrategyClass(StrategyD.class.getSimpleName()).get());
            assertFalse(getRegisteredStrategyClass(AbsentStrategy.class.getSimpleName()).isPresent());
        } finally {
            unregisterStrategy(StrategyD.class);
        }
    }

    @Test
    public void shouldIgnoreRegistrationOfDeniedStrategy() {
        registerStrategy(DeniedStrategy.class);
        assertEquals(DeniedStrategy.class,
                getRegisteredStrategyClass(DeniedStrategy.class.getSimpleName()).get());

        denyStrategy(DeniedStrategy.class);
        assertFalse(getRegisteredStrategyClass(DeniedStrategy.class.getSimpleName()).isPresent());

        registerStrategy(DeniedStrategy.class);
        assertFalse(getRegisteredStrategyClass(DeniedStrategy.class.getSimpleName()).isPresent());
        assertFalse(getRegisteredStrategyClassByFullName(DeniedStrategy.class.getName()).isPresent());
    }

    @Test
    public void shouldNotRemoveDeniedStrategyFromCurrentOrFutureGraphCaches() {
        final TraversalStrategies graphStrategies = TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone().
                addStrategies(new DeniedGraphStrategy());
        final TraversalStrategies graphComputerStrategies =
                TraversalStrategies.GlobalCache.getStrategies(GraphComputer.class).clone().
                        addStrategies(new DeniedGraphStrategy());

        TraversalStrategies.GlobalCache.registerStrategies(DeniedTestGraph.class, graphStrategies);
        TraversalStrategies.GlobalCache.registerStrategies(DeniedTestGraphComputer.class, graphComputerStrategies);
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(DeniedTestGraph.class).
                getStrategy(DeniedGraphStrategy.class).isPresent());
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(DeniedTestGraphComputer.class).
                getStrategy(DeniedGraphStrategy.class).isPresent());

        denyStrategy(DeniedGraphStrategy.class);

        assertFalse(getRegisteredStrategyClass(DeniedGraphStrategy.class.getSimpleName()).isPresent());
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(DeniedTestGraph.class).
                getStrategy(DeniedGraphStrategy.class).isPresent());
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(DeniedTestGraphComputer.class).
                getStrategy(DeniedGraphStrategy.class).isPresent());

        TraversalStrategies.GlobalCache.registerStrategies(DeniedTestGraph.class,
                graphStrategies.clone().addStrategies(new DeniedGraphStrategy()));
        TraversalStrategies.GlobalCache.registerStrategies(DeniedTestGraphComputer.class,
                graphComputerStrategies.clone().addStrategies(new DeniedGraphStrategy()));

        assertFalse(getRegisteredStrategyClass(DeniedGraphStrategy.class.getSimpleName()).isPresent());
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(DeniedTestGraph.class).
                getStrategy(DeniedGraphStrategy.class).isPresent());
        assertTrue(TraversalStrategies.GlobalCache.getStrategies(DeniedTestGraphComputer.class).
                getStrategy(DeniedGraphStrategy.class).isPresent());
    }

    public static class TestGraphComputer implements GraphComputer {

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
        public GraphComputer vertexProperties(Traversal<Vertex, ? extends Property<?>> vertexPropertyFilter) {
            return this;
        }

        @Override
        public Future<ComputerResult> submit() {
            return new CompletableFuture<>();
        }
    }

    public static class TestGraph implements Graph {

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

    public static class DeniedTestGraph extends TestGraph {
    }

    public static class DeniedTestGraphComputer extends TestGraphComputer {
    }

    @Test
    public void shouldResolveRegisteredStrategyByFullName() {
        assertEquals(OptionsStrategy.class,
                getRegisteredStrategyClassByFullName(OptionsStrategy.class.getName()).get());
        assertEquals(MatchAlgorithmStrategy.class,
                getRegisteredStrategyClassByFullName(MatchAlgorithmStrategy.class.getName()).get());
        assertEquals(ReadOnlyStrategy.class,
                getRegisteredStrategyClassByFullName(ReadOnlyStrategy.class.getName()).get());
    }

    @Test
    public void shouldResolveNestedRegisteredStrategyByFullName() {
        // StrategyA is nested, so it is registered under the segment of its name that follows the '$'
        assertEquals(StrategyA.class,
                getRegisteredStrategyClassByFullName(StrategyA.class.getName()).get());
    }

    @Test
    public void shouldNotResolveUnregisteredStrategyByFullName() {
        unregisterStrategy(AbsentStrategy.class);
        assertFalse(getRegisteredStrategyClassByFullName(AbsentStrategy.class.getName()).isPresent());
    }

    @Test
    public void shouldNotResolveStrategySharingASimpleNameWithARegisteredOne() {
        // borrowing the simple name of a registered strategy must not admit some other class of that name
        assertFalse(getRegisteredStrategyClassByFullName("com.example.ReadOnlyStrategy").isPresent());
    }

    @Test
    public void shouldNotResolveSimpleNameByFullName() {
        assertFalse(getRegisteredStrategyClassByFullName(ReadOnlyStrategy.class.getSimpleName()).isPresent());
    }

    @Test
    public void shouldNotResolveNullByFullName() {
        assertFalse(getRegisteredStrategyClassByFullName(null).isPresent());
    }

    @Test
    public void shouldResolveStrategyByFullNameAfterItIsRegistered() {
        unregisterStrategy(AbsentStrategy.class);

        try {
            registerStrategy(AbsentStrategy.class);
            assertEquals(AbsentStrategy.class,
                    getRegisteredStrategyClassByFullName(AbsentStrategy.class.getName()).get());
        } finally {
            unregisterStrategy(AbsentStrategy.class);
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

    private static class FirstStrategyNamespace {

        private static class DuplicateStrategy extends DummyStrategy {

        }
    }

    private static class SecondStrategyNamespace {

        private static class DuplicateStrategy extends DummyStrategy {

        }
    }

    private static class AbsentStrategy extends DummyStrategy {

    }

    private static class DeniedStrategy extends DummyStrategy {

    }

    private static class DeniedGraphStrategy extends DummyStrategy {

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
