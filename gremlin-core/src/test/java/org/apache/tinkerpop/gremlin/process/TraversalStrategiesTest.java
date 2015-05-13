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

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.junit.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TraversalStrategiesTest {

    /**
     * Tests that {@link org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies#sortStrategies(java.util.List)}
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

        List<TraversalStrategy<?>> s;

        //Dependency well defined
        s = Arrays.asList(b, a);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));

        //No dependency
        s = Arrays.asList(c, a);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());

        //Dependency well defined
        s = Arrays.asList(c, a, b);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(3, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(c, s.get(2));

        //Circular dependency => throws exception
        s = Arrays.asList(c, k, a, b);
        try {
            TraversalStrategies.sortStrategies(s);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }

        //Dependency well defined
        s = Arrays.asList(d, c, a, e, b);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(5, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(d, s.get(2));
        assertEquals(c, s.get(3));
        assertEquals(e, s.get(4));

        //Circular dependency => throws exception
        s = Arrays.asList(d, c, k, a, e, b);
        try {
            TraversalStrategies.sortStrategies(s);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }

        //Lots of strategies
        s = Arrays.asList(b, l, m, n, o, a);
        s = TraversalStrategies.sortStrategies(s);
        assertTrue(s.indexOf(a) < s.indexOf(b));

        // sort and then add more
        s = new ArrayList<>((List)Arrays.asList(b,a,c));
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(3, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(c, s.get(2));
        s.add(d);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(4, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(d, s.get(2));
        assertEquals(c, s.get(3));
        s.add(e);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(5, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(d, s.get(2));
        assertEquals(c, s.get(3));
        assertEquals(e, s.get(4));

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

        List<TraversalStrategy<?>> s;

        //in category sorting
        s = Arrays.asList(b, a);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));

        //mixed category sorting
        s = Arrays.asList(a, e, b, d);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(4, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(d, s.get(2));
        assertEquals(e, s.get(3));

        //full reverse sorting
        s = Arrays.asList(k,e,d,c,b,a);
        s = TraversalStrategies.sortStrategies(s);
        assertEquals(6, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(c, s.get(2));
        assertEquals(d, s.get(3));
        assertEquals(e, s.get(4));
        assertEquals(k, s.get(5));
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
