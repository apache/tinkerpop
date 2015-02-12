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

import org.apache.tinkerpop.gremlin.process.graph.traversal.strategy.AbstractTraversalStrategy;
import org.junit.Test;

import java.util.Collections;
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
     * Tests that {@link org.apache.tinkerpop.gremlin.process.TraversalStrategies#sortStrategies(java.util.List)}
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
                k = new StrategyK();

        List<TraversalStrategy> s;

        //Dependency well defined
        s = Stream.of(b, a)
                .collect(Collectors.toList());
        TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));

        //No dependency
        s = Stream.of(c, a)
                .collect(Collectors.toList());
        TraversalStrategies.sortStrategies(s);
        assertEquals(2, s.size());
        assertEquals(c, s.get(0));
        assertEquals(a, s.get(1));

        //Dependency well defined
        s = Stream.of(c, a, b)
                .collect(Collectors.toList());
        TraversalStrategies.sortStrategies(s);
        assertEquals(3, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(c, s.get(2));

        //Circular dependency => throws exception
        s = Stream.of(c, k, a, b)
                .collect(Collectors.toList());
        try {
            TraversalStrategies.sortStrategies(s);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }

        //Dependency well defined
        s = Stream.of(d, c, a, e, b)
                .collect(Collectors.toList());
        TraversalStrategies.sortStrategies(s);
        assertEquals(5, s.size());
        assertEquals(a, s.get(0));
        assertEquals(b, s.get(1));
        assertEquals(d, s.get(2));
        assertEquals(c, s.get(3));
        assertEquals(e, s.get(4));

        //Circular dependency => throws exception
        s = Stream.of(d, c, k, a, e, b)
                .collect(Collectors.toList());
        try {
            TraversalStrategies.sortStrategies(s);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }
    }


    public static class StrategyA extends DummyStrategy {

        @Override
        public Set<Class<? extends TraversalStrategy>> applyPrior() {
            return Collections.EMPTY_SET;
        }

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

    private static class DummyStrategy extends AbstractTraversalStrategy {

        @Override
        public void apply(Traversal.Admin<?, ?> traversal, TraversalEngine traversalEngine) {
            //Do nothing
        }
    }

}
