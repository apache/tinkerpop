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
package org.apache.tinkerpop.gremlin.process.util;

import org.apache.tinkerpop.gremlin.process.TraversalStrategiesTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalStrategiesTest {

    TraversalStrategy
            a = new TraversalStrategiesTest.StrategyA(),
            b = new TraversalStrategiesTest.StrategyB(),
            c = new TraversalStrategiesTest.StrategyC(),
            d = new TraversalStrategiesTest.StrategyD(),
            e = new TraversalStrategiesTest.StrategyE(),
            k = new TraversalStrategiesTest.StrategyK(),
            l = new TraversalStrategiesTest.StrategyL(),
            m = new TraversalStrategiesTest.StrategyM(),
            n = new TraversalStrategiesTest.StrategyN(),
            o = new TraversalStrategiesTest.StrategyO();

    @Test
    public void testWellDefinedDependency() {
        //Dependency well defined
        TraversalStrategies s = new DefaultTraversalStrategies();
        s.addStrategies(b, a);
        assertEquals(2, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
    }

    @Test
    public void testNoDependency() {
        //No dependency
        TraversalStrategies s = new DefaultTraversalStrategies();
        s.addStrategies(c, a);
        assertEquals(2, s.toList().size());
    }

    @Test
    public void testWellDefinedDependency2() {
        //Dependency well defined
        TraversalStrategies s = new DefaultTraversalStrategies();
        s.addStrategies(c, a, b);
        assertEquals(3, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(c, s.toList().get(2));
        s = s.clone();
        assertEquals(3, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(c, s.toList().get(2));
    }

    @Test
    public void testCircularDependency() {
        //Circular dependency => throws exception
        TraversalStrategies s = new DefaultTraversalStrategies();
        try {
            s.addStrategies(c, k, a, b);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }
    }

    @Test
    public void testWellDefinedDependency3() {
        //Dependency well defined
        TraversalStrategies s = new DefaultTraversalStrategies();
        s.addStrategies(d, c, a, e, b);
        assertEquals(5, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(d, s.toList().get(2));
        assertEquals(c, s.toList().get(3));
        assertEquals(e, s.toList().get(4));
        s = s.clone();
        assertEquals(5, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(d, s.toList().get(2));
        assertEquals(c, s.toList().get(3));
        assertEquals(e, s.toList().get(4));
    }

    @Test
    public void testCircularDependency2() {
        //Circular dependency => throws exception
        TraversalStrategies s = new DefaultTraversalStrategies();
        try {
            s.addStrategies(d, c, k, a, e, b);
            fail();
        } catch (IllegalStateException ex) {
            assertTrue(ex.getMessage().toLowerCase().contains("cyclic"));
        }
    }

    @Test
    public void testLotsOfStrategies() {
        //Lots of strategies
        TraversalStrategies s = new DefaultTraversalStrategies();
        s = s.addStrategies(b, l, m, n, o, a);
        assertTrue(s.toList().indexOf(a) < s.toList().indexOf(b));
        s = s.clone();
        assertTrue(s.toList().indexOf(a) < s.toList().indexOf(b));

        // sort and then add more
        s = new DefaultTraversalStrategies();
        s.addStrategies(b, a, c);
        assertEquals(3, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(c, s.toList().get(2));
        s.addStrategies(d);
        s = s.clone();
        assertEquals(4, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(d, s.toList().get(2));
        assertEquals(c, s.toList().get(3));
        s.addStrategies(e);
        assertEquals(5, s.toList().size());
        assertEquals(a, s.toList().get(0));
        assertEquals(b, s.toList().get(1));
        assertEquals(d, s.toList().get(2));
        assertEquals(c, s.toList().get(3));
        assertEquals(e, s.toList().get(4));
    }
}

