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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.util.GremlinValueComparator;
import org.apache.tinkerpop.gremlin.util.tools.CollectionFactory;
import org.javatuples.Pair;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import static org.apache.tinkerpop.gremlin.util.GremlinValueComparator.Type;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asList;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asMap;
import static org.apache.tinkerpop.gremlin.util.tools.CollectionFactory.asSet;

import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class OrderTest {

    /**
     * @author Stephen Mallette (http://stephen.genoprime.com)
     */
    @RunWith(Parameterized.class)
    public static class OrderListTest {

        private static final SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy", Locale.US);

        @Parameterized.Parameters(name = "{0}.sort({1}) = {2}")
        public static Iterable<Object[]> data() throws ParseException {
            return new ArrayList<>(Arrays.asList(new Object[][]{
                    {Order.asc, Arrays.asList("a", "c", null, "d"), Arrays.asList(null, "a", "c", "d")},
                    {Order.asc, Arrays.asList("b", "a", "c", "d"), Arrays.asList("a", "b", "c", "d")},
                    {Order.desc, Arrays.asList("b", "a", "c", "d"), Arrays.asList("d", "c", "b", "a")},
                    {Order.desc, Arrays.asList("c", "a", null, "d"), Arrays.asList("d", "c", "a", null)},
                    {Order.asc, Arrays.asList(formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2008")),
                            Arrays.asList(formatter.parse("1-Jan-2008"), formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"))},
                    {Order.desc, Arrays.asList(formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2008")),
                            Arrays.asList(formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2008"))},
                    {Order.desc, Arrays.asList(100L, 1L, null, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L, null)},
                    {Order.desc, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L)},
                    {Order.asc, Arrays.asList(100L, 1L, null, -1L, 0L), Arrays.asList(null, -1L, 0L, 1L, 100L)},
                    {Order.asc, Arrays.asList(100.1f, 1.1f, -1.1f, 0.1f), Arrays.asList(-1.1f, 0.1f, 1.1f, 100.1f)},
                    {Order.desc, Arrays.asList(100.1f, 1.1f, -1.1f, 0.1f), Arrays.asList(100.1f, 1.1f, 0.1f, -1.1f)},
                    {Order.asc, Arrays.asList(100.1d, 1.1d, -1.1d, 0.1d), Arrays.asList(-1.1d, 0.1d, 1.1d, 100.1d)},
                    {Order.desc, Arrays.asList(100.1d, 1.1d, -1.1d, 0.1d), Arrays.asList(100.1d, 1.1d, 0.1d, -1.1d)},
                    {Order.asc, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(-1L, 0L, 1L, 100L)},
                    {Order.desc, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L)},
                    {Order.asc, Arrays.asList(100, 1, -1, 0), Arrays.asList(-1, 0, 1, 100)},
                    {Order.desc, Arrays.asList(100, 1, -1, 0), Arrays.asList(100, 1, 0, -1)},
                    {Order.asc, Arrays.asList("b", "a", T.id, "c", "d"), Arrays.asList("a", "b", "c", "d", T.id)},
                    {Order.desc, Arrays.asList("b", "a", T.id, "c", "d"), Arrays.asList(T.id, "d", "c", "b", "a")}}));
        }

        @Parameterized.Parameter(value = 0)
        public Order order;

        @Parameterized.Parameter(value = 1)
        public Object toBeOrdered;

        @Parameterized.Parameter(value = 2)
        public Object expectedOrder;

        @Test
        public void shouldOrder() {
            Collections.sort((List) toBeOrdered, order);
            assertEquals(expectedOrder, toBeOrdered);
        }
    }

    /**
     * @author Mike Personick (http://github.com/mikepersonick)
     */
    @RunWith(Parameterized.class)
    public static class OrderabilityTest {

        private static final Comparator order = GremlinValueComparator.ORDER;

        private static final Object NaN = new Object() {
            public String toString() { return "NaN"; }
        };

        @Parameterized.Parameters(name = "Order.compare({0},{1}) = {2}")
        public static Iterable<Object[]> data() throws ParseException {
            return new ArrayList<>(Arrays.asList(new Object[][]{
                    // NaN tests - compares to 0 for itself, but larger than anything else including +Inf
                    {NaN, NaN, 0},
                    {null, NaN, -1},
                    {NaN, null, 1},
                    {null, null, 0},
                    {NaN, 0, 1},
                    {0, NaN, -1},
                    {NaN, Double.POSITIVE_INFINITY, 1},
                    {NaN, Float.POSITIVE_INFINITY, 1},

                    // type promotion means no stable order for Numbers
                    {1, 1.0d, 0},
                    {1.0d, 1, 0},

                    // cross type
                    {"foo", 1, Type.String.priority() - Type.Number.priority()},
                    {MutablePath.make(), Collections.emptySet(), Type.Path.priority() - Type.Set.priority()},
                    
                    // Collections
                    {asList(1, 2, 3), asList(1, 2, 3), 0},
                    {asList(1, 2, 3), asList(1, 2, 3, 4), -1},
                    {asList(1, 2, 4), asList(1, 2, 3, 4), 1},
                    {asSet(1, 2, 3), asSet(1, 2, 3), 0},
                    {asSet(1, 2, 3), asSet(1, 2, 3, 4), -1},
                    {asSet(1, 2, 4), asSet(1, 2, 3, 4), 1},
                    {asMap(1, 1, 2, 2, 3, 3), asMap(1, 1, 2, 2, 3, 3), 0},
                    {asMap(1, 1, 2, 2, 3, 3), asMap(1, 1, 2, 2, 3, 3, 4, 4), -1},
                    {asMap(1, 1, 2, "foo", 3, 3), asMap(1, 1, 2, 2, 3, 3), Type.String.priority() - Type.Number.priority()},
                    {asList(Double.NaN), asList(Float.NaN), 0},
                    {asList(Double.NaN), asList(0), 1},
                    {asList(0), asList(Double.NaN), -1},
                    {asMap(1, 1), asMap(1, null), 1},
                    {asList(0), asList("foo"), Type.Number.priority() - Type.String.priority()},

            }));
        }

        @Parameterized.Parameter(value = 0)
        public Object a;

        @Parameterized.Parameter(value = 1)
        public Object b;

        @Parameterized.Parameter(value = 2)
        public Integer expected;

        @Test
        public void shouldOrder() {
            if (a == NaN || b == NaN) {
                // test all the NaN combos
                final List<Pair> args = new ArrayList<>();
                if (a == NaN && b == NaN) {
                    args.add(new Pair(Double.NaN, Double.NaN));
                    args.add(new Pair(Double.NaN, Float.NaN));
                    args.add(new Pair(Float.NaN, Double.NaN));
                    args.add(new Pair(Float.NaN, Float.NaN));
                } else if (a == NaN) {
                    args.add(new Pair(Double.NaN, b));
                    args.add(new Pair(Float.NaN, b));
                } else {
                    args.add(new Pair(a, Double.NaN));
                    args.add(new Pair(a, Float.NaN));
                }
                for (final Pair arg : args) {
                    assertEquals(expected.longValue(), order.compare(arg.getValue0(), arg.getValue1()));
                }
            } else {
                assertEquals(expected.longValue(), order.compare(a, b));
            }
        }
    }

}
