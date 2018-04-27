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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class OrderTest {

    private static final SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");

    @Parameterized.Parameters(name = "{0}.test({1},{2})")
    public static Iterable<Object[]> data() throws ParseException {
        return new ArrayList<>(Arrays.asList(new Object[][]{
                {Order.asc, Arrays.asList("b", "a", "c", "d"), Arrays.asList("a", "b", "c", "d")},
                {Order.desc, Arrays.asList("b", "a", "c", "d"), Arrays.asList("d", "c", "b", "a")},
                {Order.asc, Arrays.asList(formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2008")),
                            Arrays.asList(formatter.parse("1-Jan-2008"), formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"))},
                {Order.desc, Arrays.asList(formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2008")),
                             Arrays.asList(formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2008"))},
                {Order.desc, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L)},
                {Order.asc, Arrays.asList(100.1f, 1.1f, -1.1f, 0.1f), Arrays.asList(-1.1f, 0.1f, 1.1f, 100.1f)},
                {Order.desc, Arrays.asList(100.1f, 1.1f, -1.1f, 0.1f), Arrays.asList(100.1f, 1.1f, 0.1f, -1.1f)},
                {Order.asc, Arrays.asList(100.1d, 1.1d, -1.1d, 0.1d), Arrays.asList(-1.1d, 0.1d, 1.1d, 100.1d)},
                {Order.desc, Arrays.asList(100.1d, 1.1d, -1.1d, 0.1d), Arrays.asList(100.1d, 1.1d, 0.1d, -1.1d)},
                {Order.asc, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(-1L, 0L, 1L, 100L)},
                {Order.desc, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L)},
                {Order.asc, Arrays.asList(100, 1, -1, 0), Arrays.asList(-1, 0, 1, 100)},
                {Order.desc, Arrays.asList(100, 1, -1, 0), Arrays.asList(100, 1, 0, -1)},
                {Order.incr, Arrays.asList("b", "a", "c", "d"), Arrays.asList("a", "b", "c", "d")},
                {Order.decr, Arrays.asList("b", "a", "c", "d"), Arrays.asList("d", "c", "b", "a")},
                {Order.incr, Arrays.asList(formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2008")),
                             Arrays.asList(formatter.parse("1-Jan-2008"), formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"))},
                {Order.decr, Arrays.asList(formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2008")),
                             Arrays.asList(formatter.parse("1-Jan-2020"), formatter.parse("1-Jan-2018"), formatter.parse("1-Jan-2008"))},
                {Order.decr, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L)},
                {Order.incr, Arrays.asList(100.1f, 1.1f, -1.1f, 0.1f), Arrays.asList(-1.1f, 0.1f, 1.1f, 100.1f)},
                {Order.decr, Arrays.asList(100.1f, 1.1f, -1.1f, 0.1f), Arrays.asList(100.1f, 1.1f, 0.1f, -1.1f)},
                {Order.incr, Arrays.asList(100.1d, 1.1d, -1.1d, 0.1d), Arrays.asList(-1.1d, 0.1d, 1.1d, 100.1d)},
                {Order.decr, Arrays.asList(100.1d, 1.1d, -1.1d, 0.1d), Arrays.asList(100.1d, 1.1d, 0.1d, -1.1d)},
                {Order.incr, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(-1L, 0L, 1L, 100L)},
                {Order.decr, Arrays.asList(100L, 1L, -1L, 0L), Arrays.asList(100L, 1L, 0L, -1L)},
                {Order.incr, Arrays.asList(100, 1, -1, 0), Arrays.asList(-1, 0, 1, 100)},
                {Order.decr, Arrays.asList(100, 1, -1, 0), Arrays.asList(100, 1, 0, -1)}}));
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
