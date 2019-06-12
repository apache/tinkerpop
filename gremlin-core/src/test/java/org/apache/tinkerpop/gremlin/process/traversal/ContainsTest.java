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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class ContainsTest {

    @Parameterized.Parameters(name = "{0}({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        return new ArrayList<>(Arrays.asList(new Object[][]{
                {Contains.within, 10, Arrays.asList(1, 2, 3, 4, 10), true},
                {Contains.without, 10, Arrays.asList(1, 2, 3, 4, 10), false},
                {Contains.within, 10, Collections.emptyList(), false},
                {Contains.without, 10, Collections.emptyList(), true},
                {Contains.within, 100, Arrays.asList(1, 2, 3, 4, 10), false},
                {Contains.without, 10L, Arrays.asList(1, 2, 3, 4, 10), false},
                {Contains.within, "test", Arrays.asList(1, 2, 3, "test", 10), true},
                {Contains.without, "testing", Arrays.asList(1, 2, 3, "test", 10), true}
        }));
    }

    @Parameterized.Parameter(value = 0)
    public Contains contains;

    @Parameterized.Parameter(value = 1)
    public Object first;

    @Parameterized.Parameter(value = 2)
    public Collection collection;

    @Parameterized.Parameter(value = 3)
    public boolean expected;

    @Test
    public void shouldTest() {
        assertEquals(expected, contains.test(first, collection));
    }
}
