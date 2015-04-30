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
package org.apache.tinkerpop.gremlin.structure;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class CompareTest {

    @Parameterized.Parameters(name = "{0}.test({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        final List<Object> one = Arrays.asList(1, 1l, 1d, 1f, BigDecimal.ONE, BigInteger.ONE);
        final List<Object[]> testCases = new ArrayList<>(Arrays.asList(new Object[][]{
                {Compare.eq, null, null, true},
                {Compare.eq, null, 1, false},
                {Compare.eq, 1, null, false},
                {Compare.eq, "1", "1", true},
                {Compare.eq, 100, 99, false},
                {Compare.eq, 100, 101, false},
                {Compare.eq, "z", "a", false},
                {Compare.eq, "a", "z", false},
                {Compare.neq, null, null, false},
                {Compare.neq, null, 1, true},
                {Compare.neq, 1, null, true},
                {Compare.neq, "1", "1", false},
                {Compare.neq, 100, 99, true},
                {Compare.neq, 100, 101, true},
                {Compare.neq, "z", "a", true},
                {Compare.neq, "a", "z", true},
                {Compare.gt, null, null, false},
                {Compare.gt, null, 1, false},
                {Compare.gt, 1, null, false},
                {Compare.gt, "1", "1", false},
                {Compare.gt, 100, 99, true},
                {Compare.gt, 100, 101, false},
                {Compare.gt, "z", "a", true},
                {Compare.gt, "a", "z", false},
                {Compare.lt, null, null, false},
                {Compare.lt, null, 1, false},
                {Compare.lt, 1, null, false},
                {Compare.lt, "1", "1", false},
                {Compare.lt, 100, 99, false},
                {Compare.lt, 100, 101, true},
                {Compare.lt, "z", "a", false},
                {Compare.lt, "a", "z", true},
                {Compare.gte, null, null, true},
                {Compare.gte, null, 1, false},
                {Compare.gte, 1, null, false},
                {Compare.gte, "1", "1", true},
                {Compare.gte, 100, 99, true},
                {Compare.gte, 100, 101, false},
                {Compare.gte, "z", "a", true},
                {Compare.gte, "a", "z", false},
                {Compare.lte, null, null, true},
                {Compare.lte, null, 1, false},
                {Compare.lte, 1, null, false},
                {Compare.lte, "1", "1", true},
                {Compare.lte, 100, 99, false},
                {Compare.lte, 100, 101, true},
                {Compare.lte, "z", "a", false},
                {Compare.lte, "a", "z", true}
        }));
        for (int i = 0; i < one.size(); i++) {
            for (int j = 0; j < one.size(); j++) {
                testCases.add(new Object[]{Compare.eq, one.get(i), one.get(j), true});
                testCases.add(new Object[]{Compare.neq, one.get(i), one.get(j), false});
                testCases.add(new Object[]{Compare.gt, one.get(i), one.get(j), false});
                testCases.add(new Object[]{Compare.lt, one.get(i), one.get(j), false});
                testCases.add(new Object[]{Compare.gte, one.get(i), one.get(j), true});
                testCases.add(new Object[]{Compare.lte, one.get(i), one.get(j), true});
            }
        }
        return testCases;
    }

    @Parameterized.Parameter(value = 0)
    public Compare compare;

    @Parameterized.Parameter(value = 1)
    public Object first;

    @Parameterized.Parameter(value = 2)
    public Object second;

    @Parameterized.Parameter(value = 3)
    public boolean expected;

    @Test
    public void shouldTest() {
        assertEquals(expected, compare.test(first, second));
    }
}
