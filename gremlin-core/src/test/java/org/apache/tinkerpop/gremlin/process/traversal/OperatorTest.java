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

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class OperatorTest {
    @Parameterized.Parameters(name = "{0}({1},{2}) = {3}")
    public static Iterable<Object[]> data() {
        return new ArrayList<>(Arrays.asList(new Object[][]{
                {Operator.div, 10, 2, 5},
                {Operator.div, 10l, 2l, 5l},
                {Operator.div, 10f, 2f, 5f},
                {Operator.div, 10d, 2d, 5d},
                {Operator.max, 10, 2, 10},
                {Operator.max, 10l, 2l, 10l},
                {Operator.max, 10f, 2f, 10f},
                {Operator.max, 10d, 2d, 10d},
                {Operator.max, 2, 10, 10},
                {Operator.max, 2l, 10l, 10l},
                {Operator.max, 2f, 10f, 10f},
                {Operator.max, 2d, 10d, 10d},
                {Operator.min, 10, 2, 2},
                {Operator.min, 10l, 2l, 2l},
                {Operator.min, 10f, 2f, 2f},
                {Operator.min, 10d, 2d, 2d},
                {Operator.min, 2, 10, 2},
                {Operator.min, 2l, 10l, 2l},
                {Operator.min, 2f, 10f, 2f},
                {Operator.min, 2d, 10d, 2d},
                {Operator.minus, 10, 2, 8},
                {Operator.minus, 10l, 2l, 8l},
                {Operator.minus, 10f, 2f, 8f},
                {Operator.minus, 10d, 2d, 8d},
                {Operator.mult, 5, 4, 20},
                {Operator.mult, 5l, 4l, 20l},
                {Operator.mult, 5f, 4f, 20f},
                {Operator.mult, 5d, 4d, 20d},
                {Operator.sum, 7, 3, 10},
                {Operator.sum, 7l, 3l, 10l},
                {Operator.sum, 7f, 3f, 10f},
                {Operator.sum, 7d, 3d, 10d}
        }));
    }

    @Parameterized.Parameter(value = 0)
    public Operator operator;

    @Parameterized.Parameter(value = 1)
    public Number a;

    @Parameterized.Parameter(value = 2)
    public Number b;

    @Parameterized.Parameter(value = 3)
    public Number expected;

    @Test
    public void shouldApplyOperator() {
        assertEquals(expected, operator.apply(a, b));
    }
}
