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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Eduard Tudenhoefner
 */
@RunWith(Parameterized.class)
public class CompareExceptionTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Parameterized.Parameters(name = "{0}.test({1},{2}) = {3}")
    public static Iterable<Object[]> dataThatThrowsException() {
        final List<Object[]> testCases = new ArrayList<>(Arrays.asList(new Object[][]{
                {Compare.gt, new Object(), new Object(), IllegalArgumentException.class},
                {Compare.lt, new Object(), new Object(), IllegalArgumentException.class},
                {Compare.gte, new Object(), new Object(), IllegalArgumentException.class},
                {Compare.lte, new Object(), new Object(), IllegalArgumentException.class},
                {Compare.gt, 23, "23", IllegalArgumentException.class},
                {Compare.gte, 23, "23", IllegalArgumentException.class},
                {Compare.lt, 23, "23", IllegalArgumentException.class},
                {Compare.lte, 23, "23", IllegalArgumentException.class},
                {Compare.lte, new CompareTest.A(), new CompareTest.B(), IllegalArgumentException.class},
                {Compare.lte, new CompareTest.B(), new CompareTest.A(), IllegalArgumentException.class},
                {Compare.lte, new CompareTest.C(), new CompareTest.D(), IllegalArgumentException.class},
        }));
        return testCases;
    }

    @Parameterized.Parameter(value = 0)
    public Compare compare;

    @Parameterized.Parameter(value = 1)
    public Object first;

    @Parameterized.Parameter(value = 2)
    public Object second;

    @Parameterized.Parameter(value = 3)
    public Class<? extends Throwable> expectedException;

    @Test
    public void shouldThrowException() {
        exceptionRule.expect(expectedException);
        exceptionRule.expectMessage("as both need to be an instance of Number or Comparable (and of the same type)");
        compare.test(first, second);
    }
}
