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
package org.apache.tinkerpop.gremlin.util.function;

import org.junit.Test;

import java.util.function.UnaryOperator;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TriFunctionTest {
    @Test
    public void shouldApplyCurrentFunctionAndThenAnotherSuppliedOne() {
        final TriFunction<String, String, String, String> f = (a, b, c) -> a + b + c;
        final UnaryOperator<String> after = (s) -> s + "last";
        assertEquals("123last", f.andThen(after).apply("1", "2", "3"));
    }

    @Test(expected = NullPointerException.class)
    public void shouldThrowIfAfterFunctionIsNull() {
        final TriFunction<String, String, String, String> f = (a, b, c) -> a + b + c;
        f.andThen(null);
    }
}
