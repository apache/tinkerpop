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

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import org.apache.tinkerpop.gremlin.TestHelper;
import org.junit.Test;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class FunctionUtilsTest {

    @Test
    public void shouldBeUtilityClass() throws Exception {
        TestHelper.assertIsUtilityClass(FunctionUtils.class);
    }

    @Test
    public void shouldWrapInRuntimeIfFunctionThrows() {
        final Exception t = new Exception();
        try {
            FunctionUtils.wrapFunction(FunctionUtilsTest::throwIt).apply(t);
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertSame(t, ex.getCause());
        }
    }

    @Test
    public void shouldNoReWrapInRuntimeIfFunctionThrows() {
        final Exception t = new RuntimeException();
        try {
            FunctionUtils.wrapFunction(FunctionUtilsTest::throwIt).apply(t);
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertNull(ex.getCause());
        }
    }

    @Test
    public void shouldWrapInRuntimeIfConsumerThrows() {
        final Exception t = new Exception();
        try {
            FunctionUtils.wrapConsumer((Exception a) -> {
                throw a;
            }).accept(t);
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertSame(t, ex.getCause());
        }
    }

    @Test
    public void shouldNoReWrapInRuntimeIfConsumerThrows() {
        final Exception t = new RuntimeException();
        try {
            FunctionUtils.wrapConsumer((Exception a) -> {
                throw a;
            }).accept(t);
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertNull(ex.getCause());
        }
    }

    @Test
    public void shouldWrapInRuntimeIfBiConsumerThrows() {
        final Exception t = new Exception();
        try {
            FunctionUtils.wrapBiConsumer((Exception a, String x) -> {
                throw a;
            }).accept(t, "test");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertSame(t, ex.getCause());
        }
    }

    @Test
    public void shouldNoReWrapInRuntimeIfBiConsumerThrows() {
        final Exception t = new RuntimeException();
        try {
            FunctionUtils.wrapBiConsumer((Exception a, String x) -> {
                throw a;
            }).accept(t, "test");
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertNull(ex.getCause());
        }
    }

    @Test
    public void shouldWrapInRuntimeIfSupplierThrows() {
        final Exception t = new Exception();
        try {
            FunctionUtils.wrapSupplier(() -> {
                throw t;
            }).get();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertSame(t, ex.getCause());
        }
    }

    @Test
    public void shouldNoReWrapInRuntimeIfSupplierThrows() {
        final Exception t = new RuntimeException();
        try {
            FunctionUtils.wrapSupplier(() -> {
                throw t;
            }).get();
        } catch (Exception ex) {
            assertThat(ex, instanceOf(RuntimeException.class));
            assertNull(ex.getCause());
        }
    }

    static int throwIt(final Exception t) throws Exception {
        throw t;
    }
}
