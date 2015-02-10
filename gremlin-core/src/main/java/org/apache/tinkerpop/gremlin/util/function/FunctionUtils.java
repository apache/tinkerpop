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

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class FunctionUtils {
    private FunctionUtils() {
    }

    public static <T, U> Function<T, U> wrapFunction(final ThrowingFunction<T, U> functionThatThrows) {
        return (a) -> {
            try {
                return functionThatThrows.apply(a);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> Consumer<T> wrapConsumer(final ThrowingConsumer<T> consumerThatThrows) {
        return (a) -> {
            try {
                consumerThatThrows.accept(a);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T,U> BiConsumer<T,U> wrapBiConsumer(final ThrowingBiConsumer<T,U> consumerThatThrows) {
        return (a,b) -> {
            try {
                consumerThatThrows.accept(a,b);
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    public static <T> Supplier<T> wrapSupplier(final ThrowingSupplier<T> supplierThatThrows) {
        return () -> {
            try {
                return supplierThatThrows.get();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

}
