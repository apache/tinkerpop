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
package org.apache.tinkerpop.gremlin.util;

import org.javatuples.Pair;

import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public final class TimeUtil {

    private TimeUtil() {
    }

    public static long secondsSince(final long startNanos) {
        return timeSince(startNanos, TimeUnit.SECONDS);
    }

    public static long millisSince(final long startNanos) {
        return timeSince(startNanos, TimeUnit.MILLISECONDS);
    }

    public static long minutesSince(final long startNanos) {
        return timeSince(startNanos, TimeUnit.MINUTES);
    }

    public static long timeSince(final long startNanos, final TimeUnit destUnit) {
        return destUnit.convert(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
    }

    public static double clock(final Runnable runnable) {
        return clock(100, runnable);
    }

    public static double clock(final int loops, final Runnable runnable) {
        runnable.run(); // warm-up
        return IntStream.range(0, loops).mapToDouble(i -> {
            long t = System.nanoTime();
            runnable.run();
            return (System.nanoTime() - t) * 0.000001;
        }).sum() / loops;
    }

    public static <S> Pair<Double, S> clockWithResult(final Supplier<S> supplier) {
        return clockWithResult(100, supplier);
    }

    public static <S> Pair<Double, S> clockWithResult(final int loops, final Supplier<S> supplier) {
        final S result = supplier.get(); // warm up
        return Pair.with(IntStream.range(0, loops).mapToDouble(i -> {
            long t = System.nanoTime();
            @SuppressWarnings("unused")
            final S ignored = supplier.get();
            return (System.nanoTime() - t) * 0.000001;
        }).sum() / loops, result);
    }
}
