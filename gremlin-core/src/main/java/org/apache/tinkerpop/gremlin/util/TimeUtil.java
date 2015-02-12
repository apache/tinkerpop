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

import java.util.concurrent.TimeUnit;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TimeUtil {
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
}
