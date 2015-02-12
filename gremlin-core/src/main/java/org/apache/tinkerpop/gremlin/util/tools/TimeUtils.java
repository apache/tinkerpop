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
package com.tinkerpop.gremlin.util.tools;

import java.util.stream.IntStream;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TimeUtils {

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
}
