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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class MapHelper {

    private MapHelper() {
    }

    public static <T> void incr(final Map<T, Long> map, final T key, final Long value) {
        map.put(key, value + map.getOrDefault(key, 0l));
    }

    public static <T> void incr(final Map<T, Double> map, final T key, final Double value) {
        map.put(key, value + map.getOrDefault(key, 0.0d));
    }

    public static <T, U> void incr(final Map<T, List<U>> map, final T key, final U value) {
        map.compute(key, (k, v) -> {
            if (null == v) v = new ArrayList<>();
            v.add(value);
            return v;
        });
    }
}
