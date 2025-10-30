/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tinkerpop.example.cmdb.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class MappingUtils {
    private MappingUtils() {
    }

    @SuppressWarnings("unchecked")
    public static List<List<Map<String, Object>>> toPathOfValueMaps(List<Path> paths) {
        List<List<Map<String, Object>>> out = new ArrayList<>();
        for (Path p : paths) {
            List<Map<String, Object>> row = new ArrayList<>();
            for (Object o : p.objects()) {
                if (o instanceof Map) {
                    Map<?, ?> m = (Map<?, ?>) o;
                    Map<String, Object> as = new HashMap<>();
                    for (Map.Entry<?, ?> e : m.entrySet()) {
                        as.put(String.valueOf(e.getKey()), e.getValue());
                    }
                    row.add(as);
                } else {
                    Map<String, Object> wrap = new HashMap<>();
                    wrap.put("value", o);
                    row.add(wrap);
                }
            }
            out.add(row);
        }
        return out;
    }
}
