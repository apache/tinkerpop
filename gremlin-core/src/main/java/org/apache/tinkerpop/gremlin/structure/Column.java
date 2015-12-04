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

package org.apache.tinkerpop.gremlin.structure;

import org.apache.tinkerpop.gremlin.process.traversal.Path;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.function.Function;

/**
 * Column references a particular type of column in a complex data structure such as a {@link java.util.Map}, a {@link java.util.Map.Entry}, or a {@link org.apache.tinkerpop.gremlin.process.traversal.Path}.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public enum Column implements Function<Object, Object> {

    /**
     * The keys associated with the data structure.
     */
    keys {
        @Override
        public Object apply(final Object object) {
            if (object instanceof Map)
                return new LinkedHashSet<>(((Map<?,?>) object).keySet());
            else if (object instanceof Map.Entry)
                return ((Map.Entry) object).getKey();
            else if (object instanceof Path)
                return new ArrayList<>(((Path) object).labels());
            else
                throw new IllegalArgumentException("The provided object does not have accessible keys: " + object.getClass());
        }
    },
    /**
     * The values associated with the data structure.
     */
    values {
        @Override
        public Object apply(final Object object) {
            if (object instanceof Map)
                return new ArrayList<>(((Map<?,?>) object).values());
            else if (object instanceof Map.Entry)
                return ((Map.Entry) object).getValue();
            else if (object instanceof Path)
                return new ArrayList<>(((Path) object).objects());
            else
                throw new IllegalArgumentException("The provided object does not have accessible keys: " + object.getClass());
        }
    }


}
