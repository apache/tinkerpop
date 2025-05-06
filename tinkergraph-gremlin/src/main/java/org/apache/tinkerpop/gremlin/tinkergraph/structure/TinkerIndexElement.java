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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.HashMap;
import java.util.Map;

/**
 * A container object that holds an {@link Element} with a "distance" as determined by a vector index.
 */
public class TinkerIndexElement<T> {
    private final T element;
    private final float distance;

    public TinkerIndexElement(final T element, final float distance) {
        this.element = element;
        this.distance = distance;
    }

    public T getElement() {
        return element;
    }

    public float getDistance() {
        return distance;
    }

    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {{
            put("element", element);
            put("distance", distance);
        }};
    }
}