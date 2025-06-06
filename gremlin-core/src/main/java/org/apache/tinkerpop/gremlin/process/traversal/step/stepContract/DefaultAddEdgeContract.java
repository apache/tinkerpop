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
package org.apache.tinkerpop.gremlin.process.traversal.step.stepContract;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultAddEdgeContract<L, Vertex, K, Value> implements AddEdgeContract<L, Vertex, K, Value> {
    private L label;
    private Vertex from;
    private Vertex to;
    private Map<K, Value> properties = new HashMap<>();

    public DefaultAddEdgeContract(final L label) {
        this.label = label;
    }

    @Override
    public L getLabel() {
        return label;
    }

    @Override
    public Vertex getFrom() {
        return null;
    }

    @Override
    public Vertex getTo() {
        return null;
    }

    public void addFrom(final Vertex from) {
        this.from = from;
    }

    public void addTo(final Vertex to) {
        this.to = to;
    }

    @Override
    public Map<K, Value> getProperties() {
        return Collections.unmodifiableMap(properties);
    }

    public void addProperty(final K key, final Value value){
        properties.put(key, value);
    }

    @Override
    public Value removeProperty(final K key) {
        return properties.remove(key);
    }
}
