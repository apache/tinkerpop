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

import java.util.HashMap;
import java.util.Map;

public class DefaultAddVertexContract<L, K, V> implements AddVertexContract<L, K, V> {
    private final L label;
    private final Map<K, V> properties = new HashMap<>();

    public DefaultAddVertexContract(L label) {
        this.label = label;
    }

    @Override
    public void addProperty(K key, V value){
        properties.put(key, value);
    }

    @Override
    public L getLabel() {
        return label;
    }

    @Override
    public Map<K, V> getProperties() {
        return properties;
    }

    @Override
    public V removeProperty(final K key) {
        return properties.remove(key);
    }
}
