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
package org.apache.tinkerpop.gremlin.structure.util.batch.cache;

import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
abstract class AbstractIDVertexCache implements VertexCache {

    static final int INITIAL_CAPACITY = 1000;
    static final int INITIAL_TX_CAPACITY = 100;

    private final Map<Object, Object> map;
    private final Set<Object> mapKeysInCurrentTx;

    AbstractIDVertexCache() {
        map = new HashMap<>(INITIAL_CAPACITY);
        mapKeysInCurrentTx = new HashSet<>(INITIAL_TX_CAPACITY);
    }

    @Override
    public Object getEntry(final Object externalId) {
        return map.get(externalId);
    }

    @Override
    public void set(final Vertex vertex, final Object externalId) {
        setId(vertex, externalId);
    }

    @Override
    public void setId(final Object vertexId, final Object externalId) {
        map.put(externalId, vertexId);
        mapKeysInCurrentTx.add(externalId);
    }

    @Override
    public boolean contains(final Object externalId) {
        return map.containsKey(externalId);
    }

    @Override
    public void newTransaction() {
        for (Object id : mapKeysInCurrentTx) {
            Object o = map.get(id);
            assert null != o;
            if (o instanceof Vertex) {
                Vertex v = (Vertex) o;
                map.put(id, v.id());
            }
        }
        mapKeysInCurrentTx.clear();
    }
}
