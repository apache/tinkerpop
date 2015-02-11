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
package com.apache.tinkerpop.gremlin.structure.util.batch.cache;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.carrotsearch.hppc.procedures.LongProcedure;
import com.apache.tinkerpop.gremlin.structure.Vertex;

/**
 * @author Matthias Broecheler (http://www.matthiasb.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class LongIDVertexCache implements VertexCache {

    private final LongObjectMap<Object> map;
    private final LongArrayList mapKeysInCurrentTx;
    private final LongProcedure newTransactionProcedure;

    public LongIDVertexCache() {
        map = new LongObjectOpenHashMap<>(AbstractIDVertexCache.INITIAL_CAPACITY);
        mapKeysInCurrentTx = new LongArrayList(AbstractIDVertexCache.INITIAL_TX_CAPACITY);
        newTransactionProcedure = new VertexConverterLP();
    }

    private static long getID(final Object externalID) {
        if (!(externalID instanceof Number)) throw new IllegalArgumentException("Number expected.");
        return ((Number) externalID).longValue();
    }

    @Override
    public Object getEntry(final Object externalId) {
        return map.get(getID(externalId));
    }

    @Override
    public void set(final Vertex vertex, final Object externalId) {
        setId(vertex, externalId);
    }

    @Override
    public void setId(final Object vertexId, final Object externalId) {
        final long id = getID(externalId);
        map.put(id, vertexId);
        mapKeysInCurrentTx.add(id);
    }

    @Override
    public boolean contains(final Object externalId) {
        return map.containsKey(getID(externalId));
    }

    @Override
    public void newTransaction() {
        mapKeysInCurrentTx.forEach(newTransactionProcedure);
        mapKeysInCurrentTx.clear();
    }

    /**
     * See {@link LongIDVertexCache#newTransaction()}
     */
    private class VertexConverterLP implements LongProcedure {
        /**
         * Retrieve the Object associated with each long from {@code map}. If it
         * is an {@code instanceof Vertex}, then replace it in the map with
         * {@link Vertex#id()}. Otherwise, do nothing.
         */
        @Override
        public void apply(final long l) {
            final Object o = map.get(l);
            assert null != o;
            if (o instanceof Vertex) {
                map.put(l, ((Vertex) o).id());
            }
        }
    }
}