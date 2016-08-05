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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.process.remote.traversal.DefaultRemoteTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SideEffectIterator implements Iterator<Object> {

    private final Iterator<Object> sideEffectIterator;
    private final String sideEffectKey;
    private final String sideEffectAggregator;

    public SideEffectIterator(final Object sideEffect, final String sideEffectKey) {
        this.sideEffectKey = sideEffectKey;
        sideEffectAggregator = getAggregatorType(sideEffect);
        sideEffectIterator = sideEffect instanceof BulkSet ?
                new BulkSetIterator((BulkSet) sideEffect) :
                IteratorUtils.asIterator(sideEffect);
    }

    public String getSideEffectKey() {
        return sideEffectKey;
    }

    public String getSideEffectAggregator() {
        return sideEffectAggregator;
    }

    @Override
    public boolean hasNext() {
        return sideEffectIterator.hasNext();
    }

    @Override
    public Object next() {
        return sideEffectIterator.next();
    }

    private String getAggregatorType(final Object o) {
        if (o instanceof BulkSet)
            return Tokens.VAL_AGGREGATE_TO_BULKSET;
        else if (o instanceof Iterable || o instanceof Iterator)
            return Tokens.VAL_AGGREGATE_TO_LIST;
        else if (o instanceof Map)
            return Tokens.VAL_AGGREGATE_TO_MAP;
        else
            return Tokens.VAL_AGGREGATE_TO_NONE;
    }

    static class BulkSetIterator implements Iterator {

        private final Iterator<Map.Entry<Object,Long>> itty;

        public BulkSetIterator(final BulkSet<Object> bulkSet) {
            itty = bulkSet.asBulk().entrySet().iterator();
        }

        @Override
        public boolean hasNext() {
            return itty.hasNext();
        }

        @Override
        public Object next() {
            final Map.Entry<Object, Long> entry = itty.next();
            return new DefaultRemoteTraverser<>(entry.getKey(), entry.getValue());
        }
    }
}