/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.snapshot.process.op.spill;

import org.apache.tinkerpop.gremlin.structure.snapshot.process.exec.MemoryBudget;

import java.util.HashMap;
import java.util.Map;

/**
 * The map a group or groupCount step returns. It cannot be spilled, so its entries are reserved from the budget as they
 * are added, and a result that is larger than the budget fails with the budget exception rather than with an
 * {@code OutOfMemoryError}.
 */
final class ResultMap {

    private final MemoryBudget budget;
    private final String owner;
    private Map<Object, Object> map = new HashMap<>();

    ResultMap(final MemoryBudget budget, final String owner) {
        this.budget = budget;
        this.owner = owner;
    }

    void put(final Object key, final Object value) {
        budget.reserve(SpillSupport.RESULT_ENTRY + SpillSupport.estimate(key) + SpillSupport.estimate(value), owner);
        map.put(key, value);
    }

    /**
     * Hands the map to the consumer. The consumer keeps it after this operator is reset or closed, so clear() must not
     * empty it; the budget stays reserved until then.
     */
    Map<Object, Object> handOff() {
        final Map<Object, Object> handed = map;
        map = new HashMap<>();
        return handed;
    }

    void clear() {
        map.clear();
        budget.releaseAll(owner);
    }
}
