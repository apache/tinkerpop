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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Holds the current partial match during DFS backtracking: scalar variable bindings in a flat
 * array and group (list) edge-variable bindings in a side map, both keyed by the plan's variable
 * index. {@link #snapshot()} produces an immutable {@link GqlRow} for emission.
 */
final class MatchState {

    private final Element[] scalars;
    private final Map<Integer, List<Edge>> groups;

    MatchState(final int variableCount) {
        this.scalars = new Element[variableCount];
        this.groups = new HashMap<>();
    }

    Element getScalar(final int i) {
        return scalars[i];
    }

    void setScalar(final int i, final Element e) {
        scalars[i] = e;
    }

    void clearScalar(final int i) {
        scalars[i] = null;
    }

    List<Edge> getGroup(final int i) {
        return groups.get(i);
    }

    void putGroup(final int i, final List<Edge> edges) {
        groups.put(i, edges);
    }

    void removeGroup(final int i) {
        groups.remove(i);
    }

    /**
     * Returns an immutable {@link GqlRow} snapshot of this state. The scalar array is cloned and
     * the group side map is copied so later backtracking mutations do not alias the emitted row.
     */
    GqlRow snapshot() {
        return new GqlRow(scalars.clone(),
                groups.isEmpty() ? Collections.emptyMap() : Map.copyOf(groups));
    }
}
