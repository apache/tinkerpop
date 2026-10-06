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
package org.apache.tinkerpop.gremlin.structure.snapshot.spi;

/**
 * The order in which a {@link SnapshotSource} emits edges.
 */
public enum EdgeScanOrder {

    /**
     * Edges arrive in no particular order.
     */
    ARBITRARY,

    /**
     * Edges arrive grouped by out-vertex, and the groups follow the order in which the vertices were emitted. A vertex
     * with no outgoing edges produces no group. Builders verify that out-vertex ordinals never decrease and fail if
     * they do.
     */
    GROUPED_BY_OUT_VERTEX
}
