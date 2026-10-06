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

import java.util.OptionalLong;

/**
 * A provider's read-only view of a graph, from which a snapshot is built. A builder calls {@link #scanVertices} once,
 * then {@link #scanEdges} once and then {@link #scanVariables} once. A source must emit every element exactly once and
 * must emit the same graph version for all scans. Exceptions thrown by a sink or visitor must propagate out of the
 * scan unchanged.
 * <p/>
 * Builders assign codes in order of first appearance, so the scan order determines the output: vertex key codes, meta
 * key codes and variable key codes follow the vertex scan order, then the vertex-property order within a vertex, then
 * the meta-property order within a vertex property, and variables follow the {@link #scanVariables} order. A source
 * should therefore repeat the same order for the same graph version.
 */
public interface SnapshotSource extends AutoCloseable {

    /**
     * Identity and version of the source graph, recorded in the manifest and used to invalidate the snapshot.
     */
    SourceVersion version();

    /**
     * The number of vertices the scan will emit, if known. A builder fails if the emitted count differs.
     */
    default OptionalLong vertexCount() {
        return OptionalLong.empty();
    }

    /**
     * The number of edges the scan will emit, if known. A builder fails if the emitted count differs.
     */
    default OptionalLong edgeCount() {
        return OptionalLong.empty();
    }

    /**
     * The order in which {@link #scanEdges} emits edges.
     */
    default EdgeScanOrder edgeScanOrder() {
        return EdgeScanOrder.ARBITRARY;
    }

    /**
     * Emits every vertex exactly once to the sink.
     */
    void scanVertices(VertexSink sink);

    /**
     * Emits every edge exactly once to the sink.
     */
    void scanEdges(EdgeSink sink);

    /**
     * Emits every graph variable exactly once to the visitor, with a possibly null value. Builders call it once, after
     * {@link #scanEdges}. The default emits nothing.
     */
    default void scanVariables(PropertyVisitor visitor) {
    }

    /**
     * The defaults and cardinalities of the source graph, recorded in the manifest.
     */
    default SourceDefaults defaults() {
        return SourceDefaults.DEFAULT;
    }

    @Override
    default void close() {
    }
}
