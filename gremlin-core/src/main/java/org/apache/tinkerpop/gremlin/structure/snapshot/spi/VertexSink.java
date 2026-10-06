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

import java.util.List;

/**
 * Receives vertices from {@link SnapshotSource#scanVertices}. The arguments are valid only during the call and must
 * not be retained.
 */
public interface VertexSink {

    /**
     * Receives one vertex.
     *
     * @param id         the vertex identifier, which must be a scalar value
     * @param labels     the vertex labels in source order, possibly empty. The list is valid only during the call.
     * @param properties the vertex properties, which are elements with identifiers and meta-properties
     */
    void vertex(Object id, List<String> labels, VertexPropertySource properties);
}
