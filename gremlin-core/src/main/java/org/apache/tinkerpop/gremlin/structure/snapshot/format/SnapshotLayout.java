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
package org.apache.tinkerpop.gremlin.structure.snapshot.format;

/**
 * Selects which segments are published. Builders always produce the structures they need internally; anything the
 * layout does not publish is written under scratch and deleted before publishing.
 */
public enum SnapshotLayout {

    /**
     * Adjacency offsets and neighbors in both directions.
     */
    TOPOLOGY,

    /**
     * Adds vertex and edge identifiers, labels, identifier indexes, edge endpoints, and adjacency edge ordinals. The
     * edge identifier index can be omitted with {@code BuildOptions.edgeIdIndex}.
     */
    IDENTITY,

    /**
     * Adds the property columns, including the vertex-property identifier columns.
     */
    FULL
}
