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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.junit.Test;

/**
 * Janusgraph's JanusGraphVertexDeserializer uses the following code to deserialize a vertex.
 * It causes a lot NoSuchElementException when loading a huge graph, and hurts the performance.
 * See [TINKERPOP-2831].
 *
 *     public TinkerVertex getOrCreateVertex(final long vertexId, final String label, final TinkerGraph tg) {
 *         TinkerVertex v;
 *
 *         try {
 *             v = (TinkerVertex)tg.vertices(vertexId).next();
 *         } catch (NoSuchElementException e) {
 *             if (null != label) {
 *                 v = (TinkerVertex) tg.addVertex(T.label, label, T.id, vertexId);
 *             } else {
 *                 v = (TinkerVertex) tg.addVertex(T.id, vertexId);
 *             }
 *         }
 *
 *         return v;
 *     }
 */
public class TinkerGraphIteratorTest {
    @Test(expected = FastNoSuchElementException.class)
    public void shouldThrowFastNoSuchElementException() {
        TinkerGraph tg = TinkerGraph.open();
        TinkerVertex tv = (TinkerVertex)tg.vertices(1).next();
    }
}
