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
package org.apache.tinkerpop.gremlin.structure.util;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import static org.junit.Assert.*;

public class GraphHelperTest {

  @Test
  public void shouldCloneTinkergraph() {
    final TinkerGraph original = TinkerGraph.open();
    final TinkerGraph clone = TinkerGraph.open();

    final Vertex marko = original.addVertex("name", "marko", "age", 29);
    final Vertex stephen = original.addVertex("name", "stephen", "age", 35);
    marko.addEdge("knows", stephen);
    GraphHelper.cloneElements(original, clone);

    final Vertex michael = clone.addVertex("name", "michael");
    michael.addEdge("likes", marko);
    michael.addEdge("likes", stephen);
    clone.traversal().V().property("newProperty", "someValue").toList();
    clone.traversal().E().property("newProperty", "someValue").toList();

    assertEquals("original graph should be unchanged", new Long(2), original.traversal().V().count().next());
    assertEquals("original graph should be unchanged", new Long(1), original.traversal().E().count().next());
    assertEquals("original graph should be unchanged", new Long(0), original.traversal().V().has("newProperty").count().next());

    assertEquals("cloned graph should contain new elements", new Long(3), clone.traversal().V().count().next());
    assertEquals("cloned graph should contain new elements", new Long(3), clone.traversal().E().count().next());
    assertEquals("cloned graph should contain new property", new Long(3), clone.traversal().V().has("newProperty").count().next());
    assertEquals("cloned graph should contain new property", new Long(3), clone.traversal().E().has("newProperty").count().next());

    assertNotSame("cloned elements should reference to different objects",
        original.traversal().V().has("name", "stephen").next(),
        clone.traversal().V().has("name", "stephen").next());
  }

}
