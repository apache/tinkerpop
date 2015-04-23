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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.IgnoreEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.*;

/**
 * SparsePath implements Path, but with different semantics, so PathTest does not apply.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@UseEngine(TraversalEngine.Type.STANDARD)
public class SparsePathTest extends AbstractGremlinProcessTest {

    @Test
    public void shouldHaveStandardSemanticsImplementedCorrectly() {
        Arrays.<Path>asList(SparsePath.make()).forEach(path -> {
            assertTrue(path.isSimple());
            assertEquals(0, path.size());
            path = path.extend(1, "a");
            path = path.extend(2, "b");
            path = path.extend(3, "c");
            assertEquals(3, path.size());
            assertEquals(Integer.valueOf(1), path.get("a"));
            assertEquals(Integer.valueOf(2), path.get("b"));
            assertEquals(Integer.valueOf(3), path.get("c"));
            assertEquals(Integer.valueOf(1), path.getLast("a"));
            assertEquals(Integer.valueOf(2), path.getLast("b"));
            assertEquals(Integer.valueOf(3), path.getLast("c"));
            // In other paths, getList should work, but SparsePath.getList throws.
            try {
                assertEquals(Arrays.asList(Integer.valueOf(1)), path.getList("a"));
                fail("SparsePath.getList should throw");
            }
            catch (IllegalArgumentException exc) {
                // OK
            }
            path.addLabel("d");
            assertEquals(4, path.size());  // 3 in other Paths
            assertEquals(Integer.valueOf(1), path.get("a"));
            assertEquals(Integer.valueOf(2), path.get("b"));
            assertEquals(Integer.valueOf(3), path.get("c"));
            assertEquals(Integer.valueOf(3), path.get("d"));
            assertEquals(Integer.valueOf(1), path.getLast("a"));
            assertEquals(Integer.valueOf(2), path.getLast("b"));
            assertEquals(Integer.valueOf(3), path.getLast("c"));
            assertEquals(Integer.valueOf(3), path.getLast("d"));
            assertTrue(path.hasLabel("a"));
            assertTrue(path.hasLabel("b"));
            assertTrue(path.hasLabel("c"));
            assertTrue(path.hasLabel("d"));
            assertFalse(path.hasLabel("e"));
            assertFalse(path.isSimple());  // true in other Paths
            path = path.extend(3, "e");
            assertFalse(path.isSimple());
            assertTrue(path.hasLabel("e"));
            assertEquals(5, path.size());  // 4 in other Paths
            assertEquals(Integer.valueOf(1), path.get(0));
            assertEquals(Integer.valueOf(2), path.get(1));
            assertEquals(Integer.valueOf(3), path.get(2));
            assertEquals(Integer.valueOf(3), path.get(3));
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    @IgnoreEngine(TraversalEngine.Type.COMPUTER)
    public void shouldHandleMultiLabelPaths() {
        Arrays.<Path>asList(SparsePath.make()).forEach(path -> {
            path = path.extend("marko", "a");
            path = path.extend("stephen", "b");
            path = path.extend("matthias", "a");
            assertEquals(2, path.size());  // 3 in other Paths
            assertEquals(2, path.objects().size());  // 3 in other Paths
            assertEquals(2, path.labels().size());  // 3 in other Paths
            assertEquals(2, new HashSet<>(path.labels()).size());
            assertTrue(path.get("a") instanceof String);  // List in other Paths
            assertTrue(path.get("b") instanceof String);
            assertEquals("matthias", path.get("a"));  // List containing "marko" and "matthias" in other Paths
            // getLast should return the most recent value.
            assertEquals("matthias", path.getLast("a"));
        });

        // As long as we avoid a PATH requirement, we will get a SparsePath.  This should use SparsePath in
        // TinkerGraph, but may not always do so due to implementation variations.
        final Traversal<Vertex, Map<String, String>> t =
            g.V().as("x").out().out().as("y").<String>select().by("name");

        assertTrue(t.hasNext());
        final Map<String, String> map = t.next();
        assertEquals(2, map.size());
        assertTrue(map.get("x") instanceof String);
        assertTrue(map.get("y") instanceof String);
        // Both of the paths start with marko
        assertEquals("marko", map.get("x"));
        // get should return the list of both values, which, because of nondeterminism in the order in which paths are
        // traversed, will be one of two possible values.
        assertTrue(map.get("y").equals("ripple") ||
                   map.get("y").equals("lop"));
    }

    @Test
    public void shouldExcludeUnlabeledLabelsFromPath() {
        Arrays.<Path>asList(SparsePath.make()).forEach(path -> {
            path = path.extend("marko", "a");
            path = path.extend("stephen", "b");
            path = path.extend("matthias", "c", "d");
            assertEquals(4, path.size());  // 3 in other Paths
            assertEquals(4, path.objects().size());  // 3 in other Paths
            assertEquals(4, path.labels().size());  // 3 in other Paths
            assertEquals(1, path.labels().get(0).size());
            assertEquals(1, path.labels().get(1).size());
            assertEquals("b", path.labels().get(1).iterator().next());
            assertEquals(1, path.labels().get(2).size());  // 2 in other Paths
            assertTrue(path.labels().get(2).contains("c"));
            assertTrue(path.labels().get(3).contains("d"));  // get(2) in other Paths
        });
    }
}
