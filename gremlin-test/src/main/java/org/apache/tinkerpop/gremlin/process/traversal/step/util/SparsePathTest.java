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
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.apache.tinkerpop.gremlin.process.UseEngine;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

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
            assertEquals(Arrays.asList(Integer.valueOf(1)), path.getList("a"));
            assertEquals(Arrays.asList(Integer.valueOf(2)), path.getList("b"));
            assertEquals(Arrays.asList(Integer.valueOf(3)), path.getList("c"));
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
            assertEquals(Arrays.asList(Integer.valueOf(1)), path.getList("a"));
            assertEquals(Arrays.asList(Integer.valueOf(2)), path.getList("b"));
            assertEquals(Arrays.asList(Integer.valueOf(3)), path.getList("c"));
            assertEquals(Arrays.asList(Integer.valueOf(3)), path.getList("d"));
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
            // getList should return a list of values in path order.
            assertEquals(Arrays.asList("matthias"), path.getList("a"));  // List also contains "marko" in other Paths
        });

        /* TODO: Write a traversal that will use SparsePath, and show how it works in that context.
        final Path path = g.V().as("x").repeat(out().as("y")).times(2).path().by("name").next();
        assertEquals(3, path.size());
        assertEquals(3, path.labels().size());
        assertEquals(2, new HashSet<>(path.labels()).size());
        assertTrue(path.get("x") instanceof String);
        assertTrue(path.get("y") instanceof List);
        assertEquals(2, path.<List<String>>get("y").size());
        // get should return the list of both values, which, because of nondeterminism in the order in which paths are
        // traversed, will be one of two possible values.
        assertTrue(path.<List<String>>get("y").contains("josh"));
        assertTrue(path.<List<String>>get("y").contains("ripple") ||
                   path.<List<String>>get("y").contains("lop"));
        // getLast should return the most recent value (with the same nondeterminism caveat)
        assertTrue(path.getLast("y").equals("ripple") ||
                   path.getLast("y").equals("lop"));
        // getList should return both values (with the same nondeterminism caveat)
        assertTrue(path.getList("y").equals(Arrays.asList("josh", "ripple")) ||
                   path.getList("y").equals(Arrays.asList("josh", "lop")));
        */
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
