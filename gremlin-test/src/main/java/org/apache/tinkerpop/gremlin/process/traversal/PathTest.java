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
package org.apache.tinkerpop.gremlin.process.traversal;

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest extends AbstractGremlinProcessTest {

    private final static List<Supplier<Path>> PATH_SUPPLIERS =
            Arrays.asList(MutablePath::make, ImmutablePath::make, DetachedPath::make, ReferencePath::make);

    @Test
    public void shouldHaveStandardSemanticsImplementedCorrectly() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            assertTrue(path.isSimple());
            assertEquals(0, path.size());
            path = path.extend(1, Collections.singleton("a"));
            path = path.extend(2, Collections.singleton("b"));
            path = path.extend(3, Collections.singleton("c"));
            assertEquals(3, path.size());
            assertEquals(Integer.valueOf(1), path.get("a"));
            assertEquals(Integer.valueOf(2), path.get("b"));
            assertEquals(Integer.valueOf(3), path.get("c"));
            path.addLabel("d");
            assertEquals(3, path.size());
            assertEquals(Integer.valueOf(1), path.get("a"));
            assertEquals(Integer.valueOf(2), path.get("b"));
            assertEquals(Integer.valueOf(3), path.get("c"));
            assertEquals(Integer.valueOf(3), path.get("d"));
            assertTrue(path.hasLabel("a"));
            assertTrue(path.hasLabel("b"));
            assertTrue(path.hasLabel("c"));
            assertTrue(path.hasLabel("d"));
            assertFalse(path.hasLabel("e"));
            assertTrue(path.isSimple());
            path = path.extend(3, Collections.singleton("e"));
            assertFalse(path.isSimple());
            assertTrue(path.hasLabel("e"));
            assertEquals(4, path.size());
            assertEquals(Integer.valueOf(1), path.get(0));
            assertEquals(Integer.valueOf(2), path.get(1));
            assertEquals(Integer.valueOf(3), path.get(2));
            assertEquals(Integer.valueOf(3), path.get(3));
        });
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldHandleMultiLabelPaths() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", Collections.singleton("a"));
            path = path.extend("stephen", Collections.singleton("b"));
            path = path.extend("matthias", Collections.singleton("a"));
            assertEquals(3, path.size());
            assertEquals(3, path.objects().size());
            assertEquals(3, path.labels().size());
            assertEquals(2, new HashSet<>(path.labels()).size());
            assertTrue(path.get("a") instanceof List);
            assertTrue(path.get("b") instanceof String);
            assertEquals(2, path.<List<String>>get("a").size());
            assertTrue(path.<List<String>>get("a").contains("marko"));
            assertTrue(path.<List<String>>get("a").contains("matthias"));
        });

        final Path path = g.V().as("x").repeat(out().as("y")).times(2).path().by("name").next();
        assertEquals(3, path.size());
        assertEquals(3, path.labels().size());
        assertEquals(2, new HashSet<>(path.labels()).size());
        assertTrue(path.get("x") instanceof String);
        assertTrue(path.get("y") instanceof List);
        assertEquals(2, path.<List<String>>get("y").size());
        assertTrue(path.<List<String>>get("y").contains("josh"));
        assertTrue(path.<List<String>>get("y").contains("ripple") || path.<List<String>>get("y").contains("lop"));
    }

    @Test
    public void shouldExcludeUnlabeledLabelsFromPath() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", "a");
            path = path.extend("stephen", "b");
            path = path.extend("matthias", "c", "d");
            assertEquals(3, path.size());
            assertEquals(3, path.objects().size());
            assertEquals(3, path.labels().size());
            assertEquals(1, path.labels().get(0).size());
            assertEquals(1, path.labels().get(1).size());
            assertEquals("b", path.labels().get(1).iterator().next());
            assertEquals(2, path.labels().get(2).size());
            assertTrue(path.labels().get(2).contains("c"));
            assertTrue(path.labels().get(2).contains("d"));
        });
    }

    @Test
    public void shouldHaveOrderedPathLabels() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", "a", "b");
            path = path.extend("stephen", "c", "a");
            path = path.extend("matthias", "a", "b");
            assertEquals(3, path.size());
            assertEquals(3, path.objects().size());
            assertEquals(3, path.labels().size());
            assertEquals(2, path.labels().get(0).size());
            assertEquals(2, path.labels().get(1).size());
            assertEquals(2, path.labels().get(2).size());
            ///
            Iterator<String> labels = path.labels().get(0).iterator();
            assertEquals("a", labels.next());
            assertEquals("b", labels.next());
            assertFalse(labels.hasNext());
            labels = path.labels().get(1).iterator();
            assertEquals("c", labels.next());
            assertEquals("a", labels.next());
            assertFalse(labels.hasNext());
            labels = path.labels().get(2).iterator();
            assertEquals("a", labels.next());
            assertEquals("b", labels.next());
            assertFalse(labels.hasNext());
            ///
            List<String> names = path.get("a");
            assertEquals("marko", names.get(0));
            assertEquals("stephen", names.get(1));
            assertEquals("matthias", names.get(2));
            names = path.get("b");
            assertEquals("marko", names.get(0));
            assertEquals("matthias", names.get(1));
            assertEquals("stephen", path.get("c"));
        });
    }

    @Test
    public void shouldSelectSingleCorrectly() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", "a", "b");
            path = path.extend("stephen", "a", "c");
            path = path.extend("matthias", "c", "d");
            assertEquals(3, path.size());
            assertEquals("marko", path.getSingle(Pop.head, "a"));
            assertEquals("marko", path.getSingle(Pop.head, "b"));
            assertEquals("stephen", path.getSingle(Pop.head, "c"));
            assertEquals("matthias", path.getSingle(Pop.head, "d"));
            ///
            assertEquals("marko", path.getSingle(Pop.tail, "b"));
            assertEquals("stephen", path.getSingle(Pop.tail, "a"));
            assertEquals("matthias", path.getSingle(Pop.tail, "c"));
            assertEquals("matthias", path.getSingle(Pop.tail, "d"));
        });
    }

    @Test
    public void shouldSelectListCorrectly() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", "a", "b");
            path = path.extend("stephen", "a", "c");
            path = path.extend("matthias", "c", "d");
            assertEquals(3, path.size());
            assertEquals(2, path.getList("a").size());
            assertEquals("marko", path.getList("a").get(0));
            assertEquals("stephen", path.getList("a").get(1));
            assertEquals(1, path.getList("b").size());
            assertEquals("marko", path.getList("b").get(0));
            assertEquals(2, path.getList("c").size());
            assertEquals("stephen", path.getList("c").get(0));
            assertEquals("matthias", path.getList("c").get(1));
            assertEquals(1, path.getList("d").size());
            assertEquals("matthias", path.getList("d").get(0));
            ///
            assertEquals(0,path.getList("noExist").size());
        });
    }
}
