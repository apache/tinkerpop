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

import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutablePath;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedPath;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PathTest {

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
            path = path.extend(Collections.singleton("d"));
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
            Path retractedPath = path.retract(Collections.singleton("f")).clone();
            assertFalse(path.hasLabel("f"));
            assertEquals(4, path.size());
            assertEquals(retractedPath, path);
            path = path.retract(Collections.singleton("b"));
            assertFalse(path.hasLabel("b"));
            assertEquals(3, path.size());
            assertEquals(retractedPath.retract(Collections.singleton("b")), path);
            path = path.retract(Collections.singleton("a"));
            assertEquals(2, path.size());
            assertFalse(path.hasLabel("a"));
            assertTrue(path.hasLabel("d"));
            path = path.retract(new HashSet<>(Arrays.asList("c", "d")));
            assertFalse(path.hasLabel("c"));
            assertFalse(path.hasLabel("d"));
            assertTrue(path.hasLabel("e"));
            assertEquals(1, path.size());
            path = path.retract(Collections.singleton("e"));
            assertFalse(path.hasLabel("c"));
            assertFalse(path.hasLabel("d"));
            assertFalse(path.hasLabel("e"));
            assertNotEquals(retractedPath, path);
            assertEquals(0, path.size());
        });
    }

    @Test
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
    }

    @Test
    public void shouldExcludeUnlabeledLabelsFromPath() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", Collections.singleton("a"));
            path = path.extend("stephen", Collections.singleton("b"));
            path = path.extend("matthias", new LinkedHashSet<>(Arrays.asList("c", "d")));
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
            path = path.extend("marko", new LinkedHashSet<>(Arrays.asList("a", "b")));
            path = path.extend("stephen", new LinkedHashSet<>(Arrays.asList("c", "a")));
            path = path.extend("matthias", new LinkedHashSet<>(Arrays.asList("a", "b")));
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
            path = path.extend("marko", new LinkedHashSet<String>(Arrays.asList("a", "b")));
            path = path.extend("stephen", new LinkedHashSet<>(Arrays.asList("a", "c")));
            path = path.extend("matthias", new LinkedHashSet<>(Arrays.asList("c", "d")));
            assertEquals(3, path.size());
            assertEquals("marko", path.get(Pop.first, "a"));
            assertEquals("marko", path.get(Pop.first, "b"));
            assertEquals("stephen", path.get(Pop.first, "c"));
            assertEquals("matthias", path.get(Pop.first, "d"));
            ///
            assertEquals("marko", path.get(Pop.last, "b"));
            assertEquals("stephen", path.get(Pop.last, "a"));
            assertEquals("matthias", path.get(Pop.last, "c"));
            assertEquals("matthias", path.get(Pop.last, "d"));
        });
    }

    @Test
    public void shouldSelectListCorrectly() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path path = supplier.get();
            path = path.extend("marko", new LinkedHashSet<>(Arrays.asList("a", "b")));
            path = path.extend("stephen", new LinkedHashSet<>(Arrays.asList("a", "c")));
            path = path.extend("matthias", new LinkedHashSet<>(Arrays.asList("c", "d")));
            assertEquals(3, path.size());
            assertEquals(2, path.<List>get(Pop.all, "a").size());
            assertEquals("marko", path.<List>get(Pop.all, "a").get(0));
            assertEquals("stephen", path.<List>get(Pop.all, "a").get(1));
            assertEquals(1, path.<List>get(Pop.all, "b").size());
            assertEquals("marko", path.<List>get(Pop.all, "b").get(0));
            assertEquals(2, path.<List>get(Pop.all, "c").size());
            assertEquals("stephen", path.<List>get(Pop.all, "c").get(0));
            assertEquals("matthias", path.<List>get(Pop.all, "c").get(1));
            assertEquals(1, path.<List>get(Pop.all, "d").size());
            assertEquals("matthias", path.<List>get(Pop.all, "d").get(0));
            ///
            assertEquals(0, path.<List>get(Pop.all, "noExist").size());
        });
    }

    @Test
    public void shouldHaveEquality() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path pathA1 = supplier.get();
            Path pathA2 = supplier.get();
            Path pathB1 = supplier.get();
            Path pathB2 = supplier.get();
            assertEquals(pathA1, pathA2);
            assertEquals(pathA1.hashCode(), pathA2.hashCode());
            assertEquals(pathA2, pathB1);
            assertEquals(pathA2.hashCode(), pathB1.hashCode());
            assertEquals(pathB1, pathB2);
            assertEquals(pathB1.hashCode(), pathB2.hashCode());
            ///
            pathA1 = pathA1.extend("marko", Collections.singleton("a"));
            pathA2 = pathA2.extend("marko", Collections.singleton("a"));
            pathB1 = pathB1.extend("marko", Collections.singleton("b"));
            pathB2 = pathB2.extend("marko", Collections.singleton("b"));
            assertEquals(pathA1, pathA2);
            assertEquals(pathA1.hashCode(), pathA2.hashCode());
            assertNotEquals(pathA2, pathB1);
            assertEquals(pathB1, pathB2);
            assertEquals(pathB1.hashCode(), pathB2.hashCode());
            ///
            pathA1 = pathA1.extend("daniel", new LinkedHashSet<>(Arrays.asList("aa", "aaa")));
            pathA2 = pathA2.extend("daniel", new LinkedHashSet<>(Arrays.asList("aa", "aaa")));
            pathB1 = pathB1.extend("stephen", new LinkedHashSet<>(Arrays.asList("bb", "bbb")));
            pathB2 = pathB2.extend("stephen", Collections.singleton("bb"));
            assertEquals(pathA1, pathA2);
            assertEquals(pathA1.hashCode(), pathA2.hashCode());
            assertNotEquals(pathA2, pathB1);
            assertNotEquals(pathB1, pathB2);
            ///
            pathA1 = pathA1.extend("matthias", new LinkedHashSet<>(Arrays.asList("aaaa", "aaaaa")));
            pathA2 = pathA2.extend("bob", new LinkedHashSet<>(Arrays.asList("aaaa", "aaaaa")));
            pathB1 = pathB1.extend("byn", Collections.singleton("bbbb"));
            pathB2 = pathB2.extend("bryn", Collections.singleton("bbbb"));
            assertNotEquals(pathA1, pathA2);
            assertNotEquals(pathA2, pathB1);
            assertNotEquals(pathB1, pathB2);
        });
    }

    @Test
    public void shouldHavePopEquality() {
        PATH_SUPPLIERS.forEach(supplier -> {
            Path pathA1 = supplier.get();
            Path pathA2 = supplier.get();
            Path pathB1 = supplier.get();
            Path pathB2 = supplier.get();
            assertTrue(pathA1.popEquals(Pop.all, pathA2));
            assertTrue(pathA2.popEquals(Pop.all, pathB1));
            assertTrue(pathB1.popEquals(Pop.all, pathB2));
            assertTrue(pathA1.popEquals(Pop.first, pathA2));
            assertTrue(pathA2.popEquals(Pop.first, pathB1));
            assertTrue(pathB1.popEquals(Pop.first, pathB2));
            assertTrue(pathA1.popEquals(Pop.last, pathA2));
            assertTrue(pathA2.popEquals(Pop.last, pathB1));
            assertTrue(pathB1.popEquals(Pop.last, pathB2));

            ///
            pathA1 = pathA1.extend("marko", Collections.singleton("a"));
            pathA2 = pathA2.extend("marko", Collections.singleton("a"));
            pathB1 = pathB1.extend("matthias", Collections.singleton("a"));
            pathB2 = pathB2.extend("matthias", Collections.singleton("a"));
            assertTrue(pathA1.popEquals(Pop.all, pathA2));
            assertFalse(pathA2.popEquals(Pop.all, pathB1));
            assertTrue(pathB1.popEquals(Pop.all, pathB2));
            assertTrue(pathA1.popEquals(Pop.first, pathA2));
            assertFalse(pathA2.popEquals(Pop.first, pathB1));
            assertTrue(pathB1.popEquals(Pop.first, pathB2));
            assertTrue(pathA1.popEquals(Pop.last, pathA2));
            assertFalse(pathA2.popEquals(Pop.last, pathB1));
            assertTrue(pathB1.popEquals(Pop.last, pathB2));

            ///
            pathA1 = pathA1.extend("matthias", Collections.singleton("a"));
            pathA2 = pathA2.extend("matthias", Collections.singleton("a"));
            pathB1 = pathB1.extend("marko", Collections.singleton("a"));
            pathB2 = pathB2.extend("marko", Collections.singleton("a"));
            assertTrue(pathA1.popEquals(Pop.all, pathA2));
            assertFalse(pathA2.popEquals(Pop.all, pathB1));
            assertTrue(pathB1.popEquals(Pop.all, pathB2));
            assertTrue(pathA1.popEquals(Pop.first, pathA2));
            assertFalse(pathA2.popEquals(Pop.first, pathB1));
            assertTrue(pathB1.popEquals(Pop.first, pathB2));
            assertTrue(pathA1.popEquals(Pop.last, pathA2));
            assertFalse(pathA2.popEquals(Pop.last, pathB1));
            assertTrue(pathB1.popEquals(Pop.last, pathB2));

            ///
            pathA1 = pathA1.extend("bob", Collections.singleton("a"));
            pathA2 = pathA2.extend("bob", Collections.singleton("a"));
            pathB1 = pathB1.extend("bob", Collections.singleton("a"));
            pathB2 = pathB2.extend("bob", Collections.singleton("a"));
            assertTrue(pathA1.popEquals(Pop.all, pathA2));
            assertFalse(pathA2.popEquals(Pop.all, pathB1));
            assertTrue(pathB1.popEquals(Pop.all, pathB2));
            assertTrue(pathA1.popEquals(Pop.first, pathA2));
            assertFalse(pathA2.popEquals(Pop.first, pathB1));
            assertTrue(pathB1.popEquals(Pop.first, pathB2));
            assertTrue(pathA1.popEquals(Pop.last, pathA2));
            assertTrue(pathA2.popEquals(Pop.last, pathB1));
            assertTrue(pathB1.popEquals(Pop.last, pathB2));

            ///
            pathA1 = pathA1.extend("stephen", Collections.singleton("b"));
            pathA2 = pathA2.extend("stephen", Collections.singleton("b"));
            pathB1 = pathB1.extend("stephen", Collections.singleton("b"));
            pathB2 = pathB2.extend("stephen", Collections.singleton("b"));
            assertTrue(pathA1.popEquals(Pop.all, pathA2));
            assertFalse(pathA2.popEquals(Pop.all, pathB1));
            assertTrue(pathB1.popEquals(Pop.all, pathB2));
            assertTrue(pathA1.popEquals(Pop.first, pathA2));
            assertFalse(pathA2.popEquals(Pop.first, pathB1));
            assertTrue(pathB1.popEquals(Pop.first, pathB2));
            assertTrue(pathA1.popEquals(Pop.last, pathA2));
            assertTrue(pathA2.popEquals(Pop.last, pathB1));
            assertTrue(pathB1.popEquals(Pop.last, pathB2));
        });
    }

    @Test
    public void shouldHaveCrossTypeEquality() {
        List<Path> paths = PATH_SUPPLIERS.stream()
                .map(Supplier::get)
                .map(path -> path.extend("marko", new LinkedHashSet<>(Arrays.asList("a", "aa"))))
                .map(path -> path.extend("daniel", Collections.singleton("b")))
                .collect(Collectors.toList());
        for (final Path pathA : paths) {
            for (final Path pathB : paths) {
                assertEquals(pathA, pathB);
                assertEquals(pathA.hashCode(), pathB.hashCode());
            }
        }
    }
}
