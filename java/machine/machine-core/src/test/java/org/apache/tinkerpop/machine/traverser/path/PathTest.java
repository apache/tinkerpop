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
package org.apache.tinkerpop.machine.traverser.path;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
class PathTest {

    private static List<Supplier<Path>> PATH_SUPPLIERS = List.of(BasicPath::new);

    @Test
    void testPathMethods() {
        PATH_SUPPLIERS.forEach(pathSupplier -> {
            final Path a = pathSupplier.get();
            final Path b = pathSupplier.get();
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
            assertEquals(0, a.size());
            assertEquals(0, b.size());
            ///
            a.add(Set.of("step1"), 1);
            assertNotEquals(a, b);
            assertEquals(1, a.size());
            assertEquals(0, b.size());
            ///
            b.add(Set.of("step1"), 1);
            assertEquals(a, b);
            assertEquals(1, a.size());
            assertEquals(1, b.size());
            ///
            a.add(Set.of("step2", "step3"), 2);
            b.add(Set.of("step2"), 22);
            assertNotEquals(a, b);
            assertEquals(2, a.size());
            assertEquals(2, b.size());
            ///
            a.add(Set.of("step3"), 3);
            b.add(Set.of("step3"), 33);
            assertNotEquals(a, b);
            assertEquals(3, a.size());
            assertEquals(3, b.size());
            ///
            assertEquals(1, a.get(Path.Pop.first, "step1"));
            assertEquals(1, b.get(Path.Pop.first, "step1"));
            assertEquals(1, a.get(Path.Pop.last, "step1"));
            assertEquals(1, b.get(Path.Pop.last, "step1"));
            assertEquals(List.of(1), a.get(Path.Pop.all, "step1"));
            assertEquals(List.of(1), b.get(Path.Pop.all, "step1"));
            ///
            assertEquals(2, a.get(Path.Pop.first, "step2"));
            assertEquals(22, b.get(Path.Pop.first, "step2"));
            assertEquals(2, a.get(Path.Pop.last, "step2"));
            assertEquals(22, b.get(Path.Pop.last, "step2"));
            assertEquals(List.of(2), a.get(Path.Pop.all, "step2"));
            assertEquals(List.of(22), b.get(Path.Pop.all, "step2"));
            ///
            assertEquals(2, a.get(Path.Pop.first, "step3"));
            assertEquals(33, b.get(Path.Pop.first, "step3"));
            assertEquals(3, a.get(Path.Pop.last, "step3"));
            assertEquals(33, b.get(Path.Pop.last, "step3"));
            assertEquals(List.of(2, 3), a.get(Path.Pop.all, "step3"));
            assertEquals(List.of(33), b.get(Path.Pop.all, "step3"));
            ///
            final Set<String> labels = Set.of("step4");
            a.add(labels, 4);
            b.add(labels, 4);
            assertNotEquals(a, b);
            assertEquals(4, a.size());
            assertEquals(4, b.size());
        });
    }

    @Test
    void testObjectMethods() {
        PATH_SUPPLIERS.forEach(pathSupplier -> {
            final Path a = pathSupplier.get();
            final Path b = a.clone();
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
            a.add(Set.of("step1"), 1);
            b.add(Set.of("step1"), 1);
            assertEquals(a, b);
            assertEquals(a.hashCode(), b.hashCode());
            assertEquals(1, a.size());
            assertEquals(1, b.size());
            b.add(Set.of("step2"), 2);
            assertNotEquals(a, b);
            assertNotEquals(a.hashCode(), b.hashCode());
            assertEquals(1, a.size());
            assertEquals(2, b.size());
            final Path c = b.clone();
            assertEquals(b, c);
            assertNotEquals(a, c);
            c.add(Set.of("step3"), 3);
            assertNotEquals(b, c);
            assertNotEquals(a, c);
            assertEquals(1, a.size());
            assertEquals(2, b.size());
            assertEquals(3, c.size());
        });
    }
}
