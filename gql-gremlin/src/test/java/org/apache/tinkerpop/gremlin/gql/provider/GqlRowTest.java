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
package org.apache.tinkerpop.gremlin.gql.provider;

import org.apache.tinkerpop.gremlin.gql.GqlRow;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests {@link GqlRow} from outside the {@code org.apache.tinkerpop.gremlin.gql} package, so it
 * compiles only against the public API a provider's {@code GqlExecutor} would use.
 */
public class GqlRowTest {

    private final ReferenceVertex a = new ReferenceVertex(1, "person");
    private final ReferenceVertex b = new ReferenceVertex(2, "person");
    private final ReferenceVertex c = new ReferenceVertex(3, "person");
    private final Edge ab = new ReferenceEdge(10, "knows", b, a);
    private final Edge bc = new ReferenceEdge(11, "knows", c, b);

    @Test
    public void shouldBuildAndReadScalarAndGroupBindings() {
        final List<Edge> walk = List.of(ab, bc);
        final GqlRow row = GqlRow.of(new Element[]{a, null, c}, Map.of(1, walk));

        assertEquals(3, row.size());
        assertSame(a, row.getScalar(0));
        assertFalse(row.isGroup(0));
        assertNull(row.getGroup(0));

        assertTrue(row.isGroup(1));
        assertNull(row.getScalar(1));
        assertEquals(walk, row.getGroup(1));

        assertSame(c, row.getScalar(2));
        assertFalse(row.isGroup(2));
    }

    @Test
    public void shouldTreatSlotWithNoBindingAsUnbound() {
        final GqlRow row = GqlRow.of(new Element[]{a, null}, Collections.emptyMap());
        assertNull(row.getScalar(1));
        assertNull(row.getGroup(1));
        assertFalse(row.isGroup(1));
    }

    @Test
    public void shouldNotChangeWhenInputsAreMutatedAfterConstruction() {
        final Element[] scalars = {a, null, c};
        final List<Edge> walk = new ArrayList<>(List.of(ab));
        final Map<Integer, List<Edge>> groups = new HashMap<>();
        groups.put(1, walk);

        final GqlRow row = GqlRow.of(scalars, groups);

        scalars[0] = b;
        scalars[2] = null;
        walk.add(bc);
        groups.remove(1);

        assertSame(a, row.getScalar(0));
        assertSame(c, row.getScalar(2));
        assertTrue(row.isGroup(1));
        assertEquals(List.of(ab), row.getGroup(1));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shouldExposeUnmodifiableGroupList() {
        final GqlRow row = GqlRow.of(new Element[]{a, null}, Map.of(1, List.of(ab)));
        row.getGroup(1).add(bc);
    }

    @Test
    public void shouldRejectIndexBoundAsBothScalarAndGroup() {
        try {
            GqlRow.of(new Element[]{a, ab}, Map.of(1, List.of(ab)));
            fail("an index bound as both scalar and group must be rejected");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("both a scalar and a group"));
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectGroupIndexOutsideRow() {
        GqlRow.of(new Element[]{a}, Map.of(1, List.of(ab)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullScalars() {
        GqlRow.of(null, Collections.emptyMap());
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullGroups() {
        GqlRow.of(new Element[]{a}, null);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void shouldRejectOutOfRangeIndexOnRead() {
        GqlRow.of(new Element[]{a}, Collections.emptyMap()).getScalar(1);
    }

    @Test
    public void shouldRejectOutOfRangeAndNegativeIndexOnEveryReader() {
        // Readers check the index against size() whether or not the row has groups, so an index
        // past the end or below zero fails the same way on all three.
        final GqlRow row = GqlRow.of(new Element[]{a, null}, Map.of(1, List.of(ab)));
        for (final int index : new int[]{2, 3, -1, Integer.MIN_VALUE, Integer.MAX_VALUE}) {
            assertOutOfBounds("getScalar(" + index + ")", () -> row.getScalar(index));
            assertOutOfBounds("getGroup(" + index + ")", () -> row.getGroup(index));
            assertOutOfBounds("isGroup(" + index + ")", () -> row.isGroup(index));
        }
        final GqlRow empty = GqlRow.of(new Element[0], Collections.emptyMap());
        assertEquals(0, empty.size());
        assertOutOfBounds("getScalar(0) on an empty row", () -> empty.getScalar(0));
        assertOutOfBounds("getGroup(0) on an empty row", () -> empty.getGroup(0));
        assertOutOfBounds("isGroup(0) on an empty row", () -> empty.isGroup(0));
    }

    private static void assertOutOfBounds(final String call, final Runnable read) {
        try {
            read.run();
            fail(call + " must throw IndexOutOfBoundsException");
        } catch (IndexOutOfBoundsException expected) {
            // expected
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRejectNegativeGroupIndex() {
        GqlRow.of(new Element[]{a}, Map.of(-1, List.of(ab)));
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullGroupIndex() {
        final Map<Integer, List<Edge>> groups = new HashMap<>();
        groups.put(null, List.of(ab));
        GqlRow.of(new Element[]{a, null}, groups);
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullGroupList() {
        final Map<Integer, List<Edge>> groups = new HashMap<>();
        groups.put(1, null);
        GqlRow.of(new Element[]{a, null}, groups);
    }

    @Test(expected = NullPointerException.class)
    public void shouldRejectNullEdgeInGroupList() {
        final List<Edge> walk = new ArrayList<>();
        walk.add(ab);
        walk.add(null);
        GqlRow.of(new Element[]{a, null}, Map.of(1, walk));
    }

    @Test
    public void shouldRejectEmptyGroupList() {
        try {
            GqlRow.of(new Element[]{a, null}, Map.of(1, List.of()));
            fail("an empty group list must be rejected");
        } catch (IllegalArgumentException ex) {
            assertTrue(ex.getMessage(), ex.getMessage().contains("empty"));
        }
    }

    @Test
    public void shouldBeEqualWhenBindingsAreEqual() {
        final GqlRow row = GqlRow.of(new Element[]{a, null, c}, Map.of(1, List.of(ab, bc)));
        final GqlRow same = GqlRow.of(new Element[]{a, null, c}, Map.of(1, new ArrayList<>(List.of(ab, bc))));

        assertEquals(row, row);
        assertEquals(row, same);
        assertEquals(same, row);
        assertEquals(row.hashCode(), same.hashCode());
        assertEquals(GqlRow.of(new Element[]{a}, Collections.emptyMap()),
                GqlRow.of(new Element[]{a}, new HashMap<>()));
    }

    @Test
    public void shouldNotBeEqualWhenBindingsDiffer() {
        final GqlRow row = GqlRow.of(new Element[]{a, null, c}, Map.of(1, List.of(ab, bc)));

        assertNotEquals(row, GqlRow.of(new Element[]{b, null, c}, Map.of(1, List.of(ab, bc))));
        assertNotEquals(row, GqlRow.of(new Element[]{a, null, c}, Map.of(1, List.of(ab))));
        assertNotEquals(row, GqlRow.of(new Element[]{a, null, c}, Map.of(1, List.of(bc, ab))));
        assertNotEquals(row, GqlRow.of(new Element[]{a, null, c}, Collections.emptyMap()));
        assertNotEquals(row, GqlRow.of(new Element[]{a, null, c, null}, Map.of(1, List.of(ab, bc))));
        assertNotEquals(row, null);
        assertNotEquals(row, "not a row");
    }

    @Test
    public void shouldDescribeBindingsInToString() {
        final String s = GqlRow.of(new Element[]{a, null}, Map.of(1, List.of(ab))).toString();
        assertTrue(s, s.contains(a.toString()));
        assertTrue(s, s.contains(ab.toString()));
    }
}
