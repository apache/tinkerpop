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

import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link P} with {@link BulkSet} literals ensure that if a {@link BulkSet} is used as a literal,
 * it is preserved in the {@link P} object's value, and that the {@link P} object can handle {@link BulkSet} literals
 * with {@link GValue} elements correctly. You get a {@link BulkSet} in {@link P} when you have some Gremlin like:
 * {@code aggreate('x')...where(within('x'))}.
 */
public class PBulkSetTest {

    @Test
    public void shouldPreserveBulkSetInGetValue() {
        final BulkSet<String> bulkSet = new BulkSet<>();
        bulkSet.add("a", 2);
        bulkSet.add("b", 1);

        final P p = P.within(bulkSet);
        final Object value = p.getValue();

        assertTrue("Value should be a BulkSet but was " + value.getClass().getName(), value instanceof BulkSet);
        assertEquals(bulkSet, value);
        assertEquals(3, ((BulkSet) value).size());
        assertEquals(2L, ((BulkSet) value).get("a"));
    }

    @Test
    public void shouldPreserveBulkSetInSetValueWithGValues() {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add("a", 2);
        bulkSet.add(GValue.of("x", "b"), 1);

        final P p = P.within(Collections.emptyList());
        p.setValue(bulkSet);

        final Object value = p.getValue();
        // Currently this will fail and return an ArrayList (or a List from the stream)
        assertTrue("Value should be a BulkSet but was " + (value == null ? "null" : value.getClass().getName()), value instanceof BulkSet);
        assertEquals(3, ((BulkSet) value).size());
        assertEquals(2L, ((BulkSet) value).get("a"));
    }

    @Test
    public void shouldCloneBulkSetInGetValueWithVariables() {
        final BulkSet<Object> bulkSet = new BulkSet<>();
        bulkSet.add("a", 2);

        // Subclass to access protected constructor and merge literals and variables
        final P p = new PSubWithVars(Compare.eq, bulkSet, Collections.singletonMap("var1", "b"));

        final Object value = p.getValue();
        assertTrue("Value should be a BulkSet but was " + (value == null ? "null" : value.getClass().getName()), value instanceof BulkSet);
        assertEquals(3, ((BulkSet) value).size());
        assertEquals(2L, ((BulkSet) value).get("a"));
        assertEquals(1L, ((BulkSet) value).get("b"));

        // Ensure original bulkSet was not modified (p.getValue() should have cloned)
        assertEquals(2, bulkSet.size());
        assertEquals(2L, bulkSet.get("a"));
        assertEquals(0L, bulkSet.get("b"));
    }
    
    @Test
    public void shouldPreserveBulkSetInProtectedConstructor() throws Exception {
        final BulkSet<String> bulkSet = new BulkSet<>();
        bulkSet.add("a", 2);
        
        final P p = new PSub(Compare.eq, bulkSet);
        
        final Object value = p.getValue();
        assertTrue("Value should be a BulkSet but was " + (value == null ? "null" : value.getClass().getName()), value instanceof BulkSet);
        assertEquals(2, ((BulkSet) value).size());
        assertEquals(2L, ((BulkSet) value).get("a"));
    }

    private static class PSub extends P {
        public PSub(final PBiPredicate biPredicate, final Collection literals) {
            super(biPredicate, literals, Collections.emptyMap(), true);
        }
    }

    private static class PSubWithVars extends P {
        public PSubWithVars(final PBiPredicate biPredicate, final Collection literals, final Map variables) {
            super(biPredicate, literals, variables, true);
        }
    }
}
