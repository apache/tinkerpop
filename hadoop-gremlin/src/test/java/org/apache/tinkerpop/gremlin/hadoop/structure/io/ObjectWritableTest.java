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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ObjectWritableTest {

    @Test
    public void shouldNotHaveANullPointerException() {
        final ObjectWritable object = new ObjectWritable<>(null);
        assertNull(object.get());
        assertTrue(object.isEmpty());
        assertTrue(object.equals(ObjectWritable.empty()));
        assertFalse(object.equals(new ObjectWritable<>("hello")));
        assertEquals("null", object.toString());
        assertEquals(-1, object.compareTo(new ObjectWritable<>("blah")));
        assertEquals(-1, new ObjectWritable<>("bloop").compareTo(ObjectWritable.empty()));
        assertEquals(-1, new ObjectWritable<>("bloop").compareTo(object));
        assertEquals(0, ObjectWritable.empty().compareTo(object));
        assertEquals(0, object.compareTo(ObjectWritable.empty()));
        @SuppressWarnings({"SelfComparison", "EqualsWithItself"})
        int selfComparisonResult = object.compareTo(object);
        assertEquals(0, selfComparisonResult);
    }
}
