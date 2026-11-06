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
package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LabelledCounterTest {

    @Test
    public void shouldIncrementCount() {
        final LabelledCounter lc = new LabelledCounter("a", 0);
        lc.increment();
        lc.increment();
        assertEquals(2, lc.count());
    }

    @Test
    public void shouldThrowOnOverflow() {
        final LabelledCounter lc = new LabelledCounter("a", Integer.MAX_VALUE);
        try {
            lc.increment();
            fail("Should have thrown an IllegalStateException when the loop counter overflows an int");
        } catch (IllegalStateException ise) {
            assertEquals(Integer.MAX_VALUE, lc.count());
        }
    }
}
