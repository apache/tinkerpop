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
package org.apache.tinkerpop.gremlin.process.traversal.traverser;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class LoopCounterOverflowTest {

    // Overflow is seeded directly at Integer.MAX_VALUE rather than reached by looping incrLoops()
    // billions of times, which would be impractical for a unit test.

    @Test
    public void shouldThrowOnSingleLoopOverflow() {
        final GraphTraversalSource g = EmptyGraph.instance().traversal();
        final Traversal.Admin traversal = g.withBulk(false).V().repeat(__.out()).times(10).asAdmin();
        traversal.applyStrategies();
        final Traverser.Admin<?> traverser = traversal.getTraverserGenerator().generate(new Object(), traversal.getStartStep(), 1L);
        assertEquals(O_OB_S_SE_SL_Traverser.class, traverser.getClass());

        ((O_OB_S_SE_SL_Traverser<?>) traverser).loops = Integer.MAX_VALUE;
        assertEquals(Integer.MAX_VALUE, traverser.loops());

        try {
            traverser.incrLoops();
            fail("Should have thrown an IllegalStateException when the loop counter overflows an int");
        } catch (IllegalStateException ise) {
            assertEquals(Integer.MAX_VALUE, traverser.loops());
        }
    }

    @Test
    public void shouldThrowOnNestedLoopOverflow() {
        final GraphTraversalSource g = EmptyGraph.instance().traversal();
        final Traversal.Admin traversal = g.withBulk(false).V().repeat(__.repeat(__.out())).times(10).asAdmin();
        traversal.applyStrategies();
        final Traverser.Admin<?> traverser = traversal.getTraverserGenerator().generate(new Object(), traversal.getStartStep(), 1L);
        assertEquals(NL_O_OB_S_SE_SL_Traverser.class, traverser.getClass());

        ((NL_O_OB_S_SE_SL_Traverser<?>) traverser).nestedLoops.push(new LabelledCounter("repeat", Integer.MAX_VALUE));
        assertEquals(Integer.MAX_VALUE, traverser.loops());

        try {
            traverser.incrLoops();
            fail("Should have thrown an IllegalStateException when the loop counter overflows an int");
        } catch (IllegalStateException ise) {
            assertEquals(Integer.MAX_VALUE, traverser.loops());
        }
    }
}
