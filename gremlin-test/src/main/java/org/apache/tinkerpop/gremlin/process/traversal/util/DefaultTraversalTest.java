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
package org.apache.tinkerpop.gremlin.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraversalTest {

    @Test
    public void shouldCloneTraversalCorrectly() throws CloneNotSupportedException {
        final DefaultGraphTraversal<?, ?> original = new DefaultGraphTraversal<>();
        original.out().groupCount("m").values("name").count();
        final DefaultTraversal<?, ?> clone = (DefaultTraversal) original.clone();
        assertNotEquals(original.hashCode(), clone.hashCode());
        assertEquals(original.getSteps().size(), clone.getSteps().size());

        for (int i = 0; i < original.steps.size(); i++) {
            assertNotEquals(original.getSteps().get(i), clone.getSteps().get(i));
        }
        assertNotEquals(original.sideEffects, clone.sideEffects);

    }
}
