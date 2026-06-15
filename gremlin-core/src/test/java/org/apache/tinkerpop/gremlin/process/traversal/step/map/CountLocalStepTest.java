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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.StepTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class CountLocalStepTest extends StepTest {

    @Override
    protected List<Traversal> getTraversals() {
        return Collections.singletonList(__.count(Scope.local));
    }

        @Test
        public void shouldCountTotalNodesOnTree() {
            final Traversal.Admin<Object, Long> traversal = __.<Object>count(Scope.local).asAdmin();
            final CountLocalStep<Object> step = (CountLocalStep<Object>) traversal.getSteps().get(0);
    
            // tree: a -> b (two total nodes)
            final Tree<String> tree = new Tree<>();
            tree.getOrCreateChild("a").getOrCreateChild("b");
            final Traverser.Admin<Object> traverser = new B_O_Traverser<>(tree, 1L);
    
            // count(local) on a Tree returns the total node count (Tree.nodeCount()), not the root-entry count
            assertEquals(Long.valueOf(2L), step.map(traverser));
        }
    
        @Test
        public void shouldCountTotalNodesOnMultiRootTree() {
            final Traversal.Admin<Object, Long> traversal = __.<Object>count(Scope.local).asAdmin();
            final CountLocalStep<Object> step = (CountLocalStep<Object>) traversal.getSteps().get(0);
    
            // tree with two roots and nested children: x -> x1, y (three total nodes)
            final Tree<String> tree = new Tree<>();
            tree.getOrCreateChild("x").getOrCreateChild("x1");
            tree.getOrCreateChild("y");
            final Traverser.Admin<Object> traverser = new B_O_Traverser<>(tree, 1L);
    
            assertEquals(Long.valueOf(3L), step.map(traverser));
        }
}
