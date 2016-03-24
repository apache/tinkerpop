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

package org.apache.tinkerpop.gremlin.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.UnionStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalFlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TraversalMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class TraversalParentTest {

    @Test
    public void shouldIdentityLocalChildren() {
        final Traversal.Admin<?, ?> localChild = __.as("x").select("a", "b").by("name").asAdmin();
        TraversalParent parent = new LocalStep<>(new DefaultTraversal(), localChild);
        assertTrue(parent.isLocalChild(localChild));
        assertFalse(parent.isGlobalChild(localChild));
        ///
        parent = new WhereTraversalStep<>(new DefaultTraversal(), localChild);
        assertTrue(parent.isLocalChild(localChild));
        assertFalse(parent.isGlobalChild(localChild));
        ///
        parent = new TraversalFilterStep<>(new DefaultTraversal(), localChild);
        assertTrue(parent.isLocalChild(localChild));
        assertFalse(parent.isGlobalChild(localChild));
        ///
        parent = new TraversalMapStep<>(new DefaultTraversal(), localChild);
        assertTrue(parent.isLocalChild(localChild));
        assertFalse(parent.isGlobalChild(localChild));
        ///
        parent = new TraversalFlatMapStep<>(new DefaultTraversal(), localChild);
        assertTrue(parent.isLocalChild(localChild));
        assertFalse(parent.isGlobalChild(localChild));
    }

    @Test
    public void shouldIdentityGlobalChildren() {
        final Traversal.Admin<?, ?> globalChild = __.select("a", "b").by("name").asAdmin();
        TraversalParent parent = new RepeatStep<>(new DefaultTraversal());
        ((RepeatStep) parent).setRepeatTraversal(globalChild);
        assertFalse(parent.isLocalChild(globalChild));
        assertTrue(parent.isGlobalChild(globalChild));
        ///
        parent = new UnionStep<>(new DefaultTraversal(), globalChild);
        assertFalse(parent.isLocalChild(globalChild));
        assertTrue(parent.isGlobalChild(globalChild));
    }
}
