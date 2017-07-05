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

package org.apache.tinkerpop.gremlin.process.traversal.traverser.util;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DefaultTraverserGeneratorFactoryTest {

    @Test
    public void shouldProduceExpectedTraverser() {
        final GraphTraversalSource traversalSource = EmptyGraph.instance().traversal();
        final Object start = new Object();

        Traversal.Admin traversal = traversalSource.V().out().asAdmin();
        traversal.applyStrategies();
        assertEquals(B_O_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.V().out().barrier().asAdmin();
        traversal.applyStrategies();
        assertEquals(B_O_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.V().repeat(__.out()).times(10).asAdmin();
        traversal.applyStrategies();
        assertEquals(B_O_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.V().out().sack().asAdmin();
        traversal.applyStrategies();
        assertEquals(B_O_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.V().out().sack().as("a").select("a").asAdmin();
        traversal.applyStrategies();
        assertEquals(B_LP_O_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.V().out().path().asAdmin();
        traversal.applyStrategies();
        assertEquals(B_LP_O_P_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.withBulk(false).V().out().asAdmin();
        traversal.applyStrategies();
        assertEquals(O_OB_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.withBulk(false).V().as("a").out().select("a").asAdmin();
        traversal.applyStrategies();
        assertEquals(LP_O_OB_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());
        //
        traversal = traversalSource.withBulk(false).V().out().path().asAdmin();
        traversal.applyStrategies();
        assertEquals(LP_O_OB_P_S_SE_SL_Traverser.class, traversal.getTraverserGenerator().generate(start, traversal.getStartStep(), 1l).getClass());

    }
}
