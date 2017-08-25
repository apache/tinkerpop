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
package org.apache.tinkerpop.gremlin.tinkergraph.process.traversal;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.TimeUtil;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class TinkerGraphPerformanceTest {

    /**
     * Check the traverser's hashcode() implementation if this test fails. hashcode() should
     * not generate too many hash collisions.
     */
    @Test
    public void sacksWithoutMergerShouldBeFast() {

        final TinkerGraph graph = TinkerGraph.open();
        for (int i = 0; i < 8192; i++) {
            graph.addVertex().property("x", 0);
        }

        final GraphTraversalSource g = graph.traversal();
        final double runtime = TimeUtil.clock(10,
                () -> g.withSack(0).V().values("x").barrier().iterate());

        assertThat(runtime, is(lessThan(50.0)));
    }
}
