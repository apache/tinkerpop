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

import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.GRATEFUL;
import static org.junit.Assert.assertThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(Parameterized.class)
public class TraversalInterruptionComputerTest extends AbstractGremlinProcessTest {

    @Parameterized.Parameters(name = "expectInterruption({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"g_V", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V()},
                {"g_V_out", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V().out()},
                {"g_V_outE", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V().outE()},
                {"g_V_in", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V().in()},
                {"g_V_inE", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V().inE()},
                {"g_V_properties", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V().properties()},
                {"g_E", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E()},
                {"g_E_outV", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E().outV()},
                {"g_E_inV", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E().inV()},
                {"g_E_properties", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E().properties()},
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Function<GraphTraversalSource,GraphTraversal<?,?>> traversalMaker;

    @Test
    @LoadGraphWith(GRATEFUL)
    public void shouldRespectThreadInterruptionInVertexStep() throws Exception {
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        final CountDownLatch startedIterating = new CountDownLatch(1);
        final Thread t = new Thread(() -> {
            try {
                final Traversal traversal = traversalMaker.apply(g).sideEffect(traverser -> {
                    startedIterating.countDown();
                    try {
                        // artificially slow down the traversal execution so that
                        // this test has a chance to interrupt it before it finishes
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        // if the traversal is interrupted while in this
                        // sleep, reassert the interrupt so that the thread is
                        // re-marked as interrupted and correctly handled as
                        // an interrupted traversal
                        Thread.currentThread().interrupt();
                    }
                });
                traversal.iterate();
            } catch (Exception ex) {
                exceptionThrown.set(ex instanceof TraversalInterruptedException);
            }
        }, name);

        t.start();

        // total time for test should not exceed 1 second - this prevents the test from just hanging and allows
        // it to finish with failure
        assertThat(startedIterating.await(1000, TimeUnit.MILLISECONDS), CoreMatchers.is(true));

        t.interrupt();
        t.join();

        // ensure that some but not all of the traversal was iterated and that the right exception was tossed
        assertThat(exceptionThrown.get(), CoreMatchers.is(true));
    }
}
