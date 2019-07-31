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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class TraversalInterruptionTest extends AbstractGremlinProcessTest {
    private static final Logger logger = LoggerFactory.getLogger(TraversalInterruptionTest.class);

    @Parameterized.Parameters(name = "expectInterruption({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"g_V", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V(), (UnaryOperator<GraphTraversal<?,?>>) t -> t},
                {"g_V_out", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.out()},
                {"g_V_outE", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.outE()},
                {"g_V_in", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.in()},
                {"g_V_inE", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.inE()},
                {"g_V_properties", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.V(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.properties()},
                {"g_E", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E(), (UnaryOperator<GraphTraversal<?,?>>) t -> t},
                {"g_E_outV", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.outV()},
                {"g_E_inV", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.inV()},
                {"g_E_properties", (Function<GraphTraversalSource, GraphTraversal<?,?>>) g -> g.E(), (UnaryOperator<GraphTraversal<?,?>>) t -> t.properties()},
        });
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public Function<GraphTraversalSource,GraphTraversal<?,?>> traversalBeforePause;

    @Parameterized.Parameter(value = 2)
    public UnaryOperator<GraphTraversal<?,?>> traversalAfterPause;

    @Test
    @LoadGraphWith(GRATEFUL)
    public void shouldRespectThreadInterruptionInVertexStep() throws Exception {
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);
        final CountDownLatch startedIterating = new CountDownLatch(1);
        final Thread t = new Thread(() -> {
            final Traversal traversal = traversalAfterPause.apply(traversalBeforePause.apply(g).sideEffect(traverser -> {
                // let the first iteration flow through
                if (startedIterating.getCount() == 0) {
                    // ensure that the whole traversal doesn't iterate out before we get a chance to interrupt
                    // the next iteration should stop so we can force the interrupt to be handled by VertexStep
                    try {
                        Thread.sleep(3000);
                    } catch (Exception ignored) {
                        // make sure that the interrupt propagates in case the interrupt occurs during sleep.
                        // this should ensure VertexStep gets to try to throw the TraversalInterruptedException
                        Thread.currentThread().interrupt();
                    }
                } else {
                    startedIterating.countDown();
                }
            }));
            try {
                traversal.iterate();
            } catch (Exception ex) {
                exceptionThrown.set(ex instanceof TraversalInterruptedException);

                try {
                    traversal.close();
                } catch (Exception iex) {
                    logger.error("Error closing traversal after interruption", iex);
                }
            }

        }, name);

        t.start();

        // total time for test should not exceed 5 seconds - this prevents the test from just hanging and allows
        // it to finish with failure
        assertThat(startedIterating.await(5000, TimeUnit.MILLISECONDS), CoreMatchers.is(true));

        t.interrupt();
        t.join();

        // ensure that some but not all of the traversal was iterated and that the right exception was tossed
        assertThat(exceptionThrown.get(), CoreMatchers.is(true));
    }
}