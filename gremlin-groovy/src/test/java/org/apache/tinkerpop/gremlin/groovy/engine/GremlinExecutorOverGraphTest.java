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
package org.apache.tinkerpop.gremlin.groovy.engine;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinExecutorOverGraphTest {
    private final BasicThreadFactory testingThreadFactory = new BasicThreadFactory.Builder().namingPattern("test-gremlin-executor-%d").build();

    @Test
    public void shouldAllowTraversalToIterateInDifferentThreadThanOriginallyEvaluatedWithAutoCommit() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();

        // this test sort of simulates Gremlin Server interaction where a Traversal is eval'd in one Thread, but
        // then iterated in another.  note that Gremlin Server configures the script engine to auto-commit
        // after evaluation.  this basically tests the state of the Gremlin Server GremlinExecutor when
        // being used in sessionless mode
        final ExecutorService evalExecutor = Executors.newSingleThreadExecutor(testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build()
                .afterSuccess(b -> {
                    final GraphTraversalSource ig = (GraphTraversalSource) b.get("g");
                    if (ig.getGraph().features().graph().supportsTransactions())
                        ig.tx().commit();
                })
                .executorService(evalExecutor).create();

        final Map<String,Object> bindings = new HashMap<>();
        bindings.put("g", g);

        final AtomicInteger vertexCount = new AtomicInteger(0);

        final ExecutorService iterationExecutor = Executors.newSingleThreadExecutor(testingThreadFactory);
        gremlinExecutor.eval("g.V().out()", bindings).thenAcceptAsync(o -> {
            final Iterator itty = (Iterator) o;
            itty.forEachRemaining(v -> vertexCount.incrementAndGet());
        }, iterationExecutor).join();

        assertEquals(6, vertexCount.get());

        gremlinExecutor.close();
        evalExecutor.shutdown();
        evalExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);
        iterationExecutor.shutdown();
        iterationExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);
    }

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldAllowTraversalToIterateInDifferentThreadThanOriginallyEvaluatedWithoutAutoCommit() throws Exception {
        final TinkerGraph graph = TinkerFactory.createModern();
        final GraphTraversalSource g = graph.traversal();

        // this test sort of simulates Gremlin Server interaction where a Traversal is eval'd in one Thread, but
        // then iterated in another.  this basically tests the state of the Gremlin Server GremlinExecutor when
        // being used in session mode
        final ExecutorService evalExecutor = Executors.newSingleThreadExecutor(testingThreadFactory);
        final GremlinExecutor gremlinExecutor = GremlinExecutor.build().executorService(evalExecutor).create();

        final Map<String,Object> bindings = new HashMap<>();
        bindings.put("g", g);

        final AtomicInteger vertexCount = new AtomicInteger(0);

        final ExecutorService iterationExecutor = Executors.newSingleThreadExecutor(testingThreadFactory);
        gremlinExecutor.eval("g.V().out()", bindings).thenAcceptAsync(o -> {
            final Iterator itty = (Iterator) o;
            itty.forEachRemaining(v -> vertexCount.incrementAndGet());
        }, iterationExecutor).join();

        assertEquals(6, vertexCount.get());

        gremlinExecutor.close();
        evalExecutor.shutdown();
        evalExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);
        iterationExecutor.shutdown();
        iterationExecutor.awaitTermination(30000, TimeUnit.MILLISECONDS);
    }
}
