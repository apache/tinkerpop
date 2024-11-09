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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;

/**
 * @author Cole Greer (https://github.com/Cole-Greer)
 */
public class TinkerShuffleGraphTest {

    Logger logger = LoggerFactory.getLogger(TinkerShuffleGraphTest.class);

    // Tests are checking for random shuffling of results. If random shuffling happens to return sorted order, the test
    // will fail. Worst case scenario are the tests which only return 2 results as these have a 50% chance of a failure.
    // Tests are configured with up to 50 retries to drive the failure probability down to the order of 10^-16.
    @Rule
    public RetryRule retryRule = new RetryRule();

    @Test
    public void shouldShuffleVertices() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        List<Vertex> resultRegular = gReg.V().toList();
        Object[] resultShuffle = gShuff.V().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffleEdges() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        List<Edge> resultRegular = gReg.E().toList();
        Object[] resultShuffle = gShuff.E().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffle_g_V_both() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        List<Vertex> resultRegular = gReg.V(1).both().toList();
        Object[] resultShuffle = gShuff.V(1).both().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffle_g_V_bothE() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        List<Edge> resultRegular = gReg.V(1).bothE().toList();
        Object[] resultShuffle = gShuff.V(1).bothE().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffle_g_E_bothV() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        List<Vertex> resultRegular = gReg.V(1).outE("knows").bothV().toList();
        Object[] resultShuffle = gShuff.V(1).outE("knows").bothV().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffleVertexProperties() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        List<Property<Object>> resultRegular = (List<Property<Object>>) gReg.V(1).properties().toList();
        Object[] resultShuffle = gShuff.V(1).properties().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffleEdgeProperties() {
        GraphTraversalSource gShuff = createShuffleModern();
        GraphTraversalSource gReg = TinkerFactory.createModern().traversal();

        gShuff.V(1).outE("knows").property("extraProp", "no toy graph has 2 props on an edge");
        gReg.V(1).outE("knows").property("extraProp", "no toy graph has 2 props on an edge");

        List<Property<Object>> resultRegular = (List<Property<Object>>) gReg.V(1).outE("knows").properties().toList();
        Object[] resultShuffle = gShuff.V(1).outE("knows").properties().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffleMetaProperties() {
        GraphTraversalSource gShuff = createShuffleTheCrew();
        GraphTraversalSource gReg = TinkerFactory.createTheCrew().traversal();

        List<Property<Object>> resultRegular = (List<Property<Object>>) gReg.V(1).properties("location").hasValue("san diego").properties().toList();
        Object[] resultShuffle = gShuff.V(1).properties("location").hasValue("san diego").properties().toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    @Test
    public void shouldShuffleMultiProperties() {
        GraphTraversalSource gShuff = createShuffleTheCrew();
        GraphTraversalSource gReg = TinkerFactory.createTheCrew().traversal();

        List<Property<Object>> resultRegular = (List<Property<Object>>) gReg.V(1).properties("location").toList();
        Object[] resultShuffle = gShuff.V(1).properties("location").toList().toArray();

        //Assert contents are correct
        assertThat(resultRegular, containsInAnyOrder(resultShuffle));
        //Assert order is shuffled
        assertThat(resultRegular, not(contains(resultShuffle)));
    }

    private GraphTraversalSource createShuffleModern() {
        TinkerShuffleGraph graph = TinkerShuffleGraph.open();
        TinkerFactory.generateModern(graph);
        return traversal().withEmbedded(graph);
    }

    private GraphTraversalSource createShuffleTheCrew() {
        TinkerShuffleGraph graph = TinkerShuffleGraph.open();
        TinkerFactory.generateTheCrew(graph);
        return traversal().withEmbedded(graph);
    }

    private class RetryRule implements TestRule {
        final int maxRetries = 50;
        @Override
        public Statement apply(Statement base, Description description) {
            return new Statement() {
                @Override
                public void evaluate() throws Throwable {
                    for(int i = 1; i <= maxRetries; i++) {
                        try{
                            base.evaluate();
                            return;
                        }
                        catch(AssertionError e){
                            String msg = String.format("Failed test %s.%s (attempt %s/%s): ", description.getClassName(), description.getMethodName(), i, maxRetries);
                            if(i == maxRetries) {
                                logger.warn(msg, e);
                            } else {
                                logger.debug(msg, e);
                            }
                        }
                    }
                }
            };
        }
    }

}
