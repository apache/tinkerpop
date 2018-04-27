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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration;

import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest;
import org.apache.tinkerpop.gremlin.process.GremlinProcessRunner;
import org.apache.tinkerpop.gremlin.process.remote.RemoteGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.InlineFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.NoSuchElementException;

import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.CREW;
import static org.apache.tinkerpop.gremlin.LoadGraphWith.GraphData.MODERN;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GremlinProcessRunner.class)
public class SubgraphStrategyProcessTest extends AbstractGremlinProcessTest {

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterVertexCriterion() throws Exception {
        final Traversal<Vertex, ?> vertexCriterion = has("name", P.within("josh", "lop", "ripple"));

        final GraphTraversalSource sg = g.withStrategies(SubgraphStrategy.build().vertexCriterion(vertexCriterion).create());

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // only two edges are present, even though edges are not explicitly excluded
        // (edges require their incident vertices)
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(2, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(3, g.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.V(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with label and branch factor

        assertEquals(1, g.V(convertToVertexId("josh")).local(outE("created").limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(outE("created").limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(outE("created").limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(outE("created").limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("created").limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("created").limit(1)).count().next().longValue());

        // from edge

        assertEquals(2, g.E(convertToEdgeId("josh", "created", "lop")).bothV().count().next().longValue());
        assertEquals(2, sg.E(convertToEdgeId("josh", "created", "lop")).bothV().count().next().longValue());

        assertEquals(2, g.E(convertToEdgeId("peter", "created", "lop")).bothV().count().next().longValue());
        try {
            sg.E(convertToEdgeId("peter", "created", "lop")).next();
            fail("Edge 12 should not be in the graph because peter is not a vertex");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }

        assertEquals(2, g.E(convertToEdgeId("marko", "knows", "vadas")).bothV().count().next().longValue());
        try {
            sg.E(convertToEdgeId("marko", "knows", "vadas")).next();
            fail("Edge 7 should not be in the graph because marko is not a vertex");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterEdgeCriterion() throws Exception {
        final Traversal<Edge, ?> edgeCriterion = __.or(
                has("weight", 1.0d).hasLabel("knows"), // 8
                has("weight", 0.4d).hasLabel("created").outV().has("name", "marko"), // 9
                has("weight", 1.0d).hasLabel("created") // 10
        );

        final SubgraphStrategy strategy = SubgraphStrategy.build().edgeCriterion(edgeCriterion).create();
        final GraphTraversalSource sg = g.withStrategies(strategy);

        // all vertices are here
        assertEquals(6, g.V().count().next().longValue());
        final Traversal t = sg.V();
        t.hasNext();
        printTraversalForm(t);
        assertEquals(6, sg.V().count().next().longValue());

        // only the given edges are included
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(3, sg.E().count().next().longValue());

        assertEquals(2, g.V(convertToVertexId("marko")).outE("knows").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("marko")).outE("knows").count().next().longValue());

        // wrapped Traversal<Vertex, Vertex> takes into account the edges it must pass through
        assertEquals(2, g.V(convertToVertexId("marko")).out("knows").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("marko")).out("knows").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());

        // from vertex

        assertEquals(2, g.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.V(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.V(convertToVertexId("josh")).limit(1).local(bothE().limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).limit(1).local(bothE().limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).limit(1).local(bothE().limit(1)).inV().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).limit(1).local(bothE().limit(1)).inV().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).inV().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).inV().count().next().longValue());

        // from edge

        assertEquals(2, g.E(convertToEdgeId("marko", "knows", "josh")).bothV().count().next().longValue());
        assertEquals(2, sg.E(convertToEdgeId("marko", "knows", "josh")).bothV().count().next().longValue());

        assertEquals(3, g.E(convertToEdgeId("marko", "knows", "josh")).outV().outE().count().next().longValue());
        assertEquals(2, sg.E(convertToEdgeId("marko", "knows", "josh")).outV().outE().count().next().longValue());
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterComplexVertexCriterion() throws Exception {
        checkResults(Arrays.asList("vadas", "josh"), g.withStrategies(SubgraphStrategy.build().vertices(__.<Vertex>in("knows").has("name", "marko")).create()).
                V().values("name"));
        checkResults(Arrays.asList("vadas", "josh", "lop"), g.withStrategies(SubgraphStrategy.build().vertices(__.<Vertex>in().has("name", "marko")).create()).
                V().values("name"));

        checkResults(Arrays.asList("vadas", "josh"), g.withStrategies(SubgraphStrategy.build().vertices(__.<Vertex>in("knows").where(out("created").has("name", "lop"))).create()).
                V().values("name"));
        checkResults(Arrays.asList("vadas", "josh", "lop"), g.withStrategies(SubgraphStrategy.build().vertices(__.<Vertex>in().where(has("name", "marko").out("created").has("name", "lop"))).create()).
                V().values("name"));

        checkResults(Arrays.asList("marko", "vadas", "josh", "lop"), g.withStrategies(SubgraphStrategy.build().vertices(__.or(both().has("name", "marko"), has("name", "marko"))).create()).
                V().where(bothE().count().is(P.neq(0))).values("name"));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterMixedCriteriaButNotCheckAdjacentVertices() {
        final Traversal<Vertex, ?> vertexCriterion = has("name", P.within("josh", "lop", "ripple"));

        // 9 isn't present because marko is not in the vertex list
        final Traversal<Edge, ?> edgeCriterion = __.or(
                has("weight", 0.4d).hasLabel("created"), // 11
                has("weight", 1.0d).hasLabel("created") // 10
        );

        final SubgraphStrategy strategy = SubgraphStrategy.build().
                checkAdjacentVertices(false).
                edges(edgeCriterion).vertices(vertexCriterion).create();
        final GraphTraversalSource sg = g.withStrategies(strategy);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // three edges are explicitly included as we ignore checking of adjacent vertices
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(3, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.V(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both().count().next().longValue());

        // marko not present directly because of vertexCriterion - only accessible via vertices in the subgraph
        assertEquals(1, g.V(convertToVertexId("marko")).count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("marko")).count().next().longValue());

        // with label

        assertEquals(2, g.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE().limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE().limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE().limit(1)).inV().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE().limit(1)).inV().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).inV().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).inV().count().next().longValue());

        // from edge

        // marko is not accessible from the edge
        assertEquals(2, g.E(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
        assertEquals(1, sg.E(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
    }


    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterMixedCriteria() throws Exception {
        final Traversal<Vertex, ?> vertexCriterion = has("name", P.within("josh", "lop", "ripple"));

        // 9 isn't present because marko is not in the vertex list
        final Traversal<Edge, ?> edgeCriterion = __.or(
                has("weight", 0.4d).hasLabel("created"), // 11
                has("weight", 1.0d).hasLabel("created") // 10
        );

        final SubgraphStrategy strategy = SubgraphStrategy.build().edges(edgeCriterion).vertices(vertexCriterion).create();
        final GraphTraversalSource sg = g.withStrategies(strategy);

        // three vertices are included in the subgraph
        assertEquals(6, g.V().count().next().longValue());
        assertEquals(3, sg.V().count().next().longValue());

        // three edges are explicitly included, but one is missing its out-vertex due to the vertex criterion
        assertEquals(6, g.E().count().next().longValue());
        assertEquals(2, sg.E().count().next().longValue());

        // from vertex

        assertEquals(2, g.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).outE().count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).out().count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).inE().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in().count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).in().count().next().longValue());

        assertEquals(3, g.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE().count().next().longValue());
        assertEquals(3, g.V(convertToVertexId("josh")).both().count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both().count().next().longValue());

        // with label

        assertEquals(2, g.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).outE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).out("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).bothE("created").count().next().longValue());
        assertEquals(2, g.V(convertToVertexId("josh")).both("created").count().next().longValue());
        assertEquals(2, sg.V(convertToVertexId("josh")).both("created").count().next().longValue());

        assertEquals(1, g.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).inE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).in("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).bothE("knows").count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).both("knows").count().next().longValue());
        assertEquals(0, sg.V(convertToVertexId("josh")).both("knows").count().next().longValue());

        // with branch factor

        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE().limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE().limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE().limit(1)).inV().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE().limit(1)).inV().count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).count().next().longValue());
        assertEquals(1, g.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).inV().count().next().longValue());
        assertEquals(1, sg.V(convertToVertexId("josh")).local(bothE("knows", "created").limit(1)).inV().count().next().longValue());

        // from edge

        assertEquals(2, g.E(convertToEdgeId("marko", "created", "lop")).bothV().count().next().longValue());
        try {
            sg.E(convertToEdgeId("marko", "created", "lop")).next();
            fail("Edge 9 should not be in the graph because marko is not a vertex");
        } catch (Exception ex) {
            assertTrue(ex instanceof NoSuchElementException);
        }
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldFilterVertexCriterionAndKeepLabels() throws Exception {
        // this will exclude "peter"
        final Traversal<Vertex, ?> vertexCriterion = has("name", P.within("ripple", "josh", "marko"));

        final GraphTraversalSource sg = g.withStrategies(SubgraphStrategy.build().vertexCriterion(vertexCriterion).create());

        assertEquals(9, g.V().as("a").out().in().as("b").dedup("a", "b").count().next().intValue());
        assertEquals(2, sg.V().as("a").out().in().as("b").dedup("a", "b").count().next().intValue());

        final List<Object> list = sg.V().as("a").out().in().as("b").dedup("a", "b").values("name").toList();
        assertThat(list, hasItems("marko", "josh"));
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(MODERN)
    public void shouldGetExcludedVertex() throws Exception {
        final Traversal<Vertex, ?> vertexCriterion = has("name", P.within("josh", "lop", "ripple"));

        final GraphTraversalSource sg = g.withStrategies(SubgraphStrategy.build().vertexCriterion(vertexCriterion).create());

        sg.V(convertToVertexId("marko")).next();
    }

    @Test(expected = NoSuchElementException.class)
    @LoadGraphWith(MODERN)
    public void shouldGetExcludedEdge() throws Exception {
        final Traversal<Edge, ?> edgeCriterion = __.or(
                has("weight", 1.0d).hasLabel("knows"), // 8
                has("weight", 0.4d).hasLabel("created").outV().has("name", "marko"), // 9
                has("weight", 1.0d).hasLabel("created") // 10
        );

        final GraphTraversalSource sg = g.withStrategies(SubgraphStrategy.build().edges(edgeCriterion).create());

        sg.E(sg.E(convertToEdgeId("marko", "knows", "vadas")).next()).next();
    }

    @Test
    @LoadGraphWith(CREW)
    public void shouldFilterVertexProperties() throws Exception {
        GraphTraversalSource sg = g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTEX_PROPERTIES, __.has("startTime", P.gt(2005)));
        }})));
        checkResults(Arrays.asList("purcellville", "baltimore", "oakland", "seattle", "aachen"), sg.V().properties("location").value());
        checkResults(Arrays.asList("purcellville", "baltimore", "oakland", "seattle", "aachen"), sg.V().values("location"));
        if (sg.getStrategies().getStrategy(InlineFilterStrategy.class).isPresent())
            assertFalse(TraversalHelper.hasStepOfAssignableClassRecursively(TraversalFilterStep.class, sg.V().properties("location").value().iterate().asAdmin()));
        // check to make sure edge properties are not analyzed
        sg = g.withStrategies(SubgraphStrategy.build().vertexProperties(has("startTime", P.gt(2005))).create());
        checkResults(Arrays.asList("purcellville", "baltimore", "oakland", "seattle", "aachen"), sg.V().as("a").properties("location").as("b").select("a").outE().properties().select("b").value().dedup());
        checkResults(Arrays.asList("purcellville", "baltimore", "oakland", "seattle", "aachen"), sg.V().as("a").values("location").as("b").select("a").outE().properties().select("b").dedup());
        if (sg.getStrategies().getStrategy(InlineFilterStrategy.class).isPresent())
            assertFalse(TraversalHelper.hasStepOfAssignableClassRecursively(TraversalFilterStep.class, sg.V().as("a").values("location").as("b").select("a").outE().properties().select("b").dedup().iterate().asAdmin()));
        //
        sg = g.withStrategies(SubgraphStrategy.build().vertices(has("name", P.neq("stephen"))).vertexProperties(has("startTime", P.gt(2005))).create());
        checkResults(Arrays.asList("baltimore", "oakland", "seattle", "aachen"), sg.V().properties("location").value());
        checkResults(Arrays.asList("baltimore", "oakland", "seattle", "aachen"), sg.V().values("location"));
        //
        sg = g.withStrategies(SubgraphStrategy.build().vertices(has("name", P.not(P.within("stephen", "daniel")))).vertexProperties(has("startTime", P.gt(2005))).create());
        checkResults(Arrays.asList("baltimore", "oakland", "seattle"), sg.V().properties("location").value());
        checkResults(Arrays.asList("baltimore", "oakland", "seattle"), sg.V().values("location"));
        //
        sg = g.withStrategies(SubgraphStrategy.build().vertices(has("name", P.eq("matthias"))).vertexProperties(has("startTime", P.gte(2014))).create());
        checkResults(makeMapList(1, "seattle", 1L), sg.V().groupCount().by("location"));
        //
        sg = g.withStrategies(SubgraphStrategy.build().vertices(has("location")).vertexProperties(hasNot("endTime")).create());
        checkOrderedResults(Arrays.asList("aachen", "purcellville", "santa fe", "seattle"), sg.V().order().by("location", Order.asc).values("location"));
        //
        sg = g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put(SubgraphStrategy.VERTICES, __.has("location"));
            put(SubgraphStrategy.VERTEX_PROPERTIES, __.hasNot("endTime"));
        }})));
        checkResults(Arrays.asList("aachen", "purcellville", "santa fe", "seattle"), sg.V().valueMap("location").select(Column.values).unfold().unfold());
        checkResults(Arrays.asList("aachen", "purcellville", "santa fe", "seattle"), sg.V().propertyMap("location").select(Column.values).unfold().unfold().value());
        //
        sg = g.withStrategies(SubgraphStrategy.build().edges(__.<Edge>hasLabel("uses").has("skill", 5)).create());
        checkResults(Arrays.asList(5, 5, 5), sg.V().outE().valueMap().select(Column.values).unfold());
        checkResults(Arrays.asList(5, 5, 5), sg.V().outE().propertyMap().select(Column.values).unfold().value());
        //
        sg = g.withStrategies(SubgraphStrategy.build().vertexProperties(__.hasNot("skill")).create());
        checkResults(Arrays.asList(3, 3, 3, 4, 4, 5, 5, 5), sg.V().outE("uses").values("skill"));
        checkResults(Arrays.asList(3, 3, 3, 4, 4, 5, 5, 5), sg.V().as("a").properties().select("a").dedup().outE().values("skill"));
        checkResults(Arrays.asList(3, 3, 3, 4, 4, 5, 5, 5), sg.V().as("a").properties().select("a").dedup().outE().properties("skill").as("b").identity().select("b").by(__.value()));
    }

    @Test
    @LoadGraphWith(MODERN)
    public void shouldGenerateCorrectTraversers() throws Exception {

        assumeThat(graph, Matchers.not(Matchers.instanceOf(RemoteGraph.class)));

        GraphTraversalSource sg;
        GraphTraversal.Admin traversal;
        SubgraphStrategy strategy;

        strategy = SubgraphStrategy.build().vertices(has("name", P.within("josh", "lop", "ripple"))).create();
        sg = g.withStrategies(strategy);

        traversal = sg.V().outE().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = sg.V().outE().inV().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = sg.V().out().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);

        traversal = sg.V().bothE().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = sg.V().bothE().otherV().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_LP_O_P_S_SE_SL_TraverserGenerator);
        traversal = sg.V().both().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_LP_O_P_S_SE_SL_TraverserGenerator);

        traversal = sg.V().flatMap(bothE()).iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_O_TraverserGenerator);
        traversal = sg.V().flatMap(bothE().otherV()).iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_LP_O_P_S_SE_SL_TraverserGenerator);
        traversal = sg.V().flatMap(both()).iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_LP_O_P_S_SE_SL_TraverserGenerator);

        strategy = SubgraphStrategy.build().vertices(__.filter(__.simplePath())).create();
        sg = g.withStrategies(strategy);

        traversal = sg.V().out().iterate().asAdmin();
        assertTrue(traversal.getTraverserGenerator() instanceof B_LP_O_P_S_SE_SL_TraverserGenerator);
    }
}
