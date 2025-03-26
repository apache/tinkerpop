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
package org.apache.tinkerpop.gremlin.process.traversal.dsl.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTraversalTest {
    private static final Logger logger = LoggerFactory.getLogger(GraphTraversalTest.class);
    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());

    private static Set<String> NO_GRAPH = new HashSet<>(Arrays.asList("asAdmin", "by", "read", "write", "with", "option", "iterate", "to", "from", "profile", "pageRank", "connectedComponent", "peerPressure", "shortestPath", "program", "discard"));
    private static Set<String> NO_ANONYMOUS = new HashSet<>(Arrays.asList("start", "__"));

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailPropertyWithNullVertexId() {
        g.addV().property(T.id, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailPropertyWithNullVertexLabel() {
        g.addV().property(T.label, null);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailWhenUsingOptionAndCardinalityArgumentWithoutMergeV() {
        final Map<Object, Object> m = new HashMap<Object,Object>() {{
            put("k", 1);
        }};
        g.mergeE(new HashMap<>()).option(Merge.onMatch, m, VertexProperty.Cardinality.list);
    }

    @Test(expected = IllegalStateException.class)
    public void shouldFailWhenUsingOptionAndCardinalityFunctionWithoutMergeV() {
        final Map<Object, Object> m = new HashMap<Object,Object>() {{
            put("k", VertexProperty.Cardinality.list(1));
        }};
        g.mergeE(new HashMap<>()).option(Merge.onMatch, m);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailPropertyWithCardinalityNullVertexId() {
        g.addV().property(VertexProperty.Cardinality.single, T.id, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailPropertyWithCardinalityNullVertexLabel() {
        g.addV().property(VertexProperty.Cardinality.single, T.label, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailMergeEWithBadInput() {
        g.inject(0).mergeE(CollectionUtil.asMap(T.value, 100));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailMergeVWithBadInput() {
        g.inject(0).mergeV(CollectionUtil.asMap(T.value, 100));
    }

    @Test
    public void shouldHaveMethodsOfGraphTraversalOnAnonymousGraphTraversal() {
        for (Method methodA : GraphTraversal.class.getMethods()) {
            if (Traversal.class.isAssignableFrom(methodA.getReturnType()) && !NO_GRAPH.contains(methodA.getName())) {
                boolean found = false;
                final String methodAName = methodA.getName();
                final String methodAParameters = Arrays.asList(methodA.getParameterTypes()).toString();
                for (final Method methodB : __.class.getMethods()) {
                    final String methodBName = methodB.getName();
                    final String methodBParameters = Arrays.asList(methodB.getParameterTypes()).toString();
                    if (methodAName.equals(methodBName) && methodAParameters.equals(methodBParameters))
                        found = true;
                }
                if (!found)
                    throw new IllegalStateException(__.class.getSimpleName() + " is missing the following method: " + methodAName + ":" + methodAParameters);
            }
        }
    }

    @Test
    public void shouldHaveMethodsOfAnonymousGraphTraversalOnGraphTraversal() {
        for (Method methodA : __.class.getMethods()) {
            if (Traversal.class.isAssignableFrom(methodA.getReturnType()) && !NO_ANONYMOUS.contains(methodA.getName())) {
                boolean found = false;
                final String methodAName = methodA.getName();
                final String methodAParameters = Arrays.asList(methodA.getParameterTypes()).toString();
                for (final Method methodB : GraphTraversal.class.getMethods()) {
                    final String methodBName = methodB.getName();
                    final String methodBParameters = Arrays.asList(methodB.getParameterTypes()).toString();
                    if (methodAName.equals(methodBName) && methodAParameters.equals(methodBParameters))
                        found = true;
                }
                if (!found)
                    throw new IllegalStateException(GraphTraversal.class.getSimpleName() + " is missing the following method: " + methodAName + ":" + methodAParameters);
            }
        }
    }

    @Test
    public void hasIdShouldUnrollListOfIds() {
        assertEquals(g.V().hasId(1,2,3,4,5,6,7,8),
                g.V().hasId(new Integer[]{1, 2, 3}, new Integer[]{4, 5}, Arrays.asList(6, 7), 8));
    }

    @Test
    public void hasIdShouldUnrollGValueListOfIds() {
        assertEquals(
                g.V().hasId(
                        GValue.ofInteger(null, 1), GValue.ofInteger(null, 2), GValue.ofInteger(null, 3),
                        GValue.ofInteger(null, 4), GValue.ofInteger(null, 5), GValue.ofInteger(null, 6),
                        GValue.ofInteger(null, 7), GValue.ofInteger("d", 8)
                ),
                g.V().hasId(
                        GValue.of("a", new Integer[]{1, 2, 3}), GValue.of("b", new Integer[]{4, 5}),
                        GValue.of("c", Arrays.asList(6, 7)), GValue.ofInteger("d", 8)));
    }
}
