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

import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.GremlinLang;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.step.GValue;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphTraversalSourceTest {

    private static final GraphTraversalSource g = traversal().withEmbedded(EmptyGraph.instance());

    @Test
    public void shouldCloseRemoteConnectionOnWithRemote() throws Exception {
        final RemoteConnection mock = mock(RemoteConnection.class);
        final GraphTraversalSource g = traversal().withRemote(mock);
        g.close();

        verify(mock, times(1)).close();
    }

    @Test
    public void shouldSupportMapBasedStrategies() throws Exception {
        GraphTraversalSource g = EmptyGraph.instance().traversal();
        assertFalse(g.getStrategies().getStrategy(SubgraphStrategy.class).isPresent());
        g = g.withStrategies(SubgraphStrategy.create(new MapConfiguration(new HashMap<String, Object>() {{
            put("vertices", __.hasLabel("person"));
            put("vertexProperties", __.limit(0));
            put("edges", __.hasLabel("knows"));
        }})));
        assertTrue(g.getStrategies().getStrategy(SubgraphStrategy.class).isPresent());
        g = g.withoutStrategies(SubgraphStrategy.class);
        assertFalse(g.getStrategies().getStrategy(SubgraphStrategy.class).isPresent());
        //
        assertFalse(g.getStrategies().getStrategy(ReadOnlyStrategy.class).isPresent());
        g = g.withStrategies(ReadOnlyStrategy.instance());
        assertTrue(g.getStrategies().getStrategy(ReadOnlyStrategy.class).isPresent());
        g = g.withoutStrategies(ReadOnlyStrategy.class);
        assertFalse(g.getStrategies().getStrategy(ReadOnlyStrategy.class).isPresent());
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailAddVWithNullVertexLabel() {
        g.addV((String) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailAddVWithNullVertexLabelTraversal() {
        g.addV((Traversal<?, String>) null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailMergeEForBadInput() {
        g.mergeE(CollectionUtil.asMap(T.value, "nope"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldFailMergeVForBadInput() {
        g.mergeV(CollectionUtil.asMap(T.value, "nope"));
    }

    /**
     * This test demonstrates how one method for using Mockito to mock {@code g} to help with unit testing
     * applications where the logic of the query itself does not need to be validated, but rather surrounding
     * logic of the application. It is really important to note that all internal processing of the query is
     * ignored under this model and no TinkerPop code is executed so even invalid queries that are incorrect
     * will return the static data encoded to the mock.
     */
    @Test
    public void shouldDemonstrateMocking() {
        // intercept the terminating step of toList() and return whatever results needed for the nature of
        // the test. you could just have easily mocked next() or other terminators. they key to this model lies
        // in all other methods simply returning the mock itself so that the chaining aspect of hooking steps
        // together continues to return the mock GraphTraversal. be sure to mock whatever terminator that you
        // intend to use or it will fall into that default path and there will likely be an error or other
        // unexpected behavior
        final GraphTraversal<?,?> mockTraversal = mock(GraphTraversal.class, invocation -> {
            if (invocation.getMethod().getName().equals("toList"))
                return Arrays.asList("peter", "lop", "josh");
            else
                return invocation.getMock();
        });

        // the mock of GraphTraversalSource is basically "g". Your test would want to inject this "g" to your
        // application logic to use rather than a real one that is probably produced by traversal().withRemote(...).
        // the mock returns either itself or the mock of GraphTraversal depending on the return type of the method
        // called. all other calls go to the real method. that could result in errors but if you're just testing the
        // Gremlin language (and that's the use case here) this configuration should be sufficient.
        //
        // as an aside, it may be tempting to try to mock Graph so that it can be given directly to
        // traversal().withEmbedded(...) but that turns out to be hard based on how the construction happens in there
        // with reflection that seems to bypass the mock attempts. this approach seems simpler but just requires your
        // application logic to take the mock "g" somehow.
        final GraphTraversalSource mockG = mock(GraphTraversalSource.class, invocation -> {
            final Class<?> returnType = invocation.getMethod().getReturnType();
            if (returnType.isAssignableFrom(GraphTraversalSource.class)) {
                return invocation.getMock();
            } else if (returnType.isAssignableFrom(GraphTraversal.class)) {
                return mockTraversal;
            } else {
                return invocation.callRealMethod();
            }
        });

        final GraphTraversalSource g = mockG; // traversal().withRemote(...)
        final List<Object> l = g.V().values("name").order().by(Order.shuffle).limit(3).toList();
        assertThat(l, containsInAnyOrder("peter", "lop", "josh"));
    }

    @Test
    public void shouldContainReuseableGremlinLang() {
        final Pattern paramPatter = Pattern.compile("xx\\d+");
        assertThat(g.getGremlinLang().getOptionsStrategies().isEmpty(), is(true));
        g.with("a", "1").V();
        assertThat(g.getGremlinLang().getOptionsStrategies().isEmpty(), is(true));
        g.with("b", "2").V();
        assertThat(g.getGremlinLang().getOptionsStrategies().isEmpty(), is(true));

        assertThat(g.getGremlinLang().getParameters().isEmpty(), is(true));
        GremlinLang lang = g.V(GValue.of("xx1", 11)).asAdmin().getGremlinLang();
        assertThat(g.getGremlinLang().getParameters().isEmpty(), is(true));
        assertThat(lang.getParameters().size(), is(1));
        assertThat(lang.getParameters().values(), containsInAnyOrder(11));
        Matcher paramMatcher = paramPatter.matcher(lang.getGremlin());
        paramMatcher.find();
        assertThat(lang.getParameters().keySet(), containsInAnyOrder(paramMatcher.group()));

        lang = g.V(GValue.of("xx2", 22)).asAdmin().getGremlinLang();
        assertThat(g.getGremlinLang().getParameters().isEmpty(), is(true));
        assertThat(lang.getParameters().size(), is(1));
        assertThat(lang.getParameters().values(), containsInAnyOrder(22));
        paramMatcher = paramPatter.matcher(lang.getGremlin());
        paramMatcher.find();
        assertThat(lang.getParameters().keySet(), containsInAnyOrder(paramMatcher.group()));
    }
}
