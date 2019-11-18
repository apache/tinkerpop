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

package org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization;

import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.VertexProgramInterceptor;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.process.computer.traversal.strategy.optimization.interceptor.SparkStarBarrierInterceptor;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkInterceptorStrategyTest extends AbstractSparkTest {

    @Test
    public void shouldHandleSideEffectsCorrectly() throws Exception {
        final Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(SparkSingleIterationStrategyTest.class, UUID.randomUUID().toString()));
        configuration.setProperty(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        ///
        Graph graph = GraphFactory.open(configuration);
        GraphTraversalSource g = graph.traversal().withComputer().withoutStrategies(SparkSingleIterationStrategy.class);
        assertFalse(g.getStrategies().getStrategy(SparkSingleIterationStrategy.class).isPresent());
        assertFalse(g.V().count().explain().toString().contains(SparkSingleIterationStrategy.class.getSimpleName()));
        assertTrue(g.getStrategies().getStrategy(SparkInterceptorStrategy.class).isPresent());
        assertTrue(g.V().count().explain().toString().contains(SparkInterceptorStrategy.class.getSimpleName()));
        /// groupCount(m)-test
        Traversal.Admin<Vertex, Long> traversal = g.V().groupCount("m").by(T.label).count().asAdmin();
        test(SparkStarBarrierInterceptor.class, 6l, traversal);
        assertEquals(1, traversal.getSideEffects().keys().size());
        assertTrue(traversal.getSideEffects().exists("m"));
        assertTrue(traversal.getSideEffects().keys().contains("m"));
        final Map<String, Long> map = traversal.getSideEffects().get("m");
        assertEquals(2, map.size());
        assertEquals(2, map.get("software").intValue());
        assertEquals(4, map.get("person").intValue());
    }

    @Test
    public void shouldSuccessfullyEvaluateInterceptedTraversals() throws Exception {
        final Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(SparkSingleIterationStrategyTest.class, UUID.randomUUID().toString()));
        configuration.setProperty(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        ///
        Graph graph = GraphFactory.open(configuration);
        GraphTraversalSource g = graph.traversal().withComputer().withoutStrategies(SparkSingleIterationStrategy.class);
        assertFalse(g.getStrategies().getStrategy(SparkSingleIterationStrategy.class).isPresent());
        assertFalse(g.V().count().explain().toString().contains(SparkSingleIterationStrategy.class.getSimpleName()));
        assertTrue(g.getStrategies().getStrategy(SparkInterceptorStrategy.class).isPresent());
        assertTrue(g.V().count().explain().toString().contains(SparkInterceptorStrategy.class.getSimpleName()));
        /// SparkCountInterceptor matches
        test(SparkStarBarrierInterceptor.class, 6l, g.V().count());
        test(SparkStarBarrierInterceptor.class, 2l, g.V().hasLabel("software").count());
        test(SparkStarBarrierInterceptor.class, 2l, g.V().hasLabel("person").has("age", P.gt(30)).count());
        test(SparkStarBarrierInterceptor.class, 2l, g.V().hasLabel("person").has("age", P.gt(30)).values("name").count());
        test(SparkStarBarrierInterceptor.class, 2l, g.V().hasLabel("person").has("age", P.gt(30)).properties("name").count());
        test(SparkStarBarrierInterceptor.class, 4l, g.V().hasLabel("person").has("age", P.gt(30)).properties("name", "age").count());
        test(SparkStarBarrierInterceptor.class, 3l, g.V().hasLabel("person").has("age", P.gt(30)).out().count());
        test(SparkStarBarrierInterceptor.class, 0l, g.V().hasLabel("person").has("age", P.gt(30)).out("knows").count());
        test(SparkStarBarrierInterceptor.class, 3l, g.V().has(T.label, P.not(P.within("robot", "android")).and(P.within("person", "software"))).hasLabel("person").has("age", P.gt(30)).out("created").count());
        test(SparkStarBarrierInterceptor.class, 3l, g.V(1).out().count());
        test(SparkStarBarrierInterceptor.class, 2l, g.V(1).out("knows").count());
        test(SparkStarBarrierInterceptor.class, 3l, g.V(1).out("knows", "created").count());
        test(SparkStarBarrierInterceptor.class, 5l, g.V(1, 4).out("knows", "created").count());
        test(SparkStarBarrierInterceptor.class, 1l, g.V(2).in("knows").count());
        test(SparkStarBarrierInterceptor.class, 0l, g.V(6).has("name", "peter").in().count());
        test(SparkStarBarrierInterceptor.class, 6l, g.V().as("a").values("name").as("b").count());
        test(SparkStarBarrierInterceptor.class, 6l, g.V().as("a").count());
        test(SparkStarBarrierInterceptor.class, 1l, g.V().has("name", "marko").as("a").values("name").as("b").count());
        test(SparkStarBarrierInterceptor.class, 4l, g.V().has(T.label, P.not(P.within("robot", "android")).and(P.within("person", "software"))).hasLabel("person").has("age").out("created").count());
        test(SparkStarBarrierInterceptor.class, 123l, g.V().has("age").values("age").sum());
        test(SparkStarBarrierInterceptor.class, 67l, g.V().has("age").has("age", P.gt(30)).values("age").sum());
        test(SparkStarBarrierInterceptor.class, 27, g.V().hasLabel("person").values("age").min());
        test(SparkStarBarrierInterceptor.class, 35, g.V().hasLabel("person").values("age").max());
        test(SparkStarBarrierInterceptor.class, new HashMap<String, Long>() {{
            put("software", 2l);
            put("person", 4l);
        }}, g.V().<String>groupCount().by(T.label));
        test(SparkStarBarrierInterceptor.class, Collections.singletonMap("person", 2l), g.V().has("person", "age", P.lt(30)).<String>groupCount().by(T.label));
        test(SparkStarBarrierInterceptor.class, new HashMap<String, Long>() {{
            put("software", 2l);
            put("person", 4l);
        }}, g.V().<String, Long>group().by(T.label).by(__.count()));
        test(SparkStarBarrierInterceptor.class, 123l, g.V().hasLabel("person").values("age").fold(0l, Operator.sum));
        /// No interceptor matches
        test(2l, g.V().out().out().count());
        test(6l, g.E().count());
        test(2l, g.V().out().out().count());
        test(6l, g.V().out().values("name").count());
        test(2l, g.V().out("knows").values("name").count());
        test(3l, g.V().in().has("name", "marko").count());
        test(0l, g.V().repeat(__.dedup()).times(2).count());
        test(6l, g.V().dedup().count());
        test(4l, g.V().hasLabel("person").order().by("age").count());
        test(1l, g.V().count().count());
        test(2l, g.V().limit(2).count());
        test(3l, g.V().tail(3).count());
    }

    private static <R> void test(Class<? extends VertexProgramInterceptor> expectedInterceptor, R expectedResult, final Traversal<?, R> traversal) throws Exception {
        final Traversal.Admin<?, ?> clone = traversal.asAdmin().clone();
        clone.applyStrategies();
        final Map<String, Object> configuration = TraversalHelper.getFirstStepOfAssignableClass(TraversalVertexProgramStep.class, clone).get().getComputer().getConfiguration();
        final String interceptor = (String) configuration.getOrDefault(Constants.GREMLIN_HADOOP_VERTEX_PROGRAM_INTERCEPTOR, null);
        if (null == expectedInterceptor) {
            assertNull(interceptor);
            assertFalse((Boolean) configuration.getOrDefault(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false));
            assertFalse((Boolean) configuration.getOrDefault(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false));
        } else {
            assertEquals(expectedInterceptor, Class.forName(interceptor));
            if (interceptor.equals(SparkStarBarrierInterceptor.class.getCanonicalName())) {
                assertTrue((Boolean) configuration.getOrDefault(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false));
                assertTrue((Boolean) configuration.getOrDefault(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false));
            }
        }
        assertEquals(expectedResult, traversal.next());
    }

    private static <R> void test(R expectedResult, final Traversal<?, R> traversal) throws Exception {
        test(null, expectedResult, traversal);
    }
}
