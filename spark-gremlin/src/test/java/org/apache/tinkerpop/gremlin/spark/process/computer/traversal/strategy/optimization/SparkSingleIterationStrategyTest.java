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
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoPool;
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoVersion;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import static org.apache.tinkerpop.gremlin.structure.Column.keys;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
@RunWith(Parameterized.class)
public class SparkSingleIterationStrategyTest extends AbstractSparkTest {
    @Parameterized.Parameters(name = "expect({0})")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"V1d0", GryoVersion.V1_0},
                {"V3d0", GryoVersion.V3_0}});
    }

    @Parameterized.Parameter(value = 0)
    public String name;

    @Parameterized.Parameter(value = 1)
    public GryoVersion version;

    @Test
    public void shouldSuccessfullyEvaluateSingleIterationTraversals() throws Exception {
        final String outputLocation = TestHelper.makeTestDataDirectory(SparkSingleIterationStrategyTest.class, UUID.randomUUID().toString());
        Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("tinkerpop-modern-v3d0.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        configuration.setProperty(GryoPool.CONFIG_IO_GRYO_VERSION, version.name());

        /////////// WITHOUT SINGLE-ITERATION STRATEGY LESS SINGLE-PASS OPTIONS ARE AVAILABLE

        Graph graph = GraphFactory.open(configuration);
        GraphTraversalSource g = graph.traversal().withComputer().withoutStrategies(SparkInterceptorStrategy.class, MessagePassingReductionStrategy.class);
        assertFalse(g.getStrategies().getStrategy(SparkInterceptorStrategy.class).isPresent());
        assertFalse(g.V().count().explain().getStrategyTraversals().stream().filter(pair -> pair.getValue0() instanceof SparkInterceptorStrategy).findAny().isPresent());
        assertFalse(g.getStrategies().getStrategy(MessagePassingReductionStrategy.class).isPresent());
        assertFalse(g.V().count().explain().getStrategyTraversals().stream().filter(pair -> pair.getValue0() instanceof MessagePassingReductionStrategy).findAny().isPresent());
        assertTrue(g.getStrategies().getStrategy(SparkSingleIterationStrategy.class).isPresent());
        assertTrue(g.V().count().explain().getStrategyTraversals().stream().filter(pair -> pair.getValue0() instanceof SparkSingleIterationStrategy).findAny().isPresent());

        test(true, g.V().limit(10));
        test(true, g.V().values("age").groupCount());
        test(true, g.V().groupCount().by(__.out().count()));
        test(true, g.V().outE());
        test(true, 6L, g.V().count());
        test(true, 6L, g.V().out().count());
        test(true, 6L, g.V().outE().inV().count());
        ////
        test(false, 6L, g.V().local(__.inE()).count());
        test(false, g.V().outE().inV());
        test(false, g.V().both());
        test(false, 12L, g.V().both().count());
        test(false, g.V().out().id());
        test(false, 2L, g.V().out().out().count());
        test(false, 6L, g.V().in().count());
        test(false, 6L, g.V().inE().count());

        /////////// WITH SINGLE-ITERATION STRATEGY MORE SINGLE-PASS OPTIONS ARE AVAILABLE

        graph = GraphFactory.open(configuration);
        g = graph.traversal().withComputer().withoutStrategies(SparkInterceptorStrategy.class).withStrategies(MessagePassingReductionStrategy.instance());
        assertFalse(g.getStrategies().getStrategy(SparkInterceptorStrategy.class).isPresent());
        assertFalse(g.V().count().explain().getStrategyTraversals().stream().filter(pair -> pair.getValue0() instanceof SparkInterceptorStrategy).findAny().isPresent());
        assertTrue(g.getStrategies().getStrategy(MessagePassingReductionStrategy.class).isPresent());
        assertTrue(g.V().count().explain().getStrategyTraversals().stream().filter(pair -> pair.getValue0() instanceof MessagePassingReductionStrategy).findAny().isPresent());
        assertTrue(g.getStrategies().getStrategy(SparkSingleIterationStrategy.class).isPresent());
        assertTrue(g.V().count().explain().getStrategyTraversals().stream().filter(pair -> pair.getValue0() instanceof SparkSingleIterationStrategy).findAny().isPresent());

        test(true, g.V().limit(10));
        test(true, g.V().values("age").groupCount());
        test(true, g.V().groupCount().by(__.out().count()));
        test(true, g.V().outE());
        test(true, 6L, g.V().outE().values("weight").count());
        test(true, 6L, g.V().inE().values("weight").count());
        test(true, 12L, g.V().bothE().values("weight").count());
        test(true, g.V().bothE().values("weight"));
        test(true, g.V().bothE().values("weight").limit(2));
        test(true, 6L, g.V().count());
        test(true, 6L, g.V().id().count());
        test(true, 6L, g.V().identity().outE().identity().count());
        test(true, 6L, g.V().identity().outE().has("weight").count());
        test(true, 6L, g.V().out().count());
        test(true, 6L, g.V().outE().inV().count());
        test(true, 6L, g.V().outE().inV().id().count());
        test(true, 2L, g.V().outE().inV().id().groupCount().select(Column.values).unfold().dedup().count());
        test(true, g.V().out().id());
        test(true, 6L, g.V().outE().valueMap().count());
        test(true, g.V().outE().valueMap());
        test(true, 6L, g.V().inE().valueMap().count());
        test(true, g.V().inE().valueMap());
        test(true, 12L, g.V().bothE().valueMap().count());
        test(true, g.V().bothE().valueMap());
        test(true, 6L, g.V().inE().id().count());
        test(true, 6L, g.V().outE().count());
        test(true, 4L, g.V().outE().inV().id().dedup().count());
        test(true, 4L, g.V().filter(__.in()).count());
        test(true, 6L, g.V().sideEffect(__.in()).count());
        test(true, 6L, g.V().map(__.constant("hello")).count());
        test(true, g.V().groupCount());
        test(true, g.V().groupCount("x"));
        test(true, g.V().groupCount("x").cap("x"));
        test(true, g.V().id().groupCount("x").cap("x"));
        test(true, g.V().outE().groupCount());
        test(true, g.V().outE().groupCount().by("weight"));
        test(true, g.V().inE().id().groupCount());
        test(true, g.V().inE().values("weight").groupCount());
        test(true, 6L, g.V().outE().outV().count());
        test(true, g.V().out().id().groupCount("x"));
        test(true, g.V().inE().values("weight").groupCount("x"));
        test(true, 6L, g.V().in().count());
        test(true, 12L, g.V().both().count());
        test(true, 6L, g.V().flatMap(__.in()).count());
        test(true, 4L, g.V().map(__.in()).count());
        test(true, 6L, g.V().inE().count());
        test(true, 4L, g.V().outE().inV().dedup().count());
        /////
        test(false, 6L, g.V().as("a").outE().inV().as("b").id().dedup("a", "b").by(T.id).count());
        test(false, 6L, g.V().local(__.inE()).count());
        test(false, 4L, g.V().outE().inV().dedup().by("name").count());
        test(false, 6L, g.V().local(__.in()).count());
        test(false, g.V().outE().inV());
        test(false, g.V().both());
        test(false, g.V().outE().inV().dedup());
        test(false, 2L, g.V().out().out().count());
        test(false, 6L, g.V().as("a").map(__.both()).select("a").count());
        test(false, g.V().out().values("name"));
        test(false, g.V().out().properties("name"));
        test(false, g.V().out().valueMap());
        test(false, 6L, g.V().as("a").outE().inV().values("name").as("b").dedup("a", "b").count());
        test(false, 2L, g.V().outE().inV().groupCount().select(Column.values).unfold().dedup().count());
        test(false, g.V().out().groupCount("x"));
        test(false, g.V().out().groupCount("x").cap("x"));
        test(false, 6L, g.V().both().groupCount("x").cap("x").select(keys).unfold().count());
        test(false, g.V().outE().inV().groupCount());
        test(false, g.V().outE().unfold().inV().groupCount());
        test(false, g.V().outE().inV().groupCount().by("name"));
        test(false, g.V().outE().inV().tree());
        test(false, g.V().outE().inV().id().tree());
        test(false, g.V().inE().groupCount());
        test(false, g.V().inE().groupCount().by("weight"));
        test(false, g.V().in().values("name").groupCount());
        test(false, g.V().out().groupCount("x"));
        test(false, g.V().in().groupCount("x"));
        test(false, g.V().both().groupCount("x").cap("x"));
    }

    private static <R> void test(boolean singleIteration, final Traversal<?, R> traversal) {
        test(singleIteration, null, traversal);
    }

    private static <R> void test(boolean singleIteration, R expectedResult, final Traversal<?, R> traversal) {
        traversal.asAdmin().applyStrategies();
        final Map<String, Object> configuration = TraversalHelper.getFirstStepOfAssignableClass(TraversalVertexProgramStep.class, traversal.asAdmin()).get()
                .getComputer()
                .getConfiguration();
        assertEquals(singleIteration, configuration.getOrDefault(Constants.GREMLIN_SPARK_SKIP_PARTITIONER, false));
        assertEquals(singleIteration, configuration.getOrDefault(Constants.GREMLIN_SPARK_SKIP_GRAPH_CACHE, false));
        final List<R> result = traversal.toList();
        if (null != expectedResult)
            assertEquals(expectedResult, result.get(0));
    }

}
