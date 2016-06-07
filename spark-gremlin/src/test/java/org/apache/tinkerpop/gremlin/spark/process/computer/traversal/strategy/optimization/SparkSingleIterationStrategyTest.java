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

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkSingleIterationStrategyTest extends AbstractSparkTest {

    @Test
    public void shouldSuccessfullyEvaluateSingleIterationTraversals() throws Exception {
        final String outputLocation = TestHelper.makeTestDataDirectory(SparkSingleIterationStrategyTest.class, UUID.randomUUID().toString());
        Configuration configuration = getBaseConfiguration();
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_HADOOP_DEFAULT_GRAPH_COMPUTER, SparkGraphComputer.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);

        Graph graph = GraphFactory.open(configuration);
        GraphTraversalSource g = graph.traversal().withComputer().withoutStrategies(SparkInterceptorStrategy.class);
        assertFalse(g.getStrategies().toList().contains(SparkInterceptorStrategy.instance()));
        assertFalse(g.V().count().explain().toString().contains(SparkInterceptorStrategy.class.getSimpleName()));
        assertTrue(g.getStrategies().toList().contains(SparkSingleIterationStrategy.instance()));
        assertTrue(g.V().count().explain().toString().contains(SparkSingleIterationStrategy.class.getSimpleName()));

        test(true, g.V().limit(10));
        test(true, g.V().values("age").groupCount());
        test(true, g.V().groupCount().by(__.out().count()));
        test(true, g.V().outE());
        test(true, 6l, g.V().count());
        test(true, 6l, g.V().out().count());
        test(true, 6l, g.V().local(__.inE()).count());
        test(true, 6l, g.V().outE().inV().count());
        ////
        test(false, g.V().outE().inV());
        test(false, g.V().both());
        test(false, 12l, g.V().both().count());
        test(false, g.V().out().id());
        test(false, 2l, g.V().out().out().count());
        test(false, 6l, g.V().in().count());
        test(false, 6l, g.V().inE().count());
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
