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

package org.apache.tinkerpop.gremlin.spark.structure;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.rdd.RDD;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.features.TestFiles;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.HadoopGraph;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.GryoInputFormat;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.spark.structure.io.PersistedOutputRDD;
import org.apache.tinkerpop.gremlin.spark.structure.io.gryo.GryoSerializer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Test;
import scala.collection.JavaConversions;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkTest extends AbstractSparkTest {
    @Test
    public void testCustomizedSparkAppName() {
	final String appName = "SparkAppNameTest";
	final org.apache.spark.SparkConf sparkConfiguration = new org.apache.spark.SparkConf();
	sparkConfiguration.set("spark.app.name", appName);
	sparkConfiguration.set("spark.master", "local[4]");
	final org.apache.spark.SparkContext sparkContext = Spark.create(sparkConfiguration);
	assertEquals(appName, sparkContext.getConf().get("spark.app.name"));
    }

    @Test
    public void testDefaultSparkAppName() {
	final org.apache.spark.SparkConf sparkConfiguration = new org.apache.spark.SparkConf();
	sparkConfiguration.set("spark.master", "local[4]");
	final org.apache.spark.SparkContext sparkContext = Spark.create(sparkConfiguration);
	assertEquals(Spark.DEFAULT_APP_NAME, sparkContext.getConf().get("spark.app.name"));
    }

    @Test
    public void testSparkRDDPersistence() throws Exception {
        final String prefix = TestHelper.makeTestDataFile(SparkTest.class, "testSparkRDDPersistence", "graphRDD-");
        final Configuration configuration = new BaseConfiguration();
        configuration.setProperty("spark.master", "local[4]");
        Spark.create(configuration);

        configuration.setProperty("spark.serializer", GryoSerializer.class.getCanonicalName());
        configuration.setProperty(Graph.GRAPH, HadoopGraph.class.getName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_INPUT_LOCATION, TestFiles.PATHS.get("tinkerpop-modern-v3.kryo"));
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_READER, GryoInputFormat.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_GRAPH_WRITER, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_JARS_IN_DISTRIBUTED_CACHE, false);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);

        for (int i = 0; i < 10; i++) {
            final String graphRDDName = Constants.getGraphLocation(prefix + i);
            assertEquals(i, Spark.getRDDs().size());
            configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, prefix + i);
            Graph graph = GraphFactory.open(configuration);
            graph.compute(SparkGraphComputer.class)
                .persist(GraphComputer.Persist.VERTEX_PROPERTIES)
                .vertexProperties(__.properties("dummy"))
                .program(PageRankVertexProgram.build().iterations(1).create(graph)).submit().get();
            assertNotNull(Spark.getRDD(graphRDDName));
            assertEquals(i + 1, Spark.getRDDs().size());
        }

        for (int i = 9; i >= 0; i--) {
            final String graphRDDName = Constants.getGraphLocation(prefix + i);
            assertEquals(i + 1, getPersistedRDDSize());
            assertEquals(i + 1, Spark.getRDDs().size());
            assertTrue(hasPersistedRDD(graphRDDName));
            Spark.removeRDD(graphRDDName);
            assertFalse(hasPersistedRDD(graphRDDName));
        }

        assertEquals(0, getPersistedRDDSize());
        assertEquals(0, Spark.getRDDs().size());
        Spark.close();
    }

    private static boolean hasPersistedRDD(final String name) {
        for (final RDD<?> rdd : JavaConversions.asJavaIterable(Spark.getContext().persistentRdds().values())) {
            if (null != rdd.name() && rdd.name().equals(name))
                return true;
        }
        return false;
    }

    private static int getPersistedRDDSize() {
        int counter = 0;
        for (final RDD<?> rdd : JavaConversions.asJavaIterable(Spark.getContext().persistentRdds().values())) {
            if (null != rdd.name())
                counter++;
        }
        return counter;
    }

}
