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

package org.apache.tinkerpop.gremlin.spark.structure.io;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.spark.AbstractSparkTest;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkHadoopGraphProvider;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class SparkContextStorageTest extends AbstractSparkTest {

    @Test
    public void shouldPersistGraphAndMemory() throws Exception {
        final String outputLocation = "target/test-output/" + UUID.randomUUID();
        final Configuration configuration = getBaseConfiguration(SparkHadoopGraphProvider.PATHS.get("tinkerpop-modern.kryo"));
        configuration.setProperty(Constants.GREMLIN_SPARK_GRAPH_OUTPUT_RDD, PersistedOutputRDD.class.getCanonicalName());
        configuration.setProperty(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION, outputLocation);
        configuration.setProperty(Constants.GREMLIN_SPARK_PERSIST_CONTEXT, true);
        /////
        Graph graph = GraphFactory.open(configuration);
        final ComputerResult result = graph.compute(SparkGraphComputer.class).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();
        /////
        final Storage storage = SparkContextStorage.open("local[4]");

        assertEquals(2, storage.ls().size());
        // TEST GRAPH PERSISTENCE
        assertTrue(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertEquals(6, IteratorUtils.count(storage.headGraph(outputLocation, PersistedInputRDD.class)));
        assertEquals(6, result.graph().traversal().V().count().next().longValue());
        assertEquals(0, result.graph().traversal().E().count().next().longValue());
        assertEquals(6, result.graph().traversal().V().values("name").count().next().longValue());
        assertEquals(6, result.graph().traversal().V().values(PeerPressureVertexProgram.CLUSTER).count().next().longValue());
        /////
        // TEST MEMORY PERSISTENCE
        assertEquals(2, (int) result.memory().get("clusterCount"));
        assertTrue(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertEquals(2, storage.headMemory(outputLocation, "clusterCount", PersistedInputRDD.class).next().getValue());
    }

}
