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

package org.apache.tinkerpop.gremlin.hadoop.groovy.plugin;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.FileSystemStorage;
import org.apache.tinkerpop.gremlin.hadoop.structure.util.ConfUtil;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FileSystemStorageCheck extends AbstractGremlinTest {

    @Test
    @LoadGraphWith(LoadGraphWith.GraphData.MODERN)
    public void shouldPersistGraphAndMemory() throws Exception {
        final ComputerResult result = graph.compute(graphComputerClass.get()).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();
        /////
        final Storage storage = FileSystemStorage.open(ConfUtil.makeHadoopConfiguration(graph.configuration()));
        // TEST GRAPH PERSISTENCE
        assertTrue(storage.exists(Constants.getGraphLocation(graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION))));
        assertEquals(6, result.graph().traversal().V().count().next().longValue());
        assertEquals(0, result.graph().traversal().E().count().next().longValue());
        assertEquals(6, result.graph().traversal().V().values("name").count().next().longValue());
        assertEquals(6, result.graph().traversal().V().values(PeerPressureVertexProgram.CLUSTER).count().next().longValue());
        assertEquals(2, result.graph().traversal().V().values(PeerPressureVertexProgram.CLUSTER).dedup().count().next().longValue());
        /////
        // TEST MEMORY PERSISTENCE
        assertEquals(2, (int) result.memory().get("clusterCount"));
        assertTrue(storage.exists(Constants.getMemoryLocation(graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), "clusterCount")));
        assertEquals(1, IteratorUtils.count(storage.head(graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), "clusterCount", SequenceFileInputFormat.class)));
        assertEquals(2, storage.head(graph.configuration().getString(Constants.GREMLIN_HADOOP_OUTPUT_LOCATION), "clusterCount", SequenceFileInputFormat.class).next().getValue());
    }
}