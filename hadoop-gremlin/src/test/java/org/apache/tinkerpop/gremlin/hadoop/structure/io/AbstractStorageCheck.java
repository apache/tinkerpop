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

package org.apache.tinkerpop.gremlin.hadoop.structure.io;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class AbstractStorageCheck extends AbstractGremlinTest {

    public void checkHeadMethods(final Storage storage, final String inputLocation, final String outputLocation, final Class outputGraphParserClass, final Class outputMemoryParserClass) throws Exception {
        // TEST INPUT GRAPH
        assertFalse(storage.exists(outputLocation));
        if (inputLocation.endsWith(".json") && storage.exists(inputLocation)) { // gryo is not text readable
            assertEquals(6, IteratorUtils.count(storage.head(inputLocation)));
            for (int i = 0; i < 7; i++) {
                assertEquals(i, IteratorUtils.count(storage.head(inputLocation, i)));
            }
            assertEquals(6, IteratorUtils.count(storage.head(inputLocation, 10)));
        }

        ////////////////////

        final ComputerResult result = graph.compute(graphComputerClass.get()).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();
        // TEST OUTPUT GRAPH
        assertTrue(storage.exists(outputLocation));
        assertTrue(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertEquals(6, result.graph().traversal().V().count().next().longValue());
        assertEquals(0, result.graph().traversal().E().count().next().longValue());
        assertEquals(6, result.graph().traversal().V().values("name").count().next().longValue());
        assertEquals(6, result.graph().traversal().V().values(PeerPressureVertexProgram.CLUSTER).count().next().longValue());
        assertEquals(2, result.graph().traversal().V().values(PeerPressureVertexProgram.CLUSTER).dedup().count().next().longValue());
        assertEquals(6, IteratorUtils.count(storage.head(Constants.getGraphLocation(outputLocation), outputGraphParserClass)));
        for (int i = 0; i < 7; i++) {
            assertEquals(i, IteratorUtils.count(storage.head(Constants.getGraphLocation(outputLocation), outputGraphParserClass, i)));
        }
        assertEquals(6, IteratorUtils.count(storage.head(Constants.getGraphLocation(outputLocation), outputGraphParserClass, 346)));
        /////
        // TEST MEMORY PERSISTENCE
        assertEquals(2, (int) result.memory().get("clusterCount"));
        assertTrue(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertEquals(1, IteratorUtils.count(storage.head(outputLocation, "clusterCount", outputMemoryParserClass)));
        assertEquals(2, storage.head(outputLocation, "clusterCount", outputMemoryParserClass).next().getValue());
    }

    public void checkRemoveAndListMethods(final Storage storage, final String outputLocation) throws Exception {
        graph.compute(graphComputerClass.get()).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();
        assertTrue(storage.exists(outputLocation));
        assertTrue(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertTrue(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertEquals(2, storage.ls(outputLocation).size());
        assertTrue(storage.rmr(Constants.getGraphLocation(outputLocation)));
        assertEquals(1, storage.ls(outputLocation).size());
        assertTrue(storage.rmr(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertEquals(0, storage.ls(outputLocation).size());
        assertFalse(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertFalse(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        if (storage.exists(outputLocation))
            assertTrue(storage.rmr(outputLocation));
        assertFalse(storage.exists(outputLocation));

        ////////////////

        graph.compute(graphComputerClass.get()).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();
        assertTrue(storage.exists(outputLocation));
        assertTrue(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertTrue(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertEquals(2, storage.ls(outputLocation).size());
        assertTrue(storage.rmr(outputLocation));
        assertFalse(storage.exists(outputLocation));
        assertEquals(0, storage.ls(outputLocation).size());
    }

    public void checkCopyMethods(final Storage storage, final String outputLocation, final String newOutputLocation) throws Exception {
        graph.compute(graphComputerClass.get()).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();
        assertTrue(storage.exists(outputLocation));
        assertTrue(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertTrue(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertFalse(storage.exists(newOutputLocation));

        assertTrue(storage.cp(outputLocation, newOutputLocation));
        assertTrue(storage.exists(outputLocation));
        assertTrue(storage.exists(Constants.getGraphLocation(outputLocation)));
        assertTrue(storage.exists(Constants.getMemoryLocation(outputLocation, "clusterCount")));
        assertTrue(storage.exists(newOutputLocation));
        assertTrue(storage.exists(Constants.getGraphLocation(newOutputLocation)));
        assertTrue(storage.exists(Constants.getMemoryLocation(newOutputLocation, "clusterCount")));
    }
}
