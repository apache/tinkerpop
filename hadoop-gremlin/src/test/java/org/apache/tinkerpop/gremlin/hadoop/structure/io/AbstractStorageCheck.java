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
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.hadoop.Constants;
import org.apache.tinkerpop.gremlin.process.computer.ComputerResult;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.ClusterCountMapReduce;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Map;

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

        final ComputerResult result = graphProvider.getGraphComputer(graph).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();

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
        final String graphLocation = Constants.getGraphLocation(outputLocation);
        final String memoryLocation= Constants.getMemoryLocation(outputLocation, "clusterCount");
        final GraphComputer graphComputer = graphProvider.getGraphComputer(graph);

        graphComputer.program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();

        assertTrue("storage.exists(outputLocation)", storage.exists(outputLocation));
        assertTrue("storage.exists(graphLocation)", storage.exists(graphLocation));

        assertTrue("storage.exists(memoryLocation)", storage.exists(memoryLocation));
        assertEquals(2, storage.ls(outputLocation).size());

        assertTrue("storage.rm(graphLocation)", storage.rm(graphLocation));
        assertFalse("storage.rm(graphLocation)", storage.rm(graphLocation));
        assertFalse("storage.exists(graphLocation)", storage.exists(graphLocation));

        assertEquals(1, storage.ls(outputLocation).size());

        assertTrue("storage.rm(memoryLocation)", storage.rm(memoryLocation));
        assertFalse("storage.rm(memoryLocation)", storage.rm(memoryLocation));
        assertFalse("storage.exists(memoryLocation)", storage.exists(memoryLocation));

        assertEquals(0, storage.ls(outputLocation).size());

        assertTrue("storage.rm(outputLocation)", storage.rm(outputLocation));
        assertFalse("storage.exists(outputLocation)", storage.exists(outputLocation));
        assertEquals(0, storage.ls(outputLocation).size());
    }

    public void checkCopyMethods(final Storage storage, final String outputLocation, final String newOutputLocation, final Class outputGraphParserClass, final Class outputMemoryParserClass) throws Exception {
        graphProvider.getGraphComputer(graph).program(PeerPressureVertexProgram.build().create(graph)).mapReduce(ClusterCountMapReduce.build().memoryKey("clusterCount").create()).submit().get();

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

        assertEquals(2, storage.ls(newOutputLocation).size());
        assertEquals(6, IteratorUtils.count(storage.head(outputLocation, outputGraphParserClass)));
        assertEquals(6, IteratorUtils.count(storage.head(newOutputLocation, outputGraphParserClass)));
        assertEquals(1, IteratorUtils.count(storage.head(outputLocation, "clusterCount", outputMemoryParserClass)));
        assertEquals(1, IteratorUtils.count(storage.head(newOutputLocation, "clusterCount", outputMemoryParserClass)));
    }

    public void checkResidualDataInStorage(final Storage storage, final String outputLocation) throws Exception {
        final GraphTraversal<Vertex, Long> traversal = g.V().both("knows").groupCount("m").by("age").count();
        assertEquals(4l, traversal.next().longValue());
        assertFalse(storage.exists(outputLocation));
        assertFalse(storage.exists(Constants.getGraphLocation(outputLocation)));
        ///
        assertEquals(3, traversal.asAdmin().getSideEffects().<Map<Integer, Long>>get("m").size());
        assertEquals(1, traversal.asAdmin().getSideEffects().<Map<Integer, Long>>get("m").get(27).longValue());
        assertEquals(2, traversal.asAdmin().getSideEffects().<Map<Integer, Long>>get("m").get(29).longValue());
        assertEquals(1, traversal.asAdmin().getSideEffects().<Map<Integer, Long>>get("m").get(32).longValue());
    }

    public void checkFileDirectoryDistinction(final Storage storage, final String directory1, final String directory2) throws Exception {
        assertTrue(storage.exists(directory1));
        assertTrue(storage.exists(directory2));
        assertTrue(storage.exists(directory1 + "/f*"));
        assertTrue(storage.exists(directory2 + "/f*"));
        assertEquals(10, storage.ls(directory1).size());
        assertEquals(10, storage.ls(directory1 + "/*").size());
        assertEquals(5, storage.ls(directory2).size());
        assertEquals(5, storage.ls(directory2 + "/*").size());
        for (int i = 0; i < 10; i++) {
            assertTrue(storage.exists(directory1 + "/file1-" + i + ".txt.bz"));
            assertTrue(storage.exists(directory1 + "/file1-" + i + "*"));
            assertTrue(storage.exists(directory1 + "/file1-" + i + ".txt*"));
            assertTrue(storage.exists(directory1 + "/file1-" + i + ".*.bz"));
            assertTrue(storage.exists(directory1 + "/file1-" + i + ".*.b*"));
        }
        assertFalse(storage.exists(directory1 + "/file1-10.txt.bz"));
        for (int i = 0; i < 5; i++) {
            assertTrue(storage.exists(directory2 + "/file2-" + i + ".txt.bz"));
            assertTrue(storage.exists(directory2 + "/file2-" + i + "*"));
            assertTrue(storage.exists(directory2 + "/file2-" + i + ".txt*"));
            assertTrue(storage.exists(directory2 + "/file2-" + i + ".*.bz"));
            assertTrue(storage.exists(directory2 + "/file2-" + i + ".*.b*"));
        }
        assertFalse(storage.exists(directory2 + "/file1-5.txt.bz"));
        assertTrue(storage.rm(directory1 + "/file1-0.txt.bz"));
        assertFalse(storage.rm(directory1 + "/file1-0.txt.bz"));
        assertEquals(9, storage.ls(directory1).size());
        assertEquals(9, storage.ls(directory1 + "/*").size());
        assertEquals(9, storage.ls(directory1 + "/file*").size());
        assertEquals(9, storage.ls(directory1 + "/file1*").size());
        assertEquals(0, storage.ls(directory1 + "/file2*").size());
        assertEquals(5, storage.ls(directory2).size());
        assertEquals(5, storage.ls(directory2 + "/*").size());
        assertEquals(5, storage.ls(directory2 + "/file*").size());
        assertEquals(5, storage.ls(directory2 + "/file2*").size());
        assertEquals(0, storage.ls(directory2 + "/file1*").size());
        assertTrue(storage.rm(directory1 + "/file1-*"));
        assertFalse(storage.rm(directory1 + "/file1-*"));
        assertEquals(0, storage.ls(directory1).size());
        assertEquals(0, storage.ls(directory1 + "/*").size());
        assertEquals(5, storage.ls(directory2).size());
        assertEquals(5, storage.ls(directory2 + "/*").size());
        assertTrue(storage.rm(directory2 + "/f*"));
        assertFalse(storage.rm(directory2 + "/file*"));
        assertEquals(0, storage.ls(directory2).size());
        assertEquals(0, storage.ls(directory2 + "*").size());
    }
}
