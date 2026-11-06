/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.tinkergraph.structure.storage;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerStorageGraph;

/**
 * Opens a store from a separate JVM, as another process would, and commits one vertex to it. Prints {@code committed}
 * once the commit is acknowledged and keeps the graph open until standard input closes.
 */
public final class StoreWriter {

    public static void main(final String[] args) throws Exception {
        final Configuration conf = new BaseConfiguration();
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE, args[0]);
        conf.setProperty(TinkerGraph.GREMLIN_TINKERGRAPH_STORAGE_DIRECTORY, args[1]);
        final TinkerStorageGraph graph = TinkerStorageGraph.open(conf);
        try {
            graph.addVertex(T.id, Integer.parseInt(args[2]));
            graph.tx().commit();
            System.out.println("committed");
            System.out.flush();
            while (System.in.read() != -1) {
                // wait for the test to close standard input
            }
        } finally {
            graph.close();
        }
    }
}
