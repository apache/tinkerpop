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
package ${package};

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;

import java.util.List;

public class Service implements AutoCloseable {

    /**
     * There typically needs to be only one Cluster instance in an application.
     */
    private final Cluster cluster = Cluster.build().port(45940).create();

    /**
     * Construct a remote GraphTraversalSource using the above created Cluster instance that will connect to Gremlin
     * Server.
     */
    private final GraphTraversalSource g = EmptyGraph.instance().
                                                      traversal().
                                                      withRemote(DriverRemoteConnection.using(cluster));

    /**
     * Create Service as a singleton given the simplicity of App.
     */
    private static final Service INSTANCE = new Service();

    private Service() {}

    public static Service instance() {
        return INSTANCE;
    }

    public List<Object> findCreatorsOfSoftware(String softwareName) throws Exception {
        return g.V().has("software", "name", softwareName).
                 in("created").
                 values("name").toList();
    }

    @Override
    public void close() throws Exception {
        cluster.close();
    }
}