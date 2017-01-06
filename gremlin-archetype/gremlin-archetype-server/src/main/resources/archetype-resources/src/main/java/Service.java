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
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Result;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class Service implements AutoCloseable {

    /**
     * There typically needs to be only one Cluster instance in an application.
     */
    private final Cluster cluster = Cluster.build().port(45940).create();

    /**
     * Use the Cluster instance to construct different Client instances (e.g. one for sessionless communication
     * and one or more sessions). A sessionless Client should be thread-safe and typically no more than one is
     * needed unless there is some need to divide connection pools across multiple Client instances. In this case
     * there is just a single sessionless Client instance used for the entire App.
     */
    private final Client client = cluster.connect();

    /**
     * Create Service as a singleton given the simplicity of App.
     */
    private static final Service INSTANCE = new Service();

    private Service() {}

    public static Service instance() {
        return INSTANCE;
    }

    public List<String> findCreatorsOfSoftware(String softwareName) throws Exception {
        // it is very important from a performance perspective to parameterize queries
        Map params = new HashMap();
        params.put("n", softwareName);

        return client.submit("g.V().hasLabel('software').has('name',n).in('created').values('name')", params)
                .all().get().stream().map(r -> r.getString()).collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
        client.close();
        cluster.close();
    }
}