/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.tinkerpop.example.cmdb.graph;

import jakarta.inject.Singleton;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;
import java.util.function.Function;

@Singleton
public class GremlinGateway {

    private final GraphTraversalSource g;
    private final Client client;

    public GremlinGateway(DriverRemoteConnection connection, Client client) {
        this.g = AnonymousTraversalSource.traversal().withRemote(connection);
        this.client = client;
    }

    public <T> List<T> fetch(Function<GraphTraversalSource, GraphTraversal<?, T>> fn) {
        return fn.apply(g).toList();
    }

    public void iterate(Function<GraphTraversalSource, GraphTraversal<?, ?>> fn) {
        fn.apply(g).iterate();
    }

    public Client getClient() {
        return client;
    }
}
