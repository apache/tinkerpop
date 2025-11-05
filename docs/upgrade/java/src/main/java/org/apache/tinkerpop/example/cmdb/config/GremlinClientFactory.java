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
package org.apache.tinkerpop.example.cmdb.config;

import io.micronaut.context.annotation.Factory;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;

@Factory
public class GremlinClientFactory {

    private Cluster cluster;
    private Client client;
    private DriverRemoteConnection connection;

    @Singleton
    public Cluster cluster(GremlinClientConfig config) {
        Cluster.Builder builder = Cluster.build();
        for (String host : config.getHosts()) builder.addContactPoint(host);
        builder.port(config.getPort());
        if (config.getUsername() != null && !config.getUsername().isBlank()) {
            builder.credentials(config.getUsername(), config.getPassword());
        }
        if (config.isEnableSsl()) builder.enableSsl(true);
        this.cluster = builder.create();
        return this.cluster;
    }

    @Singleton
    public Client client(Cluster cluster) {
        this.client = cluster.connect();
        return this.client;
    }

    @Singleton
    public DriverRemoteConnection driverRemoteConnection(GremlinClientConfig config, Cluster cluster) {
        this.connection = DriverRemoteConnection.using(cluster, config.getTraversalSource());
        return this.connection;
    }

    @PreDestroy
    void shutdown() throws Exception {
        if (connection != null) connection.close();
        if (client != null) client.close();
        if (cluster != null) cluster.close();
    }
}
