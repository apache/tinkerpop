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
package org.apache.tinkerpop.gremlin.driver.strategy;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.server.ServerConnection;
import org.apache.tinkerpop.gremlin.process.server.ServerConnectionException;
import org.apache.tinkerpop.gremlin.process.server.ServerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;

import java.util.Iterator;

/**
 * A {@link ServerConnection} implementation for Gremlin Server. Each {@code DriverServerConnection} is bound to one
 * graph instance hosted in Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverServerConnection implements ServerConnection {

    private final Client client;
    private final boolean tryCloseCluster;
    private final String connectionGraphName;

    public DriverServerConnection(final Configuration conf) {
        if (conf.containsKey("clusterConfigurationFile") && conf.containsKey("clusterConfiguration"))
            throw new IllegalStateException("A configuration should not contain both 'clusterConfigurationFile' and 'clusterConfiguration'");

        if (!conf.containsKey("clusterConfigurationFile") && !conf.containsKey("clusterConfiguration"))
            throw new IllegalStateException("A configuration must contain either 'clusterConfigurationFile' and 'clusterConfiguration'");

        connectionGraphName = conf.getString("connectionGraphName", "graph");

        try {
            final Cluster cluster = conf.containsKey("clusterConfigurationFile") ?
                Cluster.open(conf.getString("clusterConfigurationFile")) : Cluster.open(conf.subset("clusterConfiguration"));
            client = cluster.connect(Client.Settings.build().unrollTraversers(false).create()).alias(connectionGraphName);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        tryCloseCluster = true;
    }

    private DriverServerConnection(final Cluster cluster, final boolean tryCloseCluster, final String connectionGraphName){
        client = cluster.connect(Client.Settings.build().unrollTraversers(false).create()).alias(connectionGraphName);
        this.connectionGraphName = connectionGraphName;
        this.tryCloseCluster = tryCloseCluster;
    }

    /**
     * Creates a {@link DriverServerConnection} from an existing {@link Cluster} instance. When {@link #close()} is
     * called, the {@link Cluster} is left open for the caller to close. By default, this method will bind the
     * {@link ServerConnection} to a graph on the server named "graph".
     */
    public static DriverServerConnection using(final Cluster cluster) {
        return using(cluster, "graph");
    }

    /**
     * Creates a {@link DriverServerConnection} from an existing {@link Cluster} instance. When {@link #close()} is
     * called, the {@link Cluster} is left open for the caller to close.
     */
    public static DriverServerConnection using(final Cluster cluster, final String connectionGraphName) {
        return new DriverServerConnection(cluster, false, connectionGraphName);
    }

    /**
     * Creates a {@link DriverServerConnection} using a new {@link Cluster} instance created from the supplied
     * configuration file. When {@link #close()} is called, this new {@link Cluster} is also closed. By default,
     * this method will bind the {@link ServerConnection} to a graph on the server named "graph".
     */
    public static DriverServerConnection using(final String clusterConfFile) {
        return using(clusterConfFile, "graph");
    }

    /**
     * Creates a {@link DriverServerConnection} using a new {@link Cluster} instance created from the supplied
     * configuration file. When {@link #close()} is called, this new {@link Cluster} is also closed.
     */
    public static DriverServerConnection using(final String clusterConfFile, final String connectionGraphName) {
        try {
            return new DriverServerConnection(Cluster.open(clusterConfFile), true, connectionGraphName);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Creates a {@link DriverServerConnection} using an Apache {@code Configuration} object. This method of creation
     * is typically used by {@link ServerGraph} when being constructed via {@link GraphFactory}. The
     * {@code Configuration} object should contain one of two required keys, either: {@code clusterConfigurationFile}
     * or {@code clusterConfiguration}. The {@code clusterConfigurationFile} key is a pointer to a file location
     * containing a configuration for a {@link Cluster}. The {@code clusterConfiguration} should contain the actual
     * contents of a configuration that would be used by a {@link Cluster}.  This {@code configuration} may also
     * contain the optional, but likely necessary, {@code connectionGraphName} which tells the
     * {@code DriverServerConnection} which graph on the server to bind to.
     */
    public static DriverServerConnection using(final Configuration conf) {
        if (conf.containsKey("clusterConfigurationFile") && conf.containsKey("clusterConfiguration"))
            throw new IllegalStateException("A configuration should not contain both 'clusterConfigurationFile' and 'clusterConfiguration'");

        if (!conf.containsKey("clusterConfigurationFile") && !conf.containsKey("clusterConfiguration"))
            throw new IllegalStateException("A configuration must contain either 'clusterConfigurationFile' and 'clusterConfiguration'");

        final String connectionGraphName = conf.getString("connectionGraphName", "graph");
        if (conf.containsKey("clusterConfigurationFile"))
            return using(conf.getString("clusterConfigurationFile"), connectionGraphName);
        else {
            return using(Cluster.open(conf.subset("clusterConfiguration")), connectionGraphName);
        }
    }

    @Override
    public Iterator<Traverser> submit(final Traversal t) throws ServerConnectionException  {
        try {
            return new TraverserIterator(client.submit(t).iterator());
        } catch (Exception ex) {
            throw new ServerConnectionException(ex);
        }
    }

    @Override
    public void close() throws Exception {
        try {
            client.close();
        } catch (Exception ex) {
            throw new ServerConnectionException(ex);
        } finally {
            if (tryCloseCluster)
                client.getCluster().close();
        }
    }

    @Override
    public String toString() {
        return "DriverServerConnection-" + client.getCluster() + " [graph='" + connectionGraphName + "]";
    }

    static class TraverserIterator implements Iterator<Traverser> {

        private Iterator<Result> inner;

        public TraverserIterator(final Iterator<Result> resultIterator) {
            inner = resultIterator;
        }

        @Override
        public boolean hasNext() {
            return inner.hasNext();
        }

        @Override
        public Traverser next() {
            final Object o = inner.next().getObject();
            return (Traverser) o;
        }
    }
}
