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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
import org.apache.tinkerpop.gremlin.process.remote.traversal.RemoteTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeUtil;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_BATCH_SIZE;
import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_SCRIPT_EVAL_TIMEOUT;
import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_EVAL_TIMEOUT;
import static org.apache.tinkerpop.gremlin.driver.Tokens.ARGS_USER_AGENT;
import static org.apache.tinkerpop.gremlin.driver.Tokens.REQUEST_ID;

/**
 * A {@link RemoteConnection} implementation for Gremlin Server. Each {@code DriverServerConnection} is bound to one
 * graph instance hosted in Gremlin Server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteConnection implements RemoteConnection {

    public static final String GREMLIN_REMOTE_DRIVER_CLUSTERFILE = GREMLIN_REMOTE + "driver.clusterFile";
    public static final String GREMLIN_REMOTE_DRIVER_SOURCENAME = GREMLIN_REMOTE + "driver.sourceName";

    private static final String DEFAULT_TRAVERSAL_SOURCE = "g";

    private final Client client;
    private final boolean tryCloseCluster;
    private final boolean tryCloseClient;
    private final String remoteTraversalSourceName;
    private transient Optional<Configuration> conf = Optional.empty();

    private final boolean attachElements;

    public DriverRemoteConnection(final Configuration conf) {
        final boolean hasClusterConf = IteratorUtils.anyMatch(conf.getKeys(), k -> k.startsWith("clusterConfiguration"));
        if (conf.containsKey(GREMLIN_REMOTE_DRIVER_CLUSTERFILE) && hasClusterConf)
            throw new IllegalStateException(String.format("A configuration should not contain both '%s' and 'clusterConfiguration'", GREMLIN_REMOTE_DRIVER_CLUSTERFILE));

        remoteTraversalSourceName = conf.getString(GREMLIN_REMOTE_DRIVER_SOURCENAME, DEFAULT_TRAVERSAL_SOURCE);

        try {
            final Cluster cluster;
            if (!conf.containsKey(GREMLIN_REMOTE_DRIVER_CLUSTERFILE) && !hasClusterConf)
                cluster = Cluster.open();
            else
                cluster = conf.containsKey(GREMLIN_REMOTE_DRIVER_CLUSTERFILE) ?
                        Cluster.open(conf.getString(GREMLIN_REMOTE_DRIVER_CLUSTERFILE)) : Cluster.open(conf.subset("clusterConfiguration"));

            client = cluster.connect(Client.Settings.build().create()).alias(remoteTraversalSourceName);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }

        attachElements = false;

        tryCloseCluster = true;
        tryCloseClient = true;
        this.conf = Optional.of(conf);
    }

    private DriverRemoteConnection(final Cluster cluster, final boolean tryCloseCluster, final String remoteTraversalSourceName) {
        client = cluster.connect(Client.Settings.build().create()).alias(remoteTraversalSourceName);
        this.remoteTraversalSourceName = remoteTraversalSourceName;
        this.tryCloseCluster = tryCloseCluster;
        attachElements = false;
        tryCloseClient = true;
    }

    /**
     * This constructor is largely just for unit testing purposes and should not typically be used externally.
     */
    DriverRemoteConnection(final Cluster cluster, final Configuration conf) {
        remoteTraversalSourceName = conf.getString(GREMLIN_REMOTE_DRIVER_SOURCENAME, DEFAULT_TRAVERSAL_SOURCE);

        attachElements = conf.containsKey(GREMLIN_REMOTE + "attachment");

        client = cluster.connect(Client.Settings.build().create()).alias(remoteTraversalSourceName);
        tryCloseCluster = false;
        tryCloseClient = true;
        this.conf = Optional.of(conf);
    }

    private DriverRemoteConnection(final Client client, final String remoteTraversalSourceName) {
        this.client = client.alias(remoteTraversalSourceName);
        this.remoteTraversalSourceName = remoteTraversalSourceName;
        this.tryCloseCluster = false;
        attachElements = false;
        tryCloseClient = false;
    }

    /**
     * Creates a {@link DriverRemoteConnection} using an existing {@link Client} object. The {@link Client} will not
     * be closed on calls to {@link #close()}.
     */
    public static DriverRemoteConnection using(final Client client) {
        return using(client, DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link DriverRemoteConnection} using an existing {@link Client} object. The {@link Client} will not
     * be closed on calls to {@link #close()}.
     */
    public static DriverRemoteConnection using(final Client client, final String remoteTraversalSourceName) {
        return new DriverRemoteConnection(client, remoteTraversalSourceName);
    }

    /**
     * Creates a {@link DriverRemoteConnection} using a new {@link Cluster} instance created from the supplied host
     * and port and binds it to a remote {@link GraphTraversalSource} named "g". When {@link #close()} is called,
     * this new {@link Cluster} is also closed. By default, this method will bind the {@link RemoteConnection} to a
     * graph on the server named "graph".
     */
    public static DriverRemoteConnection using(final String host, final int port) {
        return using(Cluster.build(host).port(port).create(), DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link DriverRemoteConnection} using a new {@link Cluster} instance created from the supplied host
     * port and aliases it to the specified remote {@link GraphTraversalSource}. When {@link #close()} is called, this
     * new {@link Cluster} is also closed. By default, this method will bind the {@link RemoteConnection} to the
     * specified graph traversal source name.
     */
    public static DriverRemoteConnection using(final String host, final int port, final String remoteTraversalSourceName) {
        return using(Cluster.build(host).port(port).create(), remoteTraversalSourceName);
    }

    /**
     * Creates a {@link DriverRemoteConnection} from an existing {@link Cluster} instance. When {@link #close()} is
     * called, the {@link Cluster} is left open for the caller to close. By default, this method will bind the
     * {@link RemoteConnection} to a graph on the server named "graph".
     */
    public static DriverRemoteConnection using(final Cluster cluster) {
        return using(cluster, DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link DriverRemoteConnection} from an existing {@link Cluster} instance. When {@link #close()} is
     * called, the {@link Cluster} is left open for the caller to close.
     */
    public static DriverRemoteConnection using(final Cluster cluster, final String remoteTraversalSourceName) {
        return new DriverRemoteConnection(cluster, false, remoteTraversalSourceName);
    }

    /**
     * Creates a {@link DriverRemoteConnection} using a new {@link Cluster} instance created from the supplied
     * configuration file. When {@link #close()} is called, this new {@link Cluster} is also closed. By default,
     * this method will bind the {@link RemoteConnection} to a graph on the server named "graph".
     */
    public static DriverRemoteConnection using(final String clusterConfFile) {
        return using(clusterConfFile, DEFAULT_TRAVERSAL_SOURCE);
    }

    /**
     * Creates a {@link DriverRemoteConnection} using a new {@link Cluster} instance created from the supplied
     * configuration file. When {@link #close()} is called, this new {@link Cluster} is also closed.
     */
    public static DriverRemoteConnection using(final String clusterConfFile, final String remoteTraversalSourceName) {
        try {
            return new DriverRemoteConnection(Cluster.open(clusterConfFile), true, remoteTraversalSourceName);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
        }
    }

    /**
     * Creates a {@link DriverRemoteConnection} using an Apache {@code Configuration} object. The
     * {@code Configuration} object should contain one of two required keys, either: {@code clusterConfigurationFile}
     * or {@code clusterConfiguration}. The {@code clusterConfigurationFile} key is a pointer to a file location
     * containing a configuration for a {@link Cluster}. The {@code clusterConfiguration} should contain the actual
     * contents of a configuration that would be used by a {@link Cluster}.  This {@code configuration} may also
     * contain the optional, but likely necessary, {@code remoteTraversalSourceName} which tells the
     * {@code DriverServerConnection} which graph on the server to bind to.
     */
    public static DriverRemoteConnection using(final Configuration conf) {
        if (conf.containsKey("clusterConfigurationFile") && conf.containsKey("clusterConfiguration"))
            throw new IllegalStateException("A configuration should not contain both 'clusterConfigurationFile' and 'clusterConfiguration'");

        if (!conf.containsKey("clusterConfigurationFile") && !conf.containsKey("clusterConfiguration"))
            throw new IllegalStateException("A configuration must contain either 'clusterConfigurationFile' and 'clusterConfiguration'");

        final String remoteTraversalSourceName = conf.getString(DEFAULT_TRAVERSAL_SOURCE, DEFAULT_TRAVERSAL_SOURCE);
        if (conf.containsKey("clusterConfigurationFile"))
            return using(conf.getString("clusterConfigurationFile"), remoteTraversalSourceName);
        else {
            return using(Cluster.open(conf.subset("clusterConfiguration")), remoteTraversalSourceName);
        }
    }

    @Override
    public <E> CompletableFuture<RemoteTraversal<?, E>> submitAsync(final Bytecode bytecode) throws RemoteConnectionException {
        try {
            return client.submitAsync(bytecode, getRequestOptions(bytecode)).thenApply(rs -> new DriverRemoteTraversal<>(rs, client, attachElements, conf));
        } catch (Exception ex) {
            throw new RemoteConnectionException(ex);
        }
    }

    protected static RequestOptions getRequestOptions(final Bytecode bytecode) {
        final Iterator<OptionsStrategy> itty = BytecodeUtil.findStrategies(bytecode, OptionsStrategy.class);
        final RequestOptions.Builder builder = RequestOptions.build();
        while (itty.hasNext()) {
            final OptionsStrategy optionsStrategy = itty.next();
            final Map<String,Object> options = optionsStrategy.getOptions();
            if (options.containsKey(ARGS_EVAL_TIMEOUT))
                builder.timeout((long) options.get(ARGS_EVAL_TIMEOUT));
            if (options.containsKey(ARGS_SCRIPT_EVAL_TIMEOUT))
                builder.timeout((long) options.get(ARGS_SCRIPT_EVAL_TIMEOUT));
            if (options.containsKey(REQUEST_ID))
                builder.overrideRequestId((UUID) options.get(REQUEST_ID));
            if (options.containsKey(ARGS_BATCH_SIZE))
                builder.batchSize((int) options.get(ARGS_BATCH_SIZE));
            if (options.containsKey(ARGS_USER_AGENT))
                builder.userAgent((String) options.get(ARGS_USER_AGENT));
        }
        return builder.create();
    }

    @Override
    public void close() throws Exception {
        try {
            if (tryCloseClient)
                client.close();
        } catch (Exception ex) {
            throw new RemoteConnectionException(ex);
        } finally {
            if (tryCloseCluster)
                client.getCluster().close();
        }
    }

    @Override
    public String toString() {
        return "DriverServerConnection-" + client.getCluster() + " [graph=" + remoteTraversalSourceName + "]";
    }
}
