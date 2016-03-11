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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.process.server.ServerConnection;
import org.apache.tinkerpop.gremlin.process.server.ServerConnectionException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;

import java.util.Iterator;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverServerConnection implements ServerConnection {

    private final Client client;
    private final boolean tryCloseCluster;

    private DriverServerConnection(final Cluster cluster, final boolean tryCloseCluster){
        client = cluster.connect(Client.Settings.build().unrollTraversers(false).create()).alias("graph");
        this.tryCloseCluster = tryCloseCluster;
    }

    public static DriverServerConnection using(final Cluster cluster) {
        return new DriverServerConnection(cluster, false);
    }

    public static DriverServerConnection using(final String conf) {
        try {
            return new DriverServerConnection(Cluster.open(conf), true);
        } catch (Exception ex) {
            throw new IllegalStateException(ex);
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
