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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a method for selecting the host from a {@link Cluster}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface LoadBalancingStrategy extends Host.Listener {

    /**
     * Initialize the strategy with the {@link Cluster} instance and the expected host list.
     */
    public void initialize(final Cluster cluster, final Collection<Host> hosts);

    /**
     * Provide an ordered list of hosts to send the given {@link RequestMessage} to.
     */
    public Iterator<Host> select(final RequestMessage msg);

    /**
     * A simple round-robin strategy that simply selects the next host in the {@link Cluster} to send the
     * {@link RequestMessage} to.
     */
    public static class RoundRobin implements LoadBalancingStrategy {

        private final CopyOnWriteArrayList<Host> availableHosts = new CopyOnWriteArrayList<>();
        private final AtomicInteger index = new AtomicInteger();

        @Override
        public void initialize(final Cluster cluster, final Collection<Host> hosts) {
            this.availableHosts.addAll(hosts);
            this.index.set(new Random().nextInt(Math.max(hosts.size(), 1)));
        }

        @Override
        public Iterator<Host> select(final RequestMessage msg) {
            final List<Host> hosts = (List<Host>) availableHosts.clone();
            final int startIndex = index.getAndIncrement();

            if (startIndex > Integer.MAX_VALUE - 10000)
                index.set(0);

            return new Iterator<Host>() {

                private int currentIndex = startIndex;
                private int remainingHosts = hosts.size();

                @Override
                public boolean hasNext() {
                    return remainingHosts > 0;
                }

                @Override
                public Host next() {
                    remainingHosts--;
                    int c = currentIndex++ % hosts.size();
                    if (c < 0)
                        c += hosts.size();
                    return hosts.get(c);
                }
            };
        }

        @Override
        public void onAvailable(final Host host) {
            this.availableHosts.addIfAbsent(host);
        }

        @Override
        public void onUnavailable(final Host host) {
            this.availableHosts.remove(host);
        }

        @Override
        public void onNew(final Host host) {
            onAvailable(host);
        }

        @Override
        public void onRemove(final Host host) {
            onUnavailable(host);
        }
    }
}
