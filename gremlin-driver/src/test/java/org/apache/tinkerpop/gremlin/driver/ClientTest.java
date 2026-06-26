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
package org.apache.tinkerpop.gremlin.driver;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link Client} and its implementations.
 */
public class ClientTest {

    /**
     * Exercises {@link Client.PinnedClient}'s host selection (the transaction-pinning logic). A transaction must pin
     * to a host chosen via the cluster's load-balancing strategy, and must never pin to a host that is currently
     * unavailable (it would otherwise route every request to a dead server). The strategy is seeded directly via
     * {@code onNew} so selection can be verified without opening any real connections.
     */
    @Test
    public void shouldPinTransactionToAvailableHostSelectedByStrategy() {
        final Cluster cluster = Cluster.build("localhost").create();

        final Host availableHost = mock(Host.class);
        final Host unavailableHost = mock(Host.class);
        when(availableHost.isAvailable()).thenReturn(true);
        when(unavailableHost.isAvailable()).thenReturn(false);

        // Simulate both hosts having been registered with the strategy without standing up connection pools.
        cluster.loadBalancingStrategy().onNew(unavailableHost);
        cluster.loadBalancingStrategy().onNew(availableHost);

        final Client.PinnedClient pinnedClient = new Client.PinnedClient(new Client.ClusteredClient(cluster));

        assertEquals(availableHost, pinnedClient.getPinnedHost());
    }
}
