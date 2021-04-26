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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.tinkerpop.gremlin.driver.exception.NoHostAvailableException;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Client.ClusteredClient.class, Client.SessionedClient.class, Host.class, Cluster.class})
public class ClientTest {
    @Mock
    private Cluster cluster;

    @Mock
    private Host mockAvailableHost;

    @Mock
    private Client.Settings settings;

    private ScheduledExecutorService executor;
    private ScheduledExecutorService scheduler;

    @Before
    public void setup() {
        executor = Executors.newScheduledThreadPool(1);
        scheduler = Executors.newScheduledThreadPool(1);
        when(mockAvailableHost.isAvailable()).thenReturn(true);
        when(cluster.allHosts()).thenReturn(Collections.singletonList(mockAvailableHost));
        when(cluster.executor()).thenReturn(executor);
        when(cluster.scheduler()).thenReturn(scheduler);
    }

    @After
    public void cleanup() {
        executor.shutdown();
        scheduler.shutdown();
    }

    @Test(expected = NoHostAvailableException.class)
    public void shouldThrowErrorWhenConnPoolInitFailsForClusteredClient() throws Exception {
        Client.ClusteredClient client = new Client.ClusteredClient(cluster, settings);
        whenNew(ConnectionPool.class).withAnyArguments().thenThrow(new RuntimeException("cannot initialize client"));
        client.init();
    }

    @Test(expected = NoHostAvailableException.class)
    public void shouldThrowErrorWhenConnPoolInitFailsForSessionClient() throws Exception {
        final Client.SessionSettings sessionSettings = Client.SessionSettings.build().sessionId("my-session-id").create();
        when(settings.getSession()).thenReturn(Optional.of(sessionSettings));
        Client.SessionedClient client = new Client.SessionedClient(cluster, settings);
        whenNew(ConnectionPool.class).withAnyArguments().thenThrow(new RuntimeException("cannot initialize client"));
        client.init();
    }

}
