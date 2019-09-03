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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(PowerMockRunner.class)
public class HostTest {

    @Test
    public void shouldConstructHostWithDefaultPath() {
        final InetSocketAddress addy = new InetSocketAddress("localhost", 8182);
        final Host host = new Host(addy, Cluster.open());
        final URI webSocketUri = host.getHostUri();
        assertEquals("ws://" + addy.getAddress().getHostAddress() + ":8182/gremlin", webSocketUri.toString());
    }

    @Test
    public void shouldConstructHostWithCustomPath() {
        final InetSocketAddress addy = new InetSocketAddress("localhost", 8183);
        final Host host = new Host(addy, Cluster.build().port(8183).path("/argh").create());
        final URI webSocketUri = host.getHostUri();
        assertEquals("ws://" + addy.getAddress().getHostAddress() + ":8183/argh", webSocketUri.toString());
    }

    @Test
    @PrepareForTest(Cluster.Builder.class)
    public void shouldFindAllHostsWithDeterministicAddress() throws Exception {
        final String hostname = "test.tinkerpop.apache.org";
        final Set<String> addresses = new HashSet<>(Arrays.asList("10.10.0.1", "10.10.0.2", "10.10.0.3"));
        final InetAddress[] hosts = addresses.stream().map(addr -> inetAddress(hostname, addr)).toArray(InetAddress[]::new);

        PowerMockito.mockStatic(InetAddress.class);
        PowerMockito.when(InetAddress.getAllByName(hostname)).thenReturn(hosts);

        Cluster cluster = Cluster.build().addContactPoint(hostname).create();

        cluster.init();

        assertEquals(addresses.size(), cluster.allHosts().size());

        cluster.allHosts().forEach(host -> {
            String uriHost = host.getHostUri().getHost();
            String address = host.getAddress().getAddress().getHostAddress();
            assertEquals(address, uriHost);
            assertTrue(addresses.contains(address));
        });
    }

    private InetAddress inetAddress(String hostname, String address) {
        try {
            return InetAddress.getByAddress(hostname, InetAddress.getByName(address).getAddress());
        } catch (UnknownHostException ex) {
            throw new RuntimeException(ex);
        }
    }
}
