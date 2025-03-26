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

import java.net.InetSocketAddress;
import java.net.URI;

import static org.junit.Assert.assertEquals;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class HostTest {

    @Test
    public void shouldConstructHostWithDefaultPath() {
        final InetSocketAddress addy = new InetSocketAddress("localhost", 8182);
        final Host host = new Host(addy, Cluster.open());
        final URI webSocketUri = host.getHostUri();
        assertEquals("http://localhost:8182/gremlin", webSocketUri.toString());
    }

    @Test
    public void shouldConstructHostWithCustomPath() {
        final InetSocketAddress addy = new InetSocketAddress("localhost", 8183);
        final Host host = new Host(addy, Cluster.build().port(8183).path("/argh").create());
        final URI webSocketUri = host.getHostUri();
        assertEquals("http://localhost:8183/argh", webSocketUri.toString());
    }

}
