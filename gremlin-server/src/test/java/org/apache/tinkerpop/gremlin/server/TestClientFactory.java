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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.simple.WebSocketClient;

import java.net.URI;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TestClientFactory {

    public static final int PORT = 45940;
    public static final URI WEBSOCKET_URI = URI.create("ws://localhost:" + PORT + "/gremlin");
    public static final String HTTP = "http://localhost:" + PORT;
    public static final String RESOURCE_PATH = "conf/remote-objects.yaml";

    public static Cluster.Builder build() {
        return Cluster.build("localhost").port(45940);
    }

    public static Cluster open() {
        return build().create();
    }

    public static WebSocketClient createWebSocketClient() {
        return new WebSocketClient(WEBSOCKET_URI);
    }

    public static String createURLString() {
        return createURLString("");
    }

    public static String createURLString(final String suffix) {
        return HTTP + suffix;
    }
}
