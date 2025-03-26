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
package org.apache.tinkerpop.gremlin.socket.server;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.UUID;

/**
 * Encapsulates all constants that are needed by SimpleSocketServer. UUID request id constants are used
 * to coordinate custom response behavior between a test client and the server.
 */
public class SocketServerSettings {
    public int PORT = 0;

    /**
     * Configures which serializer will be used. Ex: "GraphBinaryV1" or "GraphSONV2"
     */
    public String SERIALIZER = "GraphBinaryV1";
    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph.
     */
    public UUID SINGLE_VERTEX_REQUEST_ID = null;

    /**
     * If a request with this ID comes to the server, the server responds back with a single vertex picked from Modern
     * graph. After a 2 second delay, server sends a Close WebSocket frame on the same connection.
     */
    public UUID SINGLE_VERTEX_DELAYED_CLOSE_CONNECTION_REQUEST_ID = null;

    /**
     * Server waits for 1 second, then responds with a 500 error status code
     */
    public UUID FAILED_AFTER_DELAY_REQUEST_ID = null;

    /**
     * Server waits for 1 second then responds with a close web socket frame
     */
    public UUID CLOSE_CONNECTION_REQUEST_ID = null;

    /**
     * Same as CLOSE_CONNECTION_REQUEST_ID
     */
    public UUID CLOSE_CONNECTION_REQUEST_ID_2 = null;

    /**
     * If a request with this ID comes to the server, the server responds with the user agent (if any) that was captured
     * during the web socket handshake.
     */
    public UUID USER_AGENT_REQUEST_ID = null;

    /**
     * If a request with this ID comes to the server, the server responds with a string containing all overridden
     * per request settings from the request message. String will be of the form
     * "requestId=19436d9e-f8fc-4b67-8a76-deec60918424 evaluationTimeout=1234, batchSize=12, userAgent=testUserAgent"
     */
    public UUID PER_REQUEST_SETTINGS_REQUEST_ID = null;

    public static SocketServerSettings read(final Path confFilePath) throws IOException {
        return read(Files.newInputStream(confFilePath));
    }

    public static SocketServerSettings read(final InputStream confInputStream) {
        Objects.requireNonNull(confInputStream);
        final LoaderOptions options = new LoaderOptions();
        final Yaml yaml = new Yaml(new Constructor(SocketServerSettings.class, options));
        return yaml.load(confInputStream);
    }
}
