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
package org.apache.tinkerpop.gremlin.socket.server;

/**
 * Constants for the test HTTP socket server. Gremlin string constants coordinate
 * response behavior between test clients and the server.
 */
public final class SocketServerConstants {

    public static final int PORT = 45943;

    public static final int PROXY_PORT = 45944;

    public static final String GREMLIN_SINGLE_VERTEX = "server_single_vertex";
    public static final String GREMLIN_CLOSE_CONNECTION = "server_close_connection";
    public static final String GREMLIN_VERTEX_THEN_CLOSE = "server_vertex_then_close";
    public static final String GREMLIN_FAIL_AFTER_DELAY = "server_fail_after_delay";
    public static final String GREMLIN_PARTIAL_CONTENT_CLOSE = "server_partial_content_close";
    public static final String GREMLIN_SLOW_RESPONSE = "server_slow_response";
    public static final String GREMLIN_MALFORMED_RESPONSE = "server_malformed_response";
    public static final String GREMLIN_NO_RESPONSE = "server_no_response";
    public static final String GREMLIN_EMPTY_BODY = "server_empty_body";

    private SocketServerConstants() {}
}
