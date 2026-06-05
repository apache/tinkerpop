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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.junit.Test;

import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;

public class GremlinServerHttpPipeliningIntegrateTest extends AbstractGremlinServerIntegrationTest {

    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        return settings;
    }

    @Test
    public void shouldCloseConnectionOnPipelinedRequest() throws Exception {
        final String request = "POST /gremlin HTTP/1.1\r\n" +
                "Host: localhost:" + TestClientFactory.PORT + "\r\n" +
                "Content-Type: application/json\r\n" +
                "Content-Length: 27\r\n" +
                "Connection: keep-alive\r\n\r\n" +
                "{\"gremlin\": \"g.inject(1)\"}";

        // send two requests in a single write so both arrive before the first can complete
        final byte[] pipelined = (request + request).getBytes(StandardCharsets.UTF_8);

        try (final Socket socket = new Socket("localhost", TestClientFactory.PORT)) {
            socket.setSoTimeout(10000);
            socket.getOutputStream().write(pipelined);
            socket.getOutputStream().flush();

            final InputStream in = socket.getInputStream();
            final byte[] buf = new byte[8192];
            final StringBuilder sb = new StringBuilder();

            // read until the server closes the connection
            int bytesRead;
            while ((bytesRead = in.read(buf)) != -1) {
                sb.append(new String(buf, 0, bytesRead, StandardCharsets.UTF_8));
            }

            final String fullResponse = sb.toString();

            // should contain exactly one HTTP response (the first request's) and then close
            final int responseCount = fullResponse.split("HTTP/1.1 200 OK").length - 1;
            assertEquals("Expected exactly one response before connection close", 1, responseCount);
        }
    }
}
