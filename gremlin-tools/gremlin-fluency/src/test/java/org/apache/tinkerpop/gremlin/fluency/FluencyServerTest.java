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
package org.apache.tinkerpop.gremlin.fluency;

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;

public class FluencyServerTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private static final HttpClient client = HttpClient.newHttpClient();
    private static FluencyServer server;

    @BeforeClass
    public static void startServer() throws Exception {
        server = FluencyServer.start(Arrays.asList("modern", "empty"), 2);
    }

    @AfterClass
    public static void stopServer() {
        if (server != null) server.close();
    }

    /**
     * Submits a script the way gremlin-mcp would reach the endpoint and returns the status and plain text body.
     */
    private static HttpResponse<String> submit(final String endpoint, final String gremlin) throws Exception {
        final String[] hostPortSource = endpoint.split("/");
        final Map<String, String> request = new LinkedHashMap<>();
        request.put("gremlin", gremlin);
        request.put("g", hostPortSource[1]);
        return client.send(HttpRequest.newBuilder(URI.create("http://" + hostPortSource[0])).
                        header("Accept", "text/plain").
                        POST(HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(request))).build(),
                HttpResponse.BodyHandlers.ofString());
    }

    @Test
    public void shouldGiveEndpointsInTheFormGremlinMcpTakes() {
        assertThat(server.endpoint("modern"), matchesPattern("localhost:\\d+/g\\d+"));
        assertThat(server.endpoint("empty"), matchesPattern("localhost:\\d+/g\\d+"));
        // the source name must not say which graph it is
        assertThat(server.endpoint("modern"), not(containsString("modern")));
        assertThat(server.endpoint("modern"), not(server.endpoint("empty")));
    }

    @Test
    public void shouldGiveEachLaneItsOwnEmptyGraph() throws Exception {
        assertThat(server.endpoint("empty", 1), matchesPattern("localhost:\\d+/g\\d+"));
        assertThat(server.endpoint("empty", 1), not(server.endpoint("empty", 0)));
        assertThat(server.endpoint("modern", 1), is(server.endpoint("modern", 0)));

        // preparing one lane for an attempt leaves the attempt in the other lane where it was
        final IntentScenario addV = CorpusTest.find("g_addVXpersonX_propertyXname_stephenX");
        server.prepare(addV, 0);
        submit(server.endpoint("empty", 0), "g.addV(\"person\")");
        server.prepare(addV, 1);
        submit(server.endpoint("empty", 1), "g.V().drop()");
        assertThat(submit(server.endpoint("empty", 0), "g.V().count()").body(), containsString("7"));
        assertThat(submit(server.endpoint("empty", 1), "g.V().count()").body(), containsString("0"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRefuseALaneItDoesNotHave() {
        server.endpoint("empty", 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldRefuseAGraphItDoesNotServe() {
        server.endpoint("airroutes");
    }

    @Test
    public void shouldServeEachGraph() throws Exception {
        final HttpResponse<String> response = submit(server.endpoint("modern"), "g.V().count()");
        assertThat(response.body(), response.statusCode(), is(200));
        assertThat(response.body(), containsString("6"));
    }

    @Test
    public void shouldRejectWritesToAPreloadedGraph() throws Exception {
        // the HTTP API reports a failed traversal in the body after the status line has gone out
        final HttpResponse<String> write = submit(server.endpoint("modern"), "g.addV(\"person\")");
        assertThat(write.body(), containsString("not read only"));
        assertThat(submit(server.endpoint("modern"), "g.V().count()").body(), containsString("6"));
    }

    @Test
    public void shouldResetTheEmptyGraphToWhereTheScenarioStarts() throws Exception {
        final IntentScenario addV = CorpusTest.find("g_addVXpersonX_propertyXname_stephenX");
        server.prepare(addV);
        assertThat(submit(server.endpoint("empty"), "g.V().count()").body(), containsString("6"));

        final HttpResponse<String> write = submit(server.endpoint("empty"), "g.addV(\"person\")");
        assertThat(write.body(), write.statusCode(), is(200));
        assertThat(submit(server.endpoint("empty"), "g.V().count()").body(), containsString("7"));

        server.prepare(addV);
        assertThat(submit(server.endpoint("empty"), "g.V().count()").body(), containsString("6"));
    }
}
