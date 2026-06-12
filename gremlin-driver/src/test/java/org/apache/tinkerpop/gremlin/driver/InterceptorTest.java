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

import org.apache.tinkerpop.gremlin.driver.auth.Auth;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class InterceptorTest {

    private static final ObjectMapper mapper = new ObjectMapper();

    private HttpRequest createRequest(final RequestMessage msg) throws Exception {
        return new HttpRequest(new HashMap<>(), msg, new URI("http://localhost:8182/gremlin"));
    }

    private HttpRequest createRequest() throws Exception {
        return createRequest(RequestMessage.build("g.V()").addG("g").create());
    }

    @Test
    public void interceptorReceivesRequestMessageInBody() throws Exception {
        final RequestMessage msg = RequestMessage.build("g.V()").create();
        final HttpRequest request = createRequest(msg);

        final List<Object> captured = new ArrayList<>();
        final RequestInterceptor interceptor = req -> captured.add(req.getBody());
        interceptor.intercept(request);

        assertEquals(1, captured.size());
        assertSame(msg, captured.get(0));
    }

    @Test
    public void interceptorCanReadAndModifyHeaders() throws Exception {
        final HttpRequest request = createRequest();
        request.headers().put("X-Existing", "original");

        final RequestInterceptor interceptor = req -> {
            assertEquals("original", req.headers().get("X-Existing"));
            req.headers().put("X-Existing", "modified");
            req.headers().put("X-New", "added");
        };
        interceptor.intercept(request);

        assertEquals("modified", request.headers().get("X-Existing"));
        assertEquals("added", request.headers().get("X-New"));
    }

    @Test
    public void interceptorCanModifyUri() throws Exception {
        final HttpRequest request = createRequest();
        final URI newUri = new URI("http://other-host:9999/gremlin");

        final RequestInterceptor interceptor = req -> req.setUri(newUri);
        interceptor.intercept(request);

        assertEquals(newUri, request.getUri());
    }

    @Test
    public void interceptorsRunInRegistrationOrder() throws Exception {
        final HttpRequest request = createRequest();
        final List<Integer> order = new ArrayList<>();

        final List<RequestInterceptor> interceptors = List.of(
                req -> order.add(1),
                req -> order.add(2),
                req -> order.add(3)
        );

        for (final RequestInterceptor i : interceptors) {
            i.intercept(request);
        }

        assertEquals(List.of(1, 2, 3), order);
    }

    @Test
    public void serializeBodyConvertsRequestMessageToJsonBytes() throws Exception {
        final RequestMessage msg = RequestMessage.build("g.V().count()").addG("g").create();
        final HttpRequest request = createRequest(msg);

        final byte[] result = request.serializeBody();

        final JsonNode json = mapper.readTree(result);
        assertEquals("g.V().count()", json.get("gremlin").asText());
        assertEquals("g", json.get("g").asText());
    }

    @Test
    public void serializeBodySetsContentTypeHeader() throws Exception {
        final HttpRequest request = createRequest();

        request.serializeBody();

        assertEquals("application/json", request.headers().get(HttpRequest.Headers.CONTENT_TYPE));
    }

    @Test
    public void serializeBodySetsContentLengthHeader() throws Exception {
        final HttpRequest request = createRequest();

        final byte[] result = request.serializeBody();

        assertEquals(String.valueOf(result.length), request.headers().get(HttpRequest.Headers.CONTENT_LENGTH));
    }

    @Test
    public void serializeBodyIsIdempotentWithPreSerializedBytes() throws Exception {
        final byte[] existing = "{\"gremlin\":\"g.V()\"}".getBytes();
        final HttpRequest request = new HttpRequest(new HashMap<>(), existing, new URI("http://localhost:8182/gremlin"));

        final byte[] first = request.serializeBody();
        final byte[] second = request.serializeBody();

        assertSame(existing, first);
        assertSame(first, second);
    }

    @Test
    public void serializeBodyIsIdempotentWithRequestMessage() throws Exception {
        final HttpRequest request = createRequest();

        final byte[] first = request.serializeBody();
        final byte[] second = request.serializeBody();

        assertSame(first, second);
    }

    @Test
    public void serializeBodyIncludesAllFields() throws Exception {
        final RequestMessage msg = RequestMessage.build("g.V()")
                .addG("g")
                .addLanguage("gremlin-lang")
                .addTimeoutMillis(5000L)
                .addBulkResults(true)
                .create();
        final HttpRequest request = createRequest(msg);

        final byte[] result = request.serializeBody();
        final JsonNode json = mapper.readTree(result);

        assertEquals("g.V()", json.get("gremlin").asText());
        assertEquals("g", json.get("g").asText());
        assertEquals("gremlin-lang", json.get("language").asText());
        assertEquals(5000, json.get("timeoutMs").asLong());
        assertEquals("true", json.get("bulkResults").asText());
    }

    @Test
    public void interceptorCanReplaceBodyBeforeSerialization() throws Exception {
        final RequestMessage original = RequestMessage.build("g.V()").addG("g").create();
        final HttpRequest request = createRequest(original);

        final RequestInterceptor interceptor = req -> {
            req.setBody(RequestMessage.build("g.E()").addG("gmodern").create());
        };
        interceptor.intercept(request);
        request.serializeBody();

        final JsonNode json = mapper.readTree((byte[]) request.getBody());
        assertEquals("g.E()", json.get("gremlin").asText());
        assertEquals("gmodern", json.get("g").asText());
    }

    @Test
    public void authIsAlwaysLastInterceptorRegardlessOfBuilderCallOrder() throws Exception {
        // auth called before interceptors
        final Cluster cluster1 = Cluster.build("localhost")
                .auth(Auth.basic("user", "pass"))
                .interceptors(req -> req.headers().put("X-Custom", "value"))
                .create();

        final List<RequestInterceptor> interceptors1 = cluster1.getRequestInterceptors();
        assertEquals(2, interceptors1.size());
        assertTrue("Auth should be last interceptor", interceptors1.get(interceptors1.size() - 1) instanceof Auth);
        cluster1.close();

        // auth called after interceptors
        final Cluster cluster2 = Cluster.build("localhost")
                .interceptors(req -> req.headers().put("X-Custom", "value"))
                .auth(Auth.basic("user", "pass"))
                .create();

        final List<RequestInterceptor> interceptors2 = cluster2.getRequestInterceptors();
        assertEquals(2, interceptors2.size());
        assertTrue("Auth should be last interceptor", interceptors2.get(interceptors2.size() - 1) instanceof Auth);
        cluster2.close();
    }

}
