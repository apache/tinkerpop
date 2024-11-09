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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV2;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.server.handler.SaslAndHttpBasicAuthenticationHandler;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.binary.TypeSerializerRegistry;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.StringRegularExpression.matchesRegex;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;

/**
 * Integration tests for server-side settings and processing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinServerHttpIntegrateTest extends AbstractGremlinServerIntegrationTest {
    private final ObjectMapper mapper = new ObjectMapper();

    private final Base64.Encoder encoder = Base64.getUrlEncoder();

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        settings.channelizer = HttpChannelizer.class.getName();
        final String nameOfTest = name.getMethodName();
        switch (nameOfTest) {
            case "should413OnPostWithResultTooLarge":
                settings.maxContentLength = 31;
                break;
            case "should200OnPOSTTransactionalGraph":
                useTinkerTransactionGraph(settings);
                break;
            case "should200OnPOSTTransactionalGraphInStrictMode":
                useTinkerTransactionGraph(settings);
                settings.strictTransactionManagement = true;
                break;
            case "should200OnPOSTWithGraphSON1d0AcceptHeaderDefaultResultToJson":
                settings.serializers.clear();
                final Settings.SerializerSettings serializerSettingsV1 = new Settings.SerializerSettings();
                serializerSettingsV1.className = GraphSONUntypedMessageSerializerV1.class.getName();
                settings.serializers.add(serializerSettingsV1);
                break;
            case "should200OnPOSTWithGraphSON2d0AcceptHeaderDefaultResultToJson":
                settings.serializers.clear();
                final Settings.SerializerSettings serializerSettingsV2 = new Settings.SerializerSettings();
                serializerSettingsV2.className = GraphSONMessageSerializerV2.class.getName();
                settings.serializers.add(serializerSettingsV2);
                break;
            case "should200OnPOSTWithGraphSON3d0AcceptHeaderDefaultResultToJson":
                settings.serializers.clear();
                final Settings.SerializerSettings serializerSettingsV3 = new Settings.SerializerSettings();
                serializerSettingsV3.className = GraphSONMessageSerializerV3.class.getName();
                settings.serializers.add(serializerSettingsV3);
                break;
            case "should200OnPOSTWithAnyGraphSONAcceptHeaderDefaultResultToJson":
                settings.serializers.clear();
                final Settings.SerializerSettings serializerSettingsUntypedV1 = new Settings.SerializerSettings();
                serializerSettingsUntypedV1.className = GraphSONUntypedMessageSerializerV1.class.getName();
                settings.serializers.add(serializerSettingsUntypedV1);
                final Settings.SerializerSettings serializerSettingsTypedV1 = new Settings.SerializerSettings();
                serializerSettingsTypedV1.className = GraphSONMessageSerializerV1.class.getName();
                settings.serializers.add(serializerSettingsTypedV1);
                final Settings.SerializerSettings serializerSettingsUntypedV2 = new Settings.SerializerSettings();
                serializerSettingsUntypedV2.className = GraphSONUntypedMessageSerializerV2.class.getName();
                settings.serializers.add(serializerSettingsUntypedV2);
                final Settings.SerializerSettings serializerSettingsTypedV2 = new Settings.SerializerSettings();
                serializerSettingsTypedV2.className = GraphSONMessageSerializerV2.class.getName();
                settings.serializers.add(serializerSettingsTypedV2);
                final Settings.SerializerSettings serializerSettingsUntypedV3 = new Settings.SerializerSettings();
                serializerSettingsUntypedV3.className = GraphSONUntypedMessageSerializerV3.class.getName();
                settings.serializers.add(serializerSettingsUntypedV3);
                final Settings.SerializerSettings serializerSettingsTypedV3 = new Settings.SerializerSettings();
                serializerSettingsTypedV3.className = GraphSONMessageSerializerV3.class.getName();
                settings.serializers.add(serializerSettingsTypedV3);
                break;
            case "should401OnGETWithNoAuthorizationHeader":
            case "should401OnPOSTWithNoAuthorizationHeader":
            case "should401OnGETWithBadAuthorizationHeader":
            case "should401OnPOSTWithBadAuthorizationHeader":
            case "should401OnGETWithBadEncodedAuthorizationHeader":
            case "should401OnPOSTWithBadEncodedAuthorizationHeader":
            case "should401OnGETWithInvalidPasswordAuthorizationHeader":
            case "should401OnPOSTWithInvalidPasswordAuthorizationHeader":
            case "should200OnGETWithAuthorizationHeader":
            case "should200OnPOSTWithAuthorizationHeaderExplicitHandlerSetting":
                configureForAuthenticationWithHandlerClass(settings);
                break;
            case "should200OnPOSTWithAuthorizationHeader":
                configureForAuthentication(settings);
                break;
            case "should500OnGETWithEvaluationTimeout":
                settings.evaluationTimeout = 5000;
                settings.gremlinPool = 1;
                break;
        }
        return settings;
    }

    private void configureForAuthentication(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.authenticator = SimpleAuthenticator.class.getName();
        authSettings.authenticationHandler = SaslAndHttpBasicAuthenticationHandler.class.getName();

        // use a credentials graph with two users in it: stephen/password and marko/rainbow-dash
        final Map<String,Object> authConfig = new HashMap<>();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");

        authSettings.config = authConfig;
        settings.authentication = authSettings;
    }

    private void configureForAuthenticationWithHandlerClass(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.authenticator = SimpleAuthenticator.class.getName();

        //Add basic auth handler to make sure the reflection code path works.
        authSettings.authenticationHandler = SaslAndHttpBasicAuthenticationHandler.class.getName();

        // use a credentials graph with two users in it: stephen/password and marko/rainbow-dash
        final Map<String,Object> authConfig = new HashMap<>();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");

        authSettings.config = authConfig;
        settings.authentication = authSettings;
    }

    @Test
    public void should413OnPostWithResultTooLarge() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        final String bigPost = RandomStringUtils.random(32);
        httppost.setEntity(new StringEntity("{\"gremlin\":\""+ bigPost + "\", \"bindings\":{\"x\":\"10\"}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(413, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnGETWithNoAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnPOSTWithNoAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should400OnPOSTWithNonUUIDRequestId() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\", \"requestId\":\"nonsense\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(400, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnGETWithBadAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Authorization", "not-base-64-encoded");

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnPOSTWithBadAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Authorization", "not-base-64-encoded");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnGETWithBadEncodedAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Authorization", "Basic: not-base-64-encoded");

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnPOSTWithBadEncodedAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Authorization", "Basic: not-base-64-encoded");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnGETWithInvalidPasswordAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:not-my-password".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should401OnPOSTWithInvalidPasswordAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:not-my-password".getBytes()));
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(401, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should200OnGETWithAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithAuthorizationHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithAuthorizationHeaderExplicitHandlerSetting() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Authorization", "Basic " + encoder.encodeToString("stephen:password".getBytes()));
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentWithBindingsAndFunction() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=addItUp(Integer.parseInt(x),Integer.parseInt(y))&bindings.x=10&bindings.y=10"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(20, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnGETOverGremlinLangWithGremlinQueryStringArgumentWithIteratorResult() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=g.inject(111)&language=gremlin-lang"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(111, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentWithIteratorResult() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=gclassic.V()"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).size());
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentWithIteratorResultGraphBinary() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=gclassic.V()"));
        final String mime = SerTokens.MIME_GRAPHBINARY_V1;
        httpget.addHeader("Accept", mime);

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(mime, response.getEntity().getContentType().getValue());

            final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(TypeSerializerRegistry.INSTANCE);
            final ResponseMessage msg = serializer.deserializeResponse(toByteBuf(response.getEntity()));
            final List<Object> data = (List<Object>) msg.getResult().getData();
            assertEquals(6, data.size());
            for (Object o : data) {
                assertThat(o, instanceOf(Vertex.class));
            }
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentWithIteratorResultGraphBinaryToString() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=gclassic.V()"));
        final String mime = SerTokens.MIME_GRAPHBINARY_V1 + "-stringd";
        httpget.addHeader("Accept", mime);

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(mime, response.getEntity().getContentType().getValue());

            final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(TypeSerializerRegistry.INSTANCE);
            final ResponseMessage msg = serializer.deserializeResponse(toByteBuf(response.getEntity()));
            final List<Object> data = (List<Object>) msg.getResult().getData();
            assertEquals(6, data.size());
            for (Object o : data) {
                assertThat(o, instanceOf(String.class));
                assertThat((String) o, matchesRegex("v\\[\\d\\]"));
            }
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentWithIteratorResultTextPlain() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=gclassic.V()"));
        final String mime = "text/plain";
        httpget.addHeader("Accept", mime);

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(mime, response.getEntity().getContentType().getValue());
            final String text = EntityUtils.toString(response.getEntity());
            final String[] split = text.split(System.lineSeparator());
            assertEquals(6, split.length);
            for (String line : split) {
                assertThat(line, matchesRegex("==>v\\[\\d\\]"));
            }
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgument() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentReturningVertex() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=graph.addVertex('name','stephen')"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals("stephen", node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get("properties").get("name").get(0).get(GraphSONTokens.VALUEPROP).get(GraphSONTokens.VALUE).asText());
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentWithBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=Integer.parseInt(x)%2BInteger.parseInt(y)&bindings.x=10&bindings.y=10"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(20, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should400OnGETWithNoGremlinQueryStringArgument() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString());

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(400, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should200OnGETWithAnyAcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Accept", "*/*");

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }
    }

    @Test
    public void should400OnGETWithBadAcceptHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.addHeader("Accept", "application/json+something-else-that-does-not-exist");

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(400, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBody() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"2-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyForJavaTime() throws Exception {
        // basic test of java.time.* serialization over JSON from the server perspective. more complete tests
        // exist in gremlin-core
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"java.time.Instant.MAX\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(Instant.MAX, Instant.parse(node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asText()));
        }
    }

    @Test
    public void should200OnPOSTTransactionalGraph() throws Exception {

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"graph.addVertex('name','stephen');g.V().count()\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }

        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=g.V().count()"));
        httpget.addHeader("Accept", "application/json");

        // execute this a bunch of times so that there's a good chance a different thread on the server processes
        // the request
        for (int ix = 0; ix < 100; ix++) {
            try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
                assertEquals(200, response.getStatusLine().getStatusCode());
                assertEquals("application/json", response.getEntity().getContentType().getValue());
                final String json = EntityUtils.toString(response.getEntity());
                final JsonNode node = mapper.readTree(json);
                assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
            }
        }
    }

    @Test
    public void should200OnPOSTTransactionalGraphInStrictMode() throws Exception {

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g1.addV()\",\"aliases\":{\"g1\":\"g\"}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).size());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyWithIteratorResult() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"gclassic.V()\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).size());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyWithTinkerGraphResult() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory.createModern()\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode resultJson = mapper.readTree(json);
            final JsonNode data = resultJson.get("result").get("data");
            assertEquals(1, data.get(GraphSONTokens.VALUEPROP).size());

            assertEquals(6, data.get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(GraphSONTokens.VERTICES).size());
            assertEquals(6, data.get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(GraphSONTokens.EDGES).size());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyWithIteratorResultAndAliases() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g1.V()\",\"aliases\":{\"g1\":\"gclassic\"}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).size());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x+y\", \"bindings\":{\"x\":10, \"y\":10}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(20, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndLongBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":10}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(10, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndDoubleBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":10.5}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(10.5d, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).doubleValue(), 0.0001);
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndStringBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":\"10\"}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals("10", node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).textValue());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndBooleanBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":true}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertThat(node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).booleanValue(), is(true));
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndNullBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":null}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertThat(node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).isNull(), is(true));
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndArrayBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":[1,2,3]}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertThat(node.get("result").get("data").get(GraphSONTokens.VALUEPROP).isArray(), is(true));
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals(2, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(1).get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals(3, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(2).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinJsonEndcodedBodyAndMapBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x\", \"bindings\":{\"x\":{\"y\":1}}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals("g:Map", node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get("@type").asText());
            assertEquals(1, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(1).get(GraphSONTokens.VALUEPROP).asInt());
        }
    }

    @Test
    public void should400OnPOSTWithGremlinJsonEndcodedBodyAndBadBindings() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"x+y\", \"bindings\":10}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(400, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should400OnPOSTWithGremlinJsonEndcodedBodyWithNoGremlinKey() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremadfadflin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(400, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should400OnPOSTWithBadAcceptHeader() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Accept", "application/json+something-else-that-does-not-exist");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(400, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should200OnPOSTWithAnyAcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.addHeader("Accept", "*/*");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }
    }

    @Test
    public void should200OnPOSTWithComplexAcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "*.*;q=0.8,application/xhtml");
        httppost.addHeader("Accept", "*/*");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }
    }

    @Test
    public void should500OnGETWithGremlinEvalFailure() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1/0\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(500, response.getStatusLine().getStatusCode());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals("java.lang.ArithmeticException", node.get(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS).get(0).asText());
            assertEquals(1, node.get(Tokens.STATUS_ATTRIBUTE_EXCEPTIONS).size());
            assertThat(node.get(Tokens.STATUS_ATTRIBUTE_STACK_TRACE).asText(), containsString("Division by zero"));
        }
    }

    @Test
    public void should200OnPOSTWithGraphSON1d0AcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).asInt());
        }
    }

    @Test
    public void should200OnPOSTWithGraphSON2d0AcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).asInt());
        }
    }

    @Test
    public void should200OnPOSTWithGraphSON3d0AcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }
    }

    @Test
    public void should200OnPOSTWithAnyGraphSONAcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost1 = new HttpPost(TestClientFactory.createURLString());
        httppost1.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
        httppost1.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_JSON);
        httppost1.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost1)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).asInt());
        }

        final HttpPost httppost1Untyped = new HttpPost(TestClientFactory.createURLString());
        httppost1Untyped.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
        httppost1Untyped.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V1_UNTYPED);
        httppost1Untyped.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost1Untyped)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_GRAPHSON_V1_UNTYPED, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).asInt());
        }

        final HttpPost httppost2 = new HttpPost(TestClientFactory.createURLString());
        httppost2.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
        httppost2.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V2);
        httppost2.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost2)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_GRAPHSON_V2, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }

        final HttpPost httppost2Untyped = new HttpPost(TestClientFactory.createURLString());
        httppost2Untyped.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
        httppost2Untyped.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V2_UNTYPED);
        httppost2Untyped.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost2Untyped)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_GRAPHSON_V2_UNTYPED, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).asInt());
        }

        final HttpPost httppost3 = new HttpPost(TestClientFactory.createURLString());
        httppost3.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
        httppost3.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V3);
        httppost3.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost3)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_GRAPHSON_V3, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }

        final HttpPost httppost3Untyped = new HttpPost(TestClientFactory.createURLString());
        httppost3Untyped.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
        httppost3Untyped.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V3_UNTYPED);
        httppost3Untyped.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost3Untyped)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_GRAPHSON_V3_UNTYPED, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get("data").get(0).asInt());
        }
    }

    @Test
    public void should500WithResultThatCantBeSerialized() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(500, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertThat(node.get("message").asText(), startsWith("Could not serialize the result with "));
        }
    }

    @Test
    public void should200OnGETWithGremlinQueryStringArgumentCallingDatetimeFunction() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=datetime%28%272018-03-22T00%3A35%3A44.741%2B1600%27%29"));

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1521621344741L, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).longValue());
        }
    }

    @Test
    public void should500OnGETWithEvaluationTimeout() throws Exception {
        // Related to TINKERPOP-2769. This is a similar test to those for WebSocketChannelizer and UnifiedChannelizer.
        final CloseableHttpClient firstClient = HttpClients.createDefault();
        final CloseableHttpClient secondClient = HttpClients.createDefault();

        final HttpPost post = new HttpPost(TestClientFactory.createURLString());
        post.setEntity(new StringEntity("{\"gremlin\":\"g.addV('person').as('p').addE('self').to('p').iterate()\"}", Consts.UTF_8));
        try (final CloseableHttpResponse response = firstClient.execute(post)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // This query has a cycle, so it runs until it times out.
        final HttpGet firstGet = new HttpGet(TestClientFactory.createURLString("?gremlin=g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate()"));
        // Add a shorter timeout to the second query to ensure that its timeout is less than the first query's running time.
        final HttpGet secondGet = new HttpGet(TestClientFactory.createURLString("?gremlin=g.with('evaluationTimeout',1000).V().repeat(__.out()).until(__.outE().count().is(0)).iterate()"));

        final Callable<Integer> firstQueryWrapper = () -> {
            try (final CloseableHttpResponse response = firstClient.execute(firstGet)) {
                return response.getStatusLine().getStatusCode();
            }
        };

        final Callable<Integer> secondQueryWrapper = () -> {
            try (final CloseableHttpResponse response = secondClient.execute(secondGet)) {
                return response.getStatusLine().getStatusCode();
            }
        };
        final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);
        final Future<Integer> firstGetResult = threadPool.submit(firstQueryWrapper);
        // Schedule the second task with a slight delay so that it runs after the first task.
        final Future<Integer> secondGetResult = threadPool.schedule(secondQueryWrapper, 1500, TimeUnit.MILLISECONDS);

        // Make sure both requests return a response and don't hang.
        assertEquals(500, firstGetResult.get().intValue());
        assertEquals(500, secondGetResult.get().intValue());

        threadPool.shutdown();
    }

    private static ByteBuf toByteBuf(final HttpEntity httpEntity) throws IOException {
        final byte[] asArray = new byte[(int)httpEntity.getContentLength()];

        httpEntity.getContent().read(asArray);

        return Unpooled.wrappedBuffer(asArray);
    }
}
