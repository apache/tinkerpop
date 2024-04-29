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
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.http.Consts;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpVersion;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.EofSensorInputStream;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.io.ChunkedInputStream;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.channel.HttpChannelizer;
import org.apache.tinkerpop.gremlin.server.handler.HttpBasicAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.util.TextPlainMessageSerializerV4;
import org.apache.tinkerpop.gremlin.structure.io.Storage;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONTokens;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.apache.tinkerpop.gremlin.util.message.RequestMessageV4;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessageV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.apache.tinkerpop.gremlin.util.ser.SerializersV4;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;

import javax.script.SimpleBindings;
import java.io.IOException;
import java.lang.reflect.Field;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.server.handler.HttpRequestIdHandler.REQUEST_ID_HEADER_NAME;
import static org.apache.tinkerpop.gremlin.util.Tokens.ARGS_MATERIALIZE_PROPERTIES;
import static org.apache.tinkerpop.gremlin.util.Tokens.MATERIALIZE_PROPERTIES_ALL;
import static org.apache.tinkerpop.gremlin.util.Tokens.MATERIALIZE_PROPERTIES_TOKENS;
import static org.apache.tinkerpop.gremlin.util.Tokens.TIMEOUT_MS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
//            TODO: remove when consensus reached on old serializer support.
//            case "should200OnPOSTWithGraphSON1d0AcceptHeaderDefaultResultToJson":
//                settings.serializers.clear();
//                final Settings.SerializerSettings serializerSettingsV1 = new Settings.SerializerSettings();
//                serializerSettingsV1.className = GraphSONUntypedMessageSerializerV1.class.getName();
//                settings.serializers.add(serializerSettingsV1);
//                break;
            case "should200OnPOSTWithGraphSON4d0AcceptHeaderDefaultResultToJson":
                settings.serializers.clear();
                final Settings.SerializerSettings serializerSettingsV4 = new Settings.SerializerSettings();
                serializerSettingsV4.className = GraphSONUntypedMessageSerializerV4.class.getName();
                settings.serializers.add(serializerSettingsV4);
                break;
//            TODO: remove when consensus reached on old serializer support.
//            case "should200OnPOSTWithGraphSON2d0AcceptHeaderDefaultResultToJson":
//                settings.serializers.clear();
//                final Settings.SerializerSettings serializerSettingsV2 = new Settings.SerializerSettings();
//                serializerSettingsV2.className = GraphSONMessageSerializerV2.class.getName();
//                settings.serializers.add(serializerSettingsV2);
//                break;
//            case "should200OnPOSTWithGraphSON3d0AcceptHeaderDefaultResultToJson":
//                settings.serializers.clear();
//                final Settings.SerializerSettings serializerSettingsV3 = new Settings.SerializerSettings();
//                serializerSettingsV3.className = GraphSONMessageSerializerV3.class.getName();
//                settings.serializers.add(serializerSettingsV3);
//                break;
//            case "should200OnPOSTWithAnyGraphSONAcceptHeaderDefaultResultToJson":
//                settings.serializers.clear();
//                final Settings.SerializerSettings serializerSettingsUntypedV1 = new Settings.SerializerSettings();
//                serializerSettingsUntypedV1.className = GraphSONUntypedMessageSerializerV1.class.getName();
//                settings.serializers.add(serializerSettingsUntypedV1);
//                final Settings.SerializerSettings serializerSettingsTypedV1 = new Settings.SerializerSettings();
//                serializerSettingsTypedV1.className = GraphSONMessageSerializerV1.class.getName();
//                settings.serializers.add(serializerSettingsTypedV1);
//                final Settings.SerializerSettings serializerSettingsUntypedV2 = new Settings.SerializerSettings();
//                serializerSettingsUntypedV2.className = GraphSONUntypedMessageSerializerV2.class.getName();
//                settings.serializers.add(serializerSettingsUntypedV2);
//                final Settings.SerializerSettings serializerSettingsTypedV2 = new Settings.SerializerSettings();
//                serializerSettingsTypedV2.className = GraphSONMessageSerializerV2.class.getName();
//                settings.serializers.add(serializerSettingsTypedV2);
//                final Settings.SerializerSettings serializerSettingsUntypedV3 = new Settings.SerializerSettings();
//                serializerSettingsUntypedV3.className = GraphSONUntypedMessageSerializerV3.class.getName();
//                settings.serializers.add(serializerSettingsUntypedV3);
//                final Settings.SerializerSettings serializerSettingsTypedV3 = new Settings.SerializerSettings();
//                serializerSettingsTypedV3.className = GraphSONMessageSerializerV3.class.getName();
//                settings.serializers.add(serializerSettingsTypedV3);
//                break;
            case "should401OnPOSTWithNoAuthorizationHeader":
            case "should401OnGETWithBadAuthorizationHeader":
            case "should401OnPOSTWithBadAuthorizationHeader":
            case "should401OnGETWithBadEncodedAuthorizationHeader":
            case "should401OnPOSTWithBadEncodedAuthorizationHeader":
            case "should401OnGETWithInvalidPasswordAuthorizationHeader":
            case "should401OnPOSTWithInvalidPasswordAuthorizationHeader":
            case "should200OnPOSTWithAuthorizationHeaderExplicitHandlerSetting":
                configureForAuthenticationWithHandlerClass(settings);
                break;
            case "should200OnPOSTWithAuthorizationHeader":
                configureForAuthentication(settings);
                break;
            case "should500OnPOSTWithEvaluationTimeout":
                settings.evaluationTimeout = 5000;
                settings.gremlinPool = 1;
                break;
            case "should200OnPOSTWithChunkedResponse":
            case "shouldHandleErrorsInFirstChunkPOSTWithChunkedResponse":
            case "shouldHandleErrorsInFirstChunkPOSTWithChunkedResponseUsingTextPlain":
            case "shouldHandleErrorsNotInFirstChunkPOSTWithChunkedResponse":
            case "shouldHandleErrorsNotInFirstChunkPOSTWithChunkedResponseUsingTextPlain":
            case "should200OnPOSTWithChunkedResponseGraphBinary":
            case "should200OnPOSTWithChunkedResponseUsingTextPlain":
                settings.resultIterationBatchSize = 16;
                break;
        }
        return settings;
    }

    private void configureForAuthentication(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.authenticator = SimpleAuthenticator.class.getName();
        authSettings.authenticationHandler = HttpBasicAuthenticationHandler.class.getName();

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
        authSettings.authenticationHandler = HttpBasicAuthenticationHandler.class.getName();

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
    public void should405OnGETRequest() throws Exception {
        // /gremlin endpoint only allows POST request for now until GET is implemented to return status
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString());

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(405, response.getStatusLine().getStatusCode());
            assertTrue(response.containsHeader(REQUEST_ID_HEADER_NAME));
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
            assertTrue(response.containsHeader(REQUEST_ID_HEADER_NAME));
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
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
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
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinQueryStringArgumentWithBindingsAndFunction() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"addItUp(Integer.parseInt(x),Integer.parseInt(y))\",\"bindings\":{\"x\":\"10\", \"y\":\"10\"}}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(20, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void should200OnPOSTOverGremlinLangWithGremlinQueryStringArgumentWithIteratorResult() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(111)\",\"language\":\"gremlin-lang\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(111, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
        }
    }

    @Test
    public void shouldHaveSameVertexResultsWithGremlinLangOrGremlinGroovy() throws Exception {
        String gremlinLangResult;
        String gremlinGroovyResult;

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V()\",\"language\":\"gremlin-lang\",\"g\":\"gmodern\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            gremlinLangResult = node.get("result").get(GraphSONTokens.VALUEPROP).toString();
        }

        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V()\",\"language\":\"gremlin-groovy\",\"g\":\"gmodern\"}", Consts.UTF_8));
        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            gremlinGroovyResult = node.get("result").get(GraphSONTokens.VALUEPROP).toString();
        }

        assertEquals(gremlinGroovyResult, gremlinLangResult);
    }

    @Test
    public void shouldNotHaveBindingsAffectLaterQueries() throws Exception {
        final String firstResult;
        final String secondResult;

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());

        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V()\"}", Consts.UTF_8));
        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            firstResult = node.get("result").get(GraphSONTokens.VALUEPROP).toString();
        }

        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V()\",\"g\":\"gmodern\"}", Consts.UTF_8));
        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            EntityUtils.toString(response.getEntity());
        }

        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V()\"}", Consts.UTF_8));
        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            secondResult = node.get("result").get(GraphSONTokens.VALUEPROP).toString();
        }

        assertEquals(firstResult, secondResult);
    }

    @Test
    public void should200OnPOSTWithGremlinQueryStringArgumentWithIteratorResult() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"gclassic.V()\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get(GraphSONTokens.VALUEPROP).size());
        }
    }

    //            TODO: remove when consensus reached on old serializer support.
//    @Test
//    public void should200OnPOSTWithGremlinQueryStringArgumentWithIteratorResultGraphBinaryToString() throws Exception {
//        final CloseableHttpClient httpclient = HttpClients.createDefault();
//        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
//        httppost.setEntity(new StringEntity("{\"gremlin\":\"gclassic.V()\"}", Consts.UTF_8));
//        final String mime = SerTokens.MIME_GRAPHBINARY_V1 + "-stringd";
//        httppost.addHeader("Accept", mime);
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(mime, response.getEntity().getContentType().getValue());
//
//            final GraphBinaryMessageSerializerV1 serializer = new GraphBinaryMessageSerializerV1(TypeSerializerRegistry.INSTANCE);
//            final ResponseMessage msg = serializer.deserializeResponse(toByteBuf(response.getEntity()));
//            final List<Object> data = (List<Object>) msg.getResult().getData();
//            assertEquals(6, data.size());
//            for (Object o : data) {
//                assertThat(o, instanceOf(String.class));
//                assertThat((String) o, matchesRegex("v\\[\\d\\]"));
//            }
//        }
//    }

    //            TODO: remove when consensus reached on old serializer support.
//    @Test
//    public void should200OnPOSTWithGremlinQueryStringArgumentWithIteratorResultTextPlain() throws Exception {
//        final CloseableHttpClient httpclient = HttpClients.createDefault();
//        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
//        httppost.setEntity(new StringEntity("{\"gremlin\":\"gclassic.V()\"}", Consts.UTF_8));
//        final String mime = "text/plain";
//        httppost.addHeader("Accept", mime);
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(mime, response.getEntity().getContentType().getValue());
//            final String text = EntityUtils.toString(response.getEntity());
//            final String[] split = text.split(System.lineSeparator());
//            assertEquals(6, split.length);
//            for (String line : split) {
//                assertThat(line, matchesRegex("==>v\\[\\d\\]"));
//            }
//        }
//    }

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
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
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
            assertEquals(Instant.MAX, Instant.parse(node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asText()));
        }
    }

    /*@Test disabled for now as current implementation doesn't support implicit transactions.
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
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
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
                assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
            }
        }
    } */

    @Test
    public void should200OnPOSTTransactionalGraphInStrictMode() throws Exception {

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.addV()\",\"g\":\"g\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).size());
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
            assertEquals(6, node.get("result").get(GraphSONTokens.VALUEPROP).size());
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
            final JsonNode data = resultJson.get("result");
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
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V()\",\"g\":\"gclassic\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get(GraphSONTokens.VALUEPROP).size());
            assertEquals(GraphSONTokens.VERTEX,
                    node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(GraphSONTokens.LABEL).asText());
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
            assertEquals(20, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
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
            assertEquals(10, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
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
            assertEquals(10.5d, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).doubleValue(), 0.0001);
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
            assertEquals("10", node.get("result").get(GraphSONTokens.VALUEPROP).get(0).textValue());
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
            assertThat(node.get("result").get(GraphSONTokens.VALUEPROP).get(0).booleanValue(), is(true));
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
            assertThat(node.get("result").get(GraphSONTokens.VALUEPROP).get(0).isNull(), is(true));
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
            assertThat(node.get("result").get(GraphSONTokens.VALUEPROP).isArray(), is(true));
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals(2, node.get("result").get(GraphSONTokens.VALUEPROP).get(1).get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals(3, node.get("result").get(GraphSONTokens.VALUEPROP).get(2).get(GraphSONTokens.VALUEPROP).intValue());
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
            assertEquals("g:Map", node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get("@type").asText());
            assertEquals(1, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(1).get(GraphSONTokens.VALUEPROP).asInt());
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
            assertEquals(0, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
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
            assertEquals(0, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
        }
    }

    @Test
    public void should500OnPOSTWithGremlinEvalFailure() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1/0\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode()); // Temporarily 200 OK.
            assertTrue(response.containsHeader(REQUEST_ID_HEADER_NAME));
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals("Division by zero", node.get("status").get("message").asText());
        }
    }

//            TODO: remove when consensus reached on old serializer support.
//    @Test
//    public void should200OnPOSTWithGraphSON1d0AcceptHeaderDefaultResultToJson() throws Exception {
//        final CloseableHttpClient httpclient = HttpClients.createDefault();
//        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
//        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).asInt());
//        }
//    }

    @Test
    public void should200OnPOSTWithGraphSON4d0AcceptHeaderDefaultResultToJson() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get(0).asInt());
        }
    }

    //            TODO: remove when consensus reached on old serializer support.
//    @Test
//    public void should200OnPOSTWithGraphSON2d0AcceptHeaderDefaultResultToJson() throws Exception {
//        final CloseableHttpClient httpclient = HttpClients.createDefault();
//        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
//        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).asInt());
//        }
//    }

    //            TODO: remove when consensus reached on old serializer support.
//    @Test
//    public void should200OnPOSTWithGraphSON3d0AcceptHeaderDefaultResultToJson() throws Exception {
//        final CloseableHttpClient httpclient = HttpClients.createDefault();
//        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
//        httppost.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
//        }
//    }

    //            TODO: remove when consensus reached on old serializer support.
//    @Test
//    public void should200OnPOSTWithAnyGraphSONAcceptHeaderDefaultResultToJson() throws Exception {
//        final CloseableHttpClient httpclient = HttpClients.createDefault();
//        final HttpPost httppost1 = new HttpPost(TestClientFactory.createURLString());
//        httppost1.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
//        httppost1.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_JSON);
//        httppost1.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost1)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).asInt());
//        }

        //            TODO: remove when consensus reached on old serializer support.
//        final HttpPost httppost1Untyped = new HttpPost(TestClientFactory.createURLString());
//        httppost1Untyped.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
//        httppost1Untyped.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V1_UNTYPED);
//        httppost1Untyped.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost1Untyped)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_GRAPHSON_V1_UNTYPED, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).asInt());
//        }
//
//        final HttpPost httppost2 = new HttpPost(TestClientFactory.createURLString());
//        httppost2.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
//        httppost2.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V2);
//        httppost2.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost2)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_GRAPHSON_V2, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).get(GraphSONTokens.VALUEPROP).asInt());
//        }
//
//        final HttpPost httppost2Untyped = new HttpPost(TestClientFactory.createURLString());
//        httppost2Untyped.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
//        httppost2Untyped.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V2_UNTYPED);
//        httppost2Untyped.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost2Untyped)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_GRAPHSON_V2_UNTYPED, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).asInt());
//        }
//
//        final HttpPost httppost3 = new HttpPost(TestClientFactory.createURLString());
//        httppost3.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
//        httppost3.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V3);
//        httppost3.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost3)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_GRAPHSON_V3, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).asInt());
//        }
//
//        final HttpPost httppost3Untyped = new HttpPost(TestClientFactory.createURLString());
//        httppost3Untyped.setHeader(HttpHeaders.CONTENT_TYPE, SerTokens.MIME_JSON);
//        httppost3Untyped.setHeader(HttpHeaders.ACCEPT, SerTokens.MIME_GRAPHSON_V3_UNTYPED);
//        httppost3Untyped.setEntity(new StringEntity("{\"gremlin\":\"1-1\"}", Consts.UTF_8));
//
//        try (final CloseableHttpResponse response = httpclient.execute(httppost3Untyped)) {
//            assertEquals(200, response.getStatusLine().getStatusCode());
//            assertEquals(SerTokens.MIME_GRAPHSON_V3_UNTYPED, response.getEntity().getContentType().getValue());
//            final String json = EntityUtils.toString(response.getEntity());
//            final JsonNode node = mapper.readTree(json);
//            assertEquals(0, node.get("result").get("data").get(0).asInt());
//        }
//    }

    @Test
    public void should500WithResultThatCantBeSerialized() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode()); // Temporarily 200
            assertEquals(SerTokens.MIME_JSON, response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertThat(node.get("status").get("message").asText(), startsWith("Error during serialization: Could not find a type identifier for the class"));
        }
    }

    @Test
    public void should200OnPOSTWithGremlinQueryStringArgumentCallingDatetimeFunction() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setEntity(new StringEntity("{\"gremlin\":\"datetime('2018-03-22T00:35:44.741+1600')\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals("application/json", response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(1521621344741L, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).longValue());
        }
    }

    @Test(timeout = 10000) // Add test timeout to prevent incorrect timeout behavior from stopping test run.
    public void should500OnPOSTWithEvaluationTimeout() throws Exception {
        // Related to TINKERPOP-2769. This is a similar test to the one for the WebSocketChannelizer.
        final CloseableHttpClient firstClient = HttpClients.createDefault();
        final CloseableHttpClient secondClient = HttpClients.createDefault();

        final HttpPost post = new HttpPost(TestClientFactory.createURLString());
        post.setEntity(new StringEntity("{\"gremlin\":\"g.addV('person').as('p').addE('self').to('p').iterate()\"}", Consts.UTF_8));
        try (final CloseableHttpResponse response = firstClient.execute(post)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
        }

        // This query has a cycle, so it runs until it times out.
        final HttpPost firstPost = new HttpPost(TestClientFactory.createURLString());
        firstPost.setEntity(new StringEntity("{\"gremlin\":\"g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate()\"}", Consts.UTF_8));
        // Add a shorter timeout to the second query to ensure that its timeout is less than the first query's running time.
        final HttpPost secondPost = new HttpPost(TestClientFactory.createURLString());
        secondPost.setEntity(new StringEntity("{\"gremlin\":\"g.V().repeat(__.out()).until(__.outE().count().is(0)).iterate()\"}", Consts.UTF_8));

        final Callable<Integer> firstQueryWrapper = () -> {
            try (final CloseableHttpResponse response = firstClient.execute(firstPost)) {
                return response.getStatusLine().getStatusCode();
            }
        };

        final Callable<Integer> secondQueryWrapper = () -> {
            try (final CloseableHttpResponse response = secondClient.execute(secondPost)) {
                return response.getStatusLine().getStatusCode();
            }
        };
        final ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(2);
        final Future<Integer> firstGetResult = threadPool.submit(firstQueryWrapper);
        // Schedule the second task with a slight delay so that it runs after the first task.
        final Future<Integer> secondGetResult = threadPool.schedule(secondQueryWrapper, 1500, TimeUnit.MILLISECONDS);

        // Make sure both requests return a response and don't hang.
        assertEquals(200, firstGetResult.get().intValue());
        assertEquals(200, secondGetResult.get().intValue());

        threadPool.shutdown();
    }
    @Test
    public void shouldErrorWhenTryingToConnectWithHttp1() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpGet httpget = new HttpGet(TestClientFactory.createURLString("?gremlin=2-1"));
        httpget.setProtocolVersion(HttpVersion.HTTP_1_0);

        try (final CloseableHttpResponse response = httpclient.execute(httpget)) {
            assertEquals(505, response.getStatusLine().getStatusCode());
        }
    }

    @Test
    public void should200OnPOSTWithChunkedResponse() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        // chunk size is 8, so should be 2 chunks
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(0,1,2,3,4,5,6,7,8,9,'ten',11,12,13,14,15,'new chunk')\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(8, node.get("result").get(GraphSONTokens.VALUEPROP).get(8).get(GraphSONTokens.VALUEPROP).intValue());
            assertEquals("ten", node.get("result").get(GraphSONTokens.VALUEPROP).get(10).textValue());
            assertEquals("new chunk", node.get("result").get(GraphSONTokens.VALUEPROP).get(16).textValue());

            final Header[] footers = getTrailingHeaders(response);
            assertEquals(2, footers.length);
            assertEquals("code", footers[0].getName());
            assertEquals("200", footers[0].getValue());
            assertEquals("message", footers[1].getName());
            assertEquals("OK", footers[1].getValue());
        }
    }

    @Test
    public void should200OnPOSTWithChunkedResponseGraphBinary() throws Exception {
        final String gremlin = "inject(0,1,2,3,4,5,6,7,8,9,'ten',11,12,13,14,15,'new chunk')";
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build(gremlin).create(), new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, SerializersV4.GRAPHBINARY_V4.getValue());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHBINARY_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            final ResponseMessageV4 responseMessage = serializer.readChunk(toByteBuf(response.getEntity()), true);
            assertEquals(17, ((List)responseMessage.getResult().getData()).size());

            final Header[] footers = getTrailingHeaders(response);
            assertEquals(2, footers.length);
            assertEquals("code", footers[0].getName());
            assertEquals("200", footers[0].getValue());
            assertEquals("message", footers[1].getName());
            assertEquals("OK", footers[1].getValue());
        }
    }

    @Test
    public void should200OnPOSTWithEmptyChunkedResponseGraphBinary() throws Exception {
        final String gremlin = "g.V().iterate()";
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build(gremlin).create(), new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, SerializersV4.GRAPHBINARY_V4.getValue());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHBINARY_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            final ResponseMessageV4 responseMessage = serializer.readChunk(toByteBuf(response.getEntity()), true);
            assertEquals(0, ((List)responseMessage.getResult().getData()).size());

            final Header[] footers = getTrailingHeaders(response);
            assertEquals(2, footers.length);
            assertEquals("code", footers[0].getName());
            assertEquals("200", footers[0].getValue());
            assertEquals("message", footers[1].getName());
            assertEquals("OK", footers[1].getValue());
        }
    }

    @Test
    public void should200OnPOSTWithChunkedResponseUsingTextPlain() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httppost.addHeader(HttpHeaders.ACCEPT, "text/plain");
        // chunk size is 8, so should be 2 chunks
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(0,1,2,3,4,5,6,7,8,9,'ten',11,12,13,14,15,'new chunk')\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            String textPlainResponse = EntityUtils.toString(response.getEntity());
            assertTrue(textPlainResponse.startsWith("==>0"));
            assertTrue(textPlainResponse.endsWith(System.lineSeparator() + "==>new chunk"));
        }
    }

    @Test
    public void should200OnPOSTWithEmptyChunkedResponseUsingTextPlain() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httppost.addHeader(HttpHeaders.ACCEPT, "text/plain");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.V().iterate()\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            String textPlainResponse = EntityUtils.toString(response.getEntity());
            assertEquals("", textPlainResponse);
        }
    }

    @Test
    public void shouldHandleErrorsInFirstChunkPOSTWithChunkedResponseUsingTextPlain() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httppost.addHeader(HttpHeaders.ACCEPT, "text/plain");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(0,1,2,3,4,5,6,7,8,9,'ten',11,12,13,14,15,16).coalesce(is(lt(0)),fail('some error'))\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            String textPlainResponse = EntityUtils.toString(response.getEntity());
            assertTrue(textPlainResponse.startsWith("some error"));
        }
    }

    @Test
    public void shouldHandleErrorsNotInFirstChunkPOSTWithChunkedResponseUsingTextPlain() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, "application/json");
        httppost.addHeader(HttpHeaders.ACCEPT, "text/plain");
        httppost.setEntity(new StringEntity("{\"gremlin\":\"g.inject(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17).coalesce(is(lt(17)),fail('some error'))\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.getEntity().isChunked());

            String textPlainResponse = EntityUtils.toString(response.getEntity());
            assertTrue(textPlainResponse.startsWith("==>0"));
            assertTrue(textPlainResponse.contains("==>5"));
            assertTrue(textPlainResponse.contains("==>15"));
            assertTrue(textPlainResponse.endsWith(System.lineSeparator() + "some error"));
        }
    }

    @Test
    public void shouldHandleErrorsInFirstChunkPOSTWithChunkedResponse() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        // default chunk size is 16, so should be 2 chunks
        httppost.setEntity(new StringEntity(
                "{\"gremlin\":\"g.inject(0,1,2,3,4,5,6,7,8,9,'ten',11,12,13,14,15,16).coalesce(is(lt(0)),fail('some error'))\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.containsHeader(REQUEST_ID_HEADER_NAME));
            assertTrue(response.getEntity().isChunked());

            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertThat(node.get("status").get("message").textValue(), startsWith("some error"));
            assertEquals(500, node.get("status").get("code").intValue());

            final Header[] footers = getTrailingHeaders(response);
            assertEquals(2, footers.length);
            assertEquals("code", footers[0].getName());
            assertEquals("500", footers[0].getValue());
            assertEquals("message", footers[1].getName());
            assertThat(footers[1].getValue(), startsWith("some+error"));
        }
    }

    @Test
    public void shouldHandleErrorsNotInFirstChunkPOSTWithChunkedResponse() throws Exception {
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        // chunk size is 16, so should be 2 chunks
        httppost.setEntity(new StringEntity(
                "{\"gremlin\":\"g.inject(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17).coalesce(is(lt(17)),fail('some error'))\"}", Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.containsHeader(REQUEST_ID_HEADER_NAME));
            assertTrue(response.getEntity().isChunked());

            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(0, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).intValue());
            Assert.assertThat(node.get("status").get("message").textValue(), startsWith("some error"));
            assertEquals(500, node.get("status").get("code").intValue());

            final Header[] footers = getTrailingHeaders(response);
            assertEquals(2, footers.length);
            assertEquals("code", footers[0].getName());
            assertEquals("500", footers[0].getValue());
            assertEquals("message", footers[1].getName());
            Assert.assertThat(footers[1].getValue(), startsWith("some+error"));
        }
    }

    private Header[] getTrailingHeaders(final CloseableHttpResponse response) throws IOException, NoSuchFieldException, IllegalAccessException {
        final EofSensorInputStream content = (EofSensorInputStream) response.getEntity().getContent();
        final Field field = content.getClass().getDeclaredField("wrappedStream");
        field.setAccessible(true);
        final ChunkedInputStream stream = (ChunkedInputStream) field.get(content);
        return stream.getFooters();
    }

    @Test
    public void should200OnPOSTWithValidGraphSONBytecodeRequest() throws Exception {
        final Bytecode bytecodeQuery = EmptyGraph.instance().traversal().V().asAdmin().getBytecode();
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build(bytecodeQuery).addG("gmodern").create(),
                new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, SerializersV4.GRAPHSON_V4.getValue());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHSON_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerializersV4.GRAPHSON_V4.getValue(), response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            final JsonNode dataArray = node.get("result").get(GraphSONTokens.VALUEPROP);
            assertTrue(dataArray.isArray());
            assertEquals(6, dataArray.size());
        }
    }

    @Test
    public void should200OnPOSTWithValidGraphBinaryBytecodeRequest() throws Exception {
        final Bytecode bytecodeQuery = EmptyGraph.instance().traversal().V().asAdmin().getBytecode();
        final GraphBinaryMessageSerializerV4 serializer = new GraphBinaryMessageSerializerV4();
//        final Map<String, String> aliases = new HashMap<>();
//        aliases.put("g", "gmodern");
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build(bytecodeQuery).addG("gmodern").create(),
//                RequestMessage.build(Tokens.OPS_BYTECODE).addArg(Tokens.ARGS_GREMLIN, bytecodeQuery).addArg(Tokens.ARGS_ALIASES, aliases).create(),
                new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, SerializersV4.GRAPHBINARY_V4.getValue());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHBINARY_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerializersV4.GRAPHBINARY_V4.getValue(), response.getEntity().getContentType().getValue());
            final ResponseMessageV4 respMsg = serializer.deserializeBinaryResponse(toByteBuf(response.getEntity()));
            Object results = respMsg.getResult().getData();
            assertTrue(results instanceof List);
            assertEquals(6, ((List) results).size());
        }
    }

    @Test
    public void should200OnPOSTWithGremlinGraphSONEndcodedBodyAndDoubleBindings() throws Exception {
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();
        final SimpleBindings bindings = new SimpleBindings();
        bindings.put("x", 10.5d);
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build("x").addBindings(bindings).create(),
                new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHSON_V4.getValue());
        httppost.addHeader("Content-Type", SerializersV4.GRAPHSON_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertEquals(SerializersV4.GRAPHSON_V4.getValue(), response.getEntity().getContentType().getValue());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(10.5d, node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).doubleValue(), 0.0001);
        }
    }

    @Test
    public void should400OnPOSTWithInvalidRequestArgsWhenInvalidBindingsSupplied() throws Exception {
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build("g.V(id)").addBinding("id", "1").create(),
                new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHSON_V4.getValue());
        httppost.addHeader("Content-Type", SerializersV4.GRAPHSON_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertTrue(node.get("status").get("message").asText().contains("The [eval] message supplies one or more invalid parameters key"));
        }
    }

    @Test
    public void should400OnPOSTWithInvalidRequestArgsWhenAliasesNotSuppliedForBytecode() throws Exception {
        final Bytecode bytecodeQuery = EmptyGraph.instance().traversal().V().asAdmin().getBytecode();
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build(bytecodeQuery).create(), new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader(HttpHeaders.CONTENT_TYPE, SerializersV4.GRAPHSON_V4.getValue());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHSON_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertTrue(node.get("status").get("message").asText().contains("A [bytecode] message requires a [g] argument"));
        }
    }

    @Test
    public void shouldErrorOnBytecodeGremlinFromApplicationJsonPostRequest() throws Exception {
        final Bytecode bytecodeQuery = EmptyGraph.instance().traversal().V().asAdmin().getBytecode();
        final UUID requestId = UUID.fromString("1e55c495-22d5-4a39-934a-a2744ba010ef");
        final String body = "{ \"gremlin\": \"" + bytecodeQuery + "\", \"requestId\": \"" + requestId + "\", \"g\": \"gmodern" + "\", \"language\":  \"gremlin-lang\"}";
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity(body, Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final JsonNode node = mapper.readTree(EntityUtils.toString(response.getEntity()));
            assertTrue(node.get("status").get("message").asText().contains("Failed to interpret Gremlin query"));
        }
    }

    @Test
    public void shouldIgnoreRequestIdInPostRequest() throws Exception {
        final UUID requestId = UUID.fromString("1e55c495-22d5-4a39-934a-a2744ba010ef");
        final String body = "{ \"gremlin\": \"" + "g.V()" + "\", \"requestId\": \"" + requestId + "\", \"g\": \"gmodern" + "\", \"language\":  \"gremlin-lang\"}";
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity(body, Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            assertTrue(response.containsHeader(REQUEST_ID_HEADER_NAME));
            assertNotEquals(requestId, UUID.fromString(response.getLastHeader(REQUEST_ID_HEADER_NAME).getValue()));
        }
    }

    @Test
    public void should100onExpectContinueRequest() throws Exception {
        final GraphSONMessageSerializerV4 serializer = new GraphSONMessageSerializerV4();
        final ByteBuf serializedRequest = serializer.serializeRequestAsBinary(
                RequestMessageV4.build("gmodern.V()").create(),
                new UnpooledByteBufAllocator(false));

        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.setConfig(RequestConfig.custom().setExpectContinueEnabled(true).build());
        httppost.addHeader(HttpHeaders.ACCEPT, SerializersV4.GRAPHSON_V4.getValue());
        httppost.addHeader("Content-Type", SerializersV4.GRAPHSON_V4.getValue());
        httppost.setEntity(new ByteArrayEntity(serializedRequest.array()));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());
            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertEquals(6, node.get("result").get(GraphSONTokens.VALUEPROP).size());
        }
    }

    @Test
    public void shouldAcceptTimeoutInRequestBody() throws Exception {
        final String body = "{ \"gremlin\": \"" + "Thread.sleep(5000)" + "\",\"language\":\"gremlin-groovy\",\"" + TIMEOUT_MS + "\":\"100\"}";
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity(body, Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());

            final String json = EntityUtils.toString(response.getEntity());
            assertTrue(json.contains("timeout occurred"));
        }
    }

    @Test
    public void shouldAcceptMaterializePropertiesAllInRequestBody() throws Exception {
        final String body = "{ \"gremlin\": \"" + "gmodern.V().limit(1)" + "\",\"language\":\"gremlin-groovy\",\""
                + ARGS_MATERIALIZE_PROPERTIES + "\":\"" + MATERIALIZE_PROPERTIES_ALL + "\"}";
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity(body, Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());

            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertNotNull(node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(GraphSONTokens.PROPERTIES));
        }
    }

    @Test
    public void shouldAcceptMaterializePropertiesTokensInRequestBody() throws Exception {
        final String body = "{ \"gremlin\": \"" + "gmodern.V().limit(1)" + "\",\"language\":\"gremlin-groovy\",\""
                + ARGS_MATERIALIZE_PROPERTIES + "\":\"" + MATERIALIZE_PROPERTIES_TOKENS + "\"}";
        final CloseableHttpClient httpclient = HttpClients.createDefault();
        final HttpPost httppost = new HttpPost(TestClientFactory.createURLString());
        httppost.addHeader("Content-Type", "application/json");
        httppost.setEntity(new StringEntity(body, Consts.UTF_8));

        try (final CloseableHttpResponse response = httpclient.execute(httppost)) {
            assertEquals(200, response.getStatusLine().getStatusCode());

            final String json = EntityUtils.toString(response.getEntity());
            final JsonNode node = mapper.readTree(json);
            assertNull(node.get("result").get(GraphSONTokens.VALUEPROP).get(0).get(GraphSONTokens.VALUEPROP).get(GraphSONTokens.PROPERTIES));
        }
    }

    private static ByteBuf toByteBuf(final HttpEntity httpEntity) throws IOException {
        final byte[] asArray = EntityUtils.toByteArray(httpEntity);
        return Unpooled.wrappedBuffer(asArray);
    }
}
