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
package org.apache.tinkerpop.gremlin.server.handler;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV1;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONMessageSerializerV3;
import org.apache.tinkerpop.gremlin.util.ser.SerTokens;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HttpGremlinEndpointHandlerTest {

    @Test
    public void shouldReleaseRequestWithEmptySerializedBody() {
        assertMalformedSerializedRequestReleased(new byte[0]);
    }

    @Test
    public void shouldReleaseRequestWithUnsignedMimeTypeLength() {
        final byte[] payload = new byte[129];
        payload[0] = (byte) 0x80;

        assertMalformedSerializedRequestReleased(payload);
    }

    @Test
    public void shouldReleaseRequestWithTruncatedMimeTypeHeader() {
        assertMalformedSerializedRequestReleased(new byte[] { 5, 'a', 'b' });
    }

    @Test
    public void shouldReleaseRequestWhenRuntimeExceptionIsThrown() {
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
        buffer.writeCharSequence("{\"gremlin\":\"g.V()\"}", CharsetUtil.UTF_8);

        final DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, "application/json");
        headers.add(HttpHeaderNames.ACCEPT, "application/json;q=invalid");
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
                buffer, headers, new DefaultHttpHeaders());
        final HttpGremlinEndpointHandler handler = new HttpGremlinEndpointHandler(
                Collections.emptyMap(), null, null, null);

        try {
            handler.channelRead(null, request);
            fail("NumberFormatException expected");
        } catch (NumberFormatException ex) {
            assertEquals("For input string: \"invalid\"", ex.getMessage());
        }

        assertEquals(0, request.refCnt());
    }

    private void assertMalformedSerializedRequestReleased(final byte[] payload) {
        final ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer(payload.length);
        buffer.writeBytes(payload);

        final DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V1);
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
                buffer, headers, new DefaultHttpHeaders());

        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1());
        final EmbeddedChannel channel = new EmbeddedChannel(
                new HttpGremlinEndpointHandler(serializers, null, null, null));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(BAD_REQUEST, response.status());
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }

    /**
     * A serialized GraphSON request is authorized on the {@link RequestMessage} it deserializes to.
     */
    @Test
    public void shouldAuthorizeDeserializedArgsOfSerializedRequest() throws Exception {
        final RequestMessage executed = RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, "g.V().values('name')").create();
        final String canonical = graphsonRequestJson(executed);
        // Add a top-level key that the GraphSON RequestMessage deserializer does not read.
        final String withTopLevelGremlin = "{\"gremlin\":\"g.V().count()\"," + canonical.substring(1);

        final RecordingAuthorizer authorizer = new RecordingAuthorizer(true);
        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHSON_V3, new GraphSONMessageSerializerV3());

        final ByteBuf body = ByteBufAllocator.DEFAULT.buffer();
        body.writeBytes(withTopLevelGremlin.getBytes(StandardCharsets.UTF_8));
        final DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHSON_V3);
        final FullHttpRequest request = new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/",
                body, headers, new DefaultHttpHeaders());

        final EmbeddedChannel channel = new EmbeddedChannel(
                new HttpGremlinEndpointHandler(serializers, null, null, settingsWith(authorizer)));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(UNAUTHORIZED, response.status());
            assertTrue(authorizer.scriptAuthorizeCalled);
            assertEquals("g.V().values('name')", authorizer.lastScript);
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }

    /**
     * Bytecode HTTP requests are authorized through
     * {@link Authorizer#authorize(AuthenticatedUser, Bytecode, java.util.Map)} rather than the script form.
     */
    @Test
    public void shouldRouteBytecodeRequestsToBytecodeAuthorize() throws Exception {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        final RequestMessage msg = RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .addArg(Tokens.ARGS_GREMLIN, bytecode)
                .addArg(Tokens.ARGS_ALIASES, Collections.singletonMap("g", "g")).create();

        final RecordingAuthorizer authorizer = new RecordingAuthorizer(true);
        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1());
        final FullHttpRequest request = binaryRequest(msg);

        final EmbeddedChannel channel = new EmbeddedChannel(
                new HttpGremlinEndpointHandler(serializers, null, null, settingsWith(authorizer)));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(UNAUTHORIZED, response.status());
            assertTrue(authorizer.bytecodeAuthorizeCalled);
            assertFalse(authorizer.scriptAuthorizeCalled);
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }

    /**
     * With an authorizer configured, a bytecode {@code gremlin} sent under a non-bytecode op is answered with
     * {@code 400} before authorization, as authorization is routed by op.
     */
    @Test
    public void shouldRejectBytecodeUnderEvalOpWhenAuthorizerConfigured() throws Exception {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        assertRejectedBeforeAuthorization(RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, bytecode)
                .addArg(Tokens.ARGS_ALIASES, Collections.singletonMap("g", "g")).create());
    }

    @Test
    public void shouldRejectScriptUnderBytecodeOpWhenAuthorizerConfigured() throws Exception {
        assertRejectedBeforeAuthorization(RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .addArg(Tokens.ARGS_GREMLIN, "g.V()")
                .addArg(Tokens.ARGS_ALIASES, Collections.singletonMap("g", "g")).create());
    }

    /**
     * With an authorizer configured, bytecode must carry exactly one alias named {@code g}, as on the WebSocket
     * path, and is answered with {@code 400} otherwise.
     */
    @Test
    public void shouldRejectBytecodeWithMultipleAliasesWhenAuthorizerConfigured() throws Exception {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        final Map<String, String> aliases = new HashMap<>();
        aliases.put("g", "gmodern");
        aliases.put("h", "gcrew");
        assertRejectedBeforeAuthorization(RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .addArg(Tokens.ARGS_GREMLIN, bytecode)
                .addArg(Tokens.ARGS_ALIASES, aliases).create());
    }

    @Test
    public void shouldRejectBytecodeWithoutAliasesWhenAuthorizerConfigured() throws Exception {
        final Bytecode bytecode = new Bytecode();
        bytecode.addStep("V");
        assertRejectedBeforeAuthorization(RequestMessage.build(Tokens.OPS_BYTECODE)
                .processor("traversal")
                .addArg(Tokens.ARGS_GREMLIN, bytecode).create());
    }

    private static void assertRejectedBeforeAuthorization(final RequestMessage msg) throws Exception {
        final RecordingAuthorizer authorizer = new RecordingAuthorizer(false);
        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1());
        final FullHttpRequest request = binaryRequest(msg);

        final EmbeddedChannel channel = new EmbeddedChannel(
                new HttpGremlinEndpointHandler(serializers, null, null, settingsWith(authorizer)));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(BAD_REQUEST, response.status());
            assertFalse(authorizer.bytecodeAuthorizeCalled);
            assertFalse(authorizer.scriptAuthorizeCalled);
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }

    /**
     * An authorizer that fails with an error other than {@link AuthorizationException} gets a {@code 500} response
     * reporting that the authorizer is not ready, rather than the request being evaluated.
     */
    @Test
    public void shouldRespondWithServerErrorWhenAuthorizerFails() throws Exception {
        final Authorizer failing = new Authorizer() {
            @Override
            public void setup(final Map<String, Object> config) {
            }

            @Override
            public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode,
                                      final Map<String, String> aliases) {
                throw new IllegalStateException("not ready");
            }

            @Override
            public void authorize(final AuthenticatedUser user, final RequestMessage msg) {
                throw new IllegalStateException("not ready");
            }
        };
        final Map<String, MessageSerializer<?>> serializers = new HashMap<>();
        serializers.put(SerTokens.MIME_GRAPHBINARY_V1, new GraphBinaryMessageSerializerV1());
        final FullHttpRequest request = binaryRequest(RequestMessage.build(Tokens.OPS_EVAL)
                .addArg(Tokens.ARGS_GREMLIN, "g.V()").create());

        final EmbeddedChannel channel = new EmbeddedChannel(
                new HttpGremlinEndpointHandler(serializers, null, null, settingsWith(failing)));
        FullHttpResponse response = null;
        try {
            channel.writeInbound(request);

            assertEquals(0, request.refCnt());
            response = channel.readOutbound();
            assertNotNull(response);
            assertEquals(INTERNAL_SERVER_ERROR, response.status());
            assertTrue(response.content().toString(StandardCharsets.UTF_8).contains("is not ready to handle requests"));
        } finally {
            ReferenceCountUtil.release(response);
            channel.finishAndReleaseAll();
        }
    }

    private static Settings settingsWith(final Authorizer authorizer) {
        final Settings settings = new Settings();
        settings.authorization.overrideAuthorizer(authorizer);
        return settings;
    }

    private static FullHttpRequest binaryRequest(final RequestMessage msg) throws Exception {
        final ByteBuf body = new GraphBinaryMessageSerializerV1().serializeRequestAsBinary(msg, ByteBufAllocator.DEFAULT);
        final DefaultHttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONTENT_TYPE, SerTokens.MIME_GRAPHBINARY_V1);
        return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/", body, headers, new DefaultHttpHeaders());
    }

    /**
     * Serializes {@code msg} with GraphSON v3 and returns just the JSON payload (stripping the MIME header that
     * {@code serializeRequestAsBinary} prepends), so a test can build a body with extra top-level keys.
     */
    private static String graphsonRequestJson(final RequestMessage msg) throws Exception {
        final ByteBuf bb = new GraphSONMessageSerializerV3().serializeRequestAsBinary(msg, ByteBufAllocator.DEFAULT);
        try {
            final byte[] all = new byte[bb.readableBytes()];
            bb.getBytes(bb.readerIndex(), all);
            final String s = new String(all, StandardCharsets.UTF_8);
            return s.substring(s.indexOf('{'));
        } finally {
            ReferenceCountUtil.release(bb);
        }
    }

    private static class RecordingAuthorizer implements Authorizer {
        private final boolean deny;
        private volatile String lastScript;
        private volatile boolean bytecodeAuthorizeCalled;
        private volatile boolean scriptAuthorizeCalled;

        private RecordingAuthorizer(final boolean deny) {
            this.deny = deny;
        }

        @Override
        public void setup(final Map<String, Object> config) {
        }

        @Override
        public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode,
                                  final Map<String, String> aliases) throws AuthorizationException {
            bytecodeAuthorizeCalled = true;
            if (deny) throw new AuthorizationException("denied");
            return bytecode;
        }

        @Override
        public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
            scriptAuthorizeCalled = true;
            lastScript = String.valueOf(msg.getArgOrDefault(Tokens.ARGS_GREMLIN, ""));
            if (deny) throw new AuthorizationException("denied");
        }
    }
}
