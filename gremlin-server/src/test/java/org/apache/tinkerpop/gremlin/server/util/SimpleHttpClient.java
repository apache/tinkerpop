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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.http.Consts;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.tinkerpop.gremlin.driver.simple.SimpleClient;
import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphSONUntypedMessageSerializerV4;
import org.apache.tinkerpop.gremlin.util.ser.HeapBufferFactory;
import org.apache.tinkerpop.gremlin.util.ser.SerializationException;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * A simple HTTP-based implementation of {@link SimpleClient} for testing purposes.
 * Uses Apache HttpClient 4.x to send requests and the GraphSON serializer for message encoding/decoding.
 */
public class SimpleHttpClient implements SimpleClient {

    private static final HeapBufferFactory bufferFactory = new HeapBufferFactory();

    private final URI uri;
    private final CloseableHttpClient httpClient;
    private final GraphSONUntypedMessageSerializerV4 serializer = new GraphSONUntypedMessageSerializerV4();

    public SimpleHttpClient(final URI uri) {
        this.uri = uri;
        if ("https".equals(uri.getScheme())) {
            this.httpClient = createSslClient();
        } else {
            this.httpClient = HttpClients.createDefault();
        }
    }

    private static CloseableHttpClient createSslClient() {
        try {
            final SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, new TrustManager[]{new X509TrustManager() {
                public X509Certificate[] getAcceptedIssuers() { return null; }
                public void checkClientTrusted(X509Certificate[] certs, String authType) {}
                public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }}, null);
            final SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);
            return HttpClients.custom().setSSLSocketFactory(sslsf).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception {
        final List<ResponseMessage> responses = submit(requestMessage);
        responses.forEach(callback);
    }

    @Override
    public List<ResponseMessage> submit(final RequestMessage requestMessage) throws Exception {
        final Buffer buffer = serializer.serializeRequestAsBinary(requestMessage);
        final byte[] bytes = new byte[buffer.readableBytes()];
        buffer.readBytes(bytes);
        buffer.release();

        final HttpPost httpPost = new HttpPost(uri);
        httpPost.addHeader("Content-Type", "application/json");
        httpPost.addHeader("Accept", "application/json");
        httpPost.setEntity(new StringEntity(new String(bytes, Consts.UTF_8), Consts.UTF_8));

        try (final CloseableHttpResponse response = httpClient.execute(httpPost)) {
            final String responseBody = EntityUtils.toString(response.getEntity(), Consts.UTF_8);
            final ResponseMessage responseMessage = deserializeResponse(responseBody);
            return Collections.singletonList(responseMessage);
        }
    }

    @Override
    public CompletableFuture<List<ResponseMessage>> submitAsync(final RequestMessage requestMessage) throws Exception {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return submit(requestMessage);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private ResponseMessage deserializeResponse(final String body) throws SerializationException {
        final byte[] bytes = body.getBytes(Consts.UTF_8);
        final Buffer buffer = bufferFactory.create(bytes.length);
        buffer.writeBytes(bytes);
        return serializer.deserializeBinaryResponse(buffer);
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
