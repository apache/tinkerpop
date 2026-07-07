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

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.littleshoot.proxy.HttpFiltersAdapter;
import org.littleshoot.proxy.HttpFiltersSourceAdapter;
import org.littleshoot.proxy.HttpProxyServer;
import org.littleshoot.proxy.impl.DefaultHttpProxyServer;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * A proxy used for cross-GLV proxy testing, backed by the LittleProxy library. It records the
 * {@code host:port} target of every request it proxies and exposes a small control API so tests can
 * prove that traffic actually transited the proxy.
 * <p>
 * Two categories of requests are handled by the installed {@code HttpFilters}:
 * <ul>
 *   <li><b>Proxied</b> requests (HTTP CONNECT tunnels used by Java/Netty and JS/undici clients, plus
 *   absolute-URI forward requests used by Go/Python/.NET for http targets): the target authority is
 *   recorded and LittleProxy is allowed to proxy the request normally.</li>
 *   <li><b>Origin-form control</b> requests served on the same proxy port: {@code GET /__recorded}
 *   returns a JSON array of the recorded targets, {@code POST /__reset} clears the recorded list. Any
 *   other control path returns 404. These are short-circuited by returning a response directly from
 *   the filter so they never leave the proxy.</li>
 * </ul>
 */
public class RecordingProxyServer {

    private final int port;
    private final List<String> recordedTargets = new CopyOnWriteArrayList<>();
    private HttpProxyServer proxyServer;

    public RecordingProxyServer(final int port) {
        this.port = port;
    }

    public void start() {
        proxyServer = DefaultHttpProxyServer.bootstrap()
                .withPort(port)
                .withFiltersSource(new HttpFiltersSourceAdapter() {
                    @Override
                    public org.littleshoot.proxy.HttpFilters filterRequest(final HttpRequest originalRequest) {
                        return new RecordingFilters(originalRequest, recordedTargets);
                    }
                })
                .start();
    }

    public void stop() {
        if (proxyServer != null) {
            proxyServer.stop();
        }
    }

    /**
     * Records proxied targets and short-circuits the origin-form control API.
     */
    private static class RecordingFilters extends HttpFiltersAdapter {

        private final List<String> recordedTargets;

        RecordingFilters(final HttpRequest originalRequest, final List<String> recordedTargets) {
            super(originalRequest);
            this.recordedTargets = recordedTargets;
        }

        @Override
        public io.netty.handler.codec.http.HttpResponse clientToProxyRequest(final HttpObject httpObject) {
            if (!(httpObject instanceof HttpRequest)) {
                return null;
            }

            final HttpRequest request = (HttpRequest) httpObject;
            final String uri = request.uri();

            if (uri.startsWith("/")) {
                // origin-form -> control API, short-circuit with a direct response.
                return handleControl(request);
            }

            // otherwise it is a proxied request: record the authority and let LittleProxy proxy it.
            final String authority = extractAuthority(request);
            if (authority != null) {
                recordedTargets.add(authority);
            }
            return null;
        }

        /**
         * Extracts the {@code host:port} authority from a proxied request. For CONNECT the uri is
         * already {@code host:port}; for an absolute-URI request the uri is parsed with
         * {@link URI}, falling back to the Host header when the uri cannot be parsed.
         */
        private String extractAuthority(final HttpRequest request) {
            final String uri = request.uri();
            if (HttpMethod.CONNECT.equals(request.method())) {
                return uri;
            }

            try {
                final URI parsed = new URI(uri);
                final String host = parsed.getHost();
                if (host != null) {
                    final int parsedPort = parsed.getPort();
                    return host + ":" + (parsedPort == -1 ? 80 : parsedPort);
                }
            } catch (Exception ignored) {
                // fall through to the Host header fallback below
            }

            final String hostHeader = request.headers().get(HttpHeaderNames.HOST);
            if (hostHeader != null && !hostHeader.isEmpty()) {
                return hostHeader.contains(":") ? hostHeader : hostHeader + ":80";
            }
            return null;
        }

        private FullHttpResponse handleControl(final HttpRequest request) {
            final String uri = request.uri();
            if (HttpMethod.GET.equals(request.method()) && "/__recorded".equals(uri)) {
                return buildResponse(HttpResponseStatus.OK, toJsonArray(recordedTargets), "application/json");
            } else if (HttpMethod.POST.equals(request.method()) && "/__reset".equals(uri)) {
                recordedTargets.clear();
                return buildResponse(HttpResponseStatus.OK, "", "text/plain");
            } else {
                return buildResponse(HttpResponseStatus.NOT_FOUND, "", "text/plain");
            }
        }

        private static FullHttpResponse buildResponse(final HttpResponseStatus status, final String body,
                                                      final String contentType) {
            final FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, status, Unpooled.copiedBuffer(body, StandardCharsets.UTF_8));
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType + "; charset=UTF-8");
            response.headers().set(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
            return response;
        }

        /**
         * Builds a JSON array of strings by hand to avoid pulling in a JSON dependency.
         */
        private static String toJsonArray(final List<String> targets) {
            final StringBuilder sb = new StringBuilder("[");
            final List<String> snapshot = new ArrayList<>(targets);
            for (int i = 0; i < snapshot.size(); i++) {
                if (i > 0) {
                    sb.append(",");
                }
                sb.append("\"").append(escapeJson(snapshot.get(i))).append("\"");
            }
            sb.append("]");
            return sb.toString();
        }

        private static String escapeJson(final String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
