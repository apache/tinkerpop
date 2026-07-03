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

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.util.ReferenceCountUtil;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * A self-contained Netty HTTP proxy used for cross-GLV proxy testing. It records the
 * {@code host:port} target of every request it proxies and exposes a small control API so tests can
 * prove that traffic actually transited the proxy.
 * <p>
 * Three kinds of requests are supported:
 * <ul>
 *   <li><b>HTTP CONNECT</b> tunneling (used by Java/Netty and JS/undici clients): the target is
 *   recorded, a 200 "Connection Established" is returned and the pipeline is reconfigured to relay
 *   raw bytes bidirectionally between the client and a freshly opened upstream connection.</li>
 *   <li><b>Absolute-URI forward</b> requests (used by Go/Python/.NET for http targets): the target
 *   authority is recorded and the request is forwarded to that target over a new upstream HTTP
 *   connection with the response relayed back to the client.</li>
 *   <li><b>Origin-form control</b> requests: {@code GET /__recorded} returns a JSON array of the
 *   recorded targets, {@code POST /__reset} clears the recorded list. Any other control path
 *   returns 404.</li>
 * </ul>
 */
public class RecordingProxyServer {

    private final int port;
    private final List<String> recordedTargets = new CopyOnWriteArrayList<>();
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public RecordingProxyServer(final int port) {
        this.port = port;
    }

    public Channel start() throws InterruptedException {
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        final ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(final SocketChannel ch) {
                        ch.pipeline().addLast("codec", new HttpServerCodec());
                        ch.pipeline().addLast("aggregator", new HttpObjectAggregator(65536));
                        ch.pipeline().addLast("handler", new ProxyFrontendHandler(recordedTargets));
                    }
                });
        return b.bind(port).sync().channel();
    }

    public void stop() {
        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    /**
     * Handles the client-facing side of the proxy: CONNECT tunneling, absolute-URI forwarding and
     * the origin-form control API.
     */
    private static class ProxyFrontendHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final List<String> recordedTargets;

        ProxyFrontendHandler(final List<String> recordedTargets) {
            this.recordedTargets = recordedTargets;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpRequest request) {
            final String uri = request.uri();
            if (HttpMethod.CONNECT.equals(request.method())) {
                handleConnect(ctx, uri);
            } else if (uri.startsWith("/")) {
                // origin-form -> control API
                handleControl(ctx, request);
            } else {
                // absolute-form -> forward to the target origin
                handleForward(ctx, request);
            }
        }

        // (a) CONNECT tunneling: record host:port, reply 200, then relay raw bytes.
        private void handleConnect(final ChannelHandlerContext ctx, final String target) {
            recordedTargets.add(target);

            final int colon = target.lastIndexOf(':');
            final String host = target.substring(0, colon);
            final int targetPort = Integer.parseInt(target.substring(colon + 1));

            final Channel clientChannel = ctx.channel();
            // Pause reads on the client until the tunnel is fully wired up.
            clientChannel.config().setAutoRead(false);

            final Bootstrap b = new Bootstrap();
            b.group(clientChannel.eventLoop())
                    .channel(NioSocketChannel.class)
                    .option(ChannelOption.AUTO_READ, false)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(final Channel ch) {
                            ch.pipeline().addLast(new RelayHandler(clientChannel));
                        }
                    });

            final ChannelFuture connectFuture = b.connect(host, targetPort);
            final Channel upstreamChannel = connectFuture.channel();
            connectFuture.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    sendControlResponse(clientChannel, HttpResponseStatus.BAD_GATEWAY, "", "text/plain");
                    closeOnFlush(clientChannel);
                    return;
                }
                final FullHttpResponse established = new DefaultFullHttpResponse(
                        HTTP_1_1, new HttpResponseStatus(200, "Connection Established"));
                established.headers().set(HttpHeaderNames.CONTENT_LENGTH, 0);
                clientChannel.writeAndFlush(established).addListener((ChannelFutureListener) writeFuture -> {
                    if (!writeFuture.isSuccess()) {
                        closeOnFlush(upstreamChannel);
                        closeOnFlush(clientChannel);
                        return;
                    }
                    // Switch to a raw byte relay. Add the relay before removing the HTTP handlers
                    // (codec last) so any bytes buffered by the codec flow into the relay.
                    clientChannel.pipeline().addLast(new RelayHandler(upstreamChannel));
                    clientChannel.pipeline().remove("handler");
                    clientChannel.pipeline().remove("aggregator");
                    clientChannel.pipeline().remove("codec");
                    clientChannel.config().setAutoRead(true);
                    upstreamChannel.config().setAutoRead(true);
                    clientChannel.read();
                    upstreamChannel.read();
                });
            });
        }

        // (b) Absolute-URI forward: record host:port, forward request, relay response back.
        private void handleForward(final ChannelHandlerContext ctx, final FullHttpRequest request) {
            final URI parsed;
            try {
                parsed = new URI(request.uri());
            } catch (Exception e) {
                sendControlResponse(ctx.channel(), HttpResponseStatus.BAD_REQUEST, "", "text/plain");
                return;
            }

            final String host = parsed.getHost();
            final int targetPort = parsed.getPort();

            recordedTargets.add(host + ":" + targetPort);

            // Build an origin-form request for the upstream. Copy synchronously since the inbound
            // request is released once channelRead0 returns.
            String pathAndQuery = parsed.getRawPath();
            if (pathAndQuery == null || pathAndQuery.isEmpty()) {
                pathAndQuery = "/";
            }
            if (parsed.getRawQuery() != null) {
                pathAndQuery = pathAndQuery + "?" + parsed.getRawQuery();
            }

            final FullHttpRequest forwardRequest = request.copy();
            forwardRequest.setUri(pathAndQuery);
            forwardRequest.headers().set(HttpHeaderNames.HOST, host + ":" + targetPort);

            final Channel clientChannel = ctx.channel();
            final String upstreamHost = host;
            final int upstreamPort = targetPort;

            final Bootstrap b = new Bootstrap();
            b.group(clientChannel.eventLoop())
                    .channel(NioSocketChannel.class)
                    .handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(final Channel ch) {
                            ch.pipeline().addLast(new HttpClientCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(65536));
                            ch.pipeline().addLast(new UpstreamResponseHandler(clientChannel));
                        }
                    });

            b.connect(upstreamHost, upstreamPort).addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess()) {
                    forwardRequest.release();
                    sendControlResponse(clientChannel, HttpResponseStatus.BAD_GATEWAY, "", "text/plain");
                    return;
                }
                future.channel().writeAndFlush(forwardRequest);
            });
        }

        // (c) Origin-form control API.
        private void handleControl(final ChannelHandlerContext ctx, final FullHttpRequest request) {
            final String uri = request.uri();
            if (HttpMethod.GET.equals(request.method()) && "/__recorded".equals(uri)) {
                sendControlResponse(ctx.channel(), HttpResponseStatus.OK, toJsonArray(recordedTargets),
                        "application/json");
            } else if (HttpMethod.POST.equals(request.method()) && "/__reset".equals(uri)) {
                recordedTargets.clear();
                sendControlResponse(ctx.channel(), HttpResponseStatus.OK, "", "text/plain");
            } else {
                sendControlResponse(ctx.channel(), HttpResponseStatus.NOT_FOUND, "", "text/plain");
            }
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            closeOnFlush(ctx.channel());
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

    /**
     * Writes a simple full HTTP response with the given body and content type.
     */
    private static void sendControlResponse(final Channel channel, final HttpResponseStatus status,
                                            final String body, final String contentType) {
        final ByteBuf content = Unpooled.copiedBuffer(body, StandardCharsets.UTF_8);
        final FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, content);
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType + "; charset=UTF-8");
        HttpUtil.setContentLength(response, content.readableBytes());
        channel.writeAndFlush(response);
    }

    private static void closeOnFlush(final Channel channel) {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }

    /**
     * Relays raw bytes to a peer channel and mirrors close/error events so both ends of a tunnel are
     * torn down together.
     */
    private static class RelayHandler extends ChannelInboundHandlerAdapter {

        private final Channel relayChannel;

        RelayHandler(final Channel relayChannel) {
            this.relayChannel = relayChannel;
        }

        @Override
        public void channelRead(final ChannelHandlerContext ctx, final Object msg) {
            if (relayChannel.isActive()) {
                relayChannel.writeAndFlush(msg).addListener((ChannelFutureListener) future -> {
                    if (future.isSuccess()) {
                        ctx.channel().read();
                    } else {
                        future.channel().close();
                    }
                });
            } else {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void channelInactive(final ChannelHandlerContext ctx) {
            closeOnFlush(relayChannel);
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            closeOnFlush(ctx.channel());
        }
    }

    /**
     * Relays a forwarded HTTP response from the upstream origin back to the client, then closes the
     * upstream connection.
     */
    private static class UpstreamResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        private final Channel clientChannel;

        UpstreamResponseHandler(final Channel clientChannel) {
            this.clientChannel = clientChannel;
        }

        @Override
        protected void channelRead0(final ChannelHandlerContext ctx, final FullHttpResponse response) {
            if (clientChannel.isActive()) {
                clientChannel.writeAndFlush(response.retain());
            }
            ctx.close();
        }

        @Override
        public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
            ctx.close();
            closeOnFlush(clientChannel);
        }
    }
}
