package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.databind.JsonNode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * A temporary, basic and probably slightly flawed client from Gremlin Server. Not meant for use outside of testing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class WebSocketClient {

    private final URI uri;
    private Channel ch;
    private static final EventLoopGroup group = new NioEventLoopGroup();

    protected static ConcurrentHashMap<UUID, ArrayBlockingQueue<JsonNode>> responses = new ConcurrentHashMap<>();

    public WebSocketClient(final String uri) {
        this.uri = URI.create(uri);
    }

    static void putResponse(final UUID requestId, final JsonNode response) {
        if (!responses.containsKey(requestId)) {
            // probably a timeout if we get here... ???
            System.out.println(String.format("No queue found in the response map: %s", requestId));
            return;
        }

        try {
            final ArrayBlockingQueue<JsonNode> queue = responses.get(requestId);
            if (queue != null) {
                queue.put(response);
            }
            else {
                // no queue for some reason....why ???
                System.out.println(String.format("No queue found in the response map: %s", requestId));
            }
        }
        catch (InterruptedException e) {
            // just trap this one ???
            System.out.println("Error reading the queue in the response map.");
            e.printStackTrace();
        }
    }

    public void open() throws Exception {
        Bootstrap b = new Bootstrap();
        String protocol = uri.getScheme();
        if (!"ws".equals(protocol)) {
            throw new IllegalArgumentException("Unsupported protocol: " + protocol);
        }

        // Connect with V13 (RFC 6455 aka HyBi-17). You can change it to V08 or V00.
        // If you change it to V00, ping is not supported and remember to change
        // HttpResponseDecoder to WebSocketHttpResponseDecoder in the pipeline.
        final WebSocketClientHandler handler =
                new WebSocketClientHandler(
                        WebSocketClientHandshakerFactory.newHandshaker(
                                uri, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS), this);

        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("http-codec", new HttpClientCodec());
                        pipeline.addLast("aggregator", new HttpObjectAggregator(8192));
                        pipeline.addLast("ws-handler", handler);
                    }
                });

        //System.out.println("WebSocket Client connecting");
        ch = b.connect(uri.getHost(), uri.getPort()).sync().channel();
        handler.handshakeFuture().sync();
    }

    public void close() throws InterruptedException {
        //System.out.println("WebSocket Client sending close");
        ch.writeAndFlush(new CloseWebSocketFrame());
        ch.closeFuture().sync();
        //group.shutdownGracefully();
    }

    public <T> T eval(final String gremlin) throws IOException {
        final RequestMessage msg = new RequestMessage(ServerTokens.OPS_EVAL);
        msg.requestId = UUID.randomUUID();
        msg.args = new HashMap<String, Object>() {{
            put(ServerTokens.ARGS_GREMLIN, gremlin);
            put(ServerTokens.ARGS_ACCEPT, "application/json");
        }};

        final ArrayBlockingQueue<JsonNode> responseQueue = new ArrayBlockingQueue<>(1);
        final UUID requestId = msg.requestId;
        responses.put(requestId, responseQueue);

        ch.writeAndFlush(new TextWebSocketFrame(RequestMessage.Serializer.json(msg)));

        JsonNode resultMessage;
        try {
            resultMessage = responseQueue.poll(8000, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
            responses.remove(requestId);
            throw new IOException(ex);
        }

        responses.remove(requestId);

        if (resultMessage == null)
            throw new IOException(String.format("Message received response timeoutConnection (%s s)", 1000));

        JsonNode o = resultMessage.get("result");
        String tv = o.toString();
        return (T) tv;

    }
}