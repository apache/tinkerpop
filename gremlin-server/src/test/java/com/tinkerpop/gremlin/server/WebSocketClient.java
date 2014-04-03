package com.tinkerpop.gremlin.server;

import com.fasterxml.jackson.databind.JsonNode;
import com.tinkerpop.gremlin.server.message.RequestMessage;
import com.tinkerpop.gremlin.server.util.ser.JsonMessageSerializerV1d0;
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
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientHandshakerFactory;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Optional;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A temporary, basic and probably slightly flawed client from Gremlin Server. Not meant for use outside of testing.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class WebSocketClient {

    private final URI uri;
    private Channel ch;
    private static final EventLoopGroup group = new NioEventLoopGroup();

    protected static ConcurrentHashMap<UUID, ArrayBlockingQueue<Optional<JsonNode>>> responses = new ConcurrentHashMap<>();

    private static MessageSerializer serializer = new JsonMessageSerializerV1d0();

    public WebSocketClient(final String uri) {
        this.uri = URI.create(uri);
    }

    void putResponse(final UUID requestId, final Optional<JsonNode> response) {
        if (!responses.containsKey(requestId)) {
            // probably a timeout if we get here... ???
            System.out.println(String.format("No queue found in the response map: %s", requestId));
            return;
        }

        try {
            final ArrayBlockingQueue<Optional<JsonNode>> queue = responses.get(requestId);
            if (queue != null) {
                queue.put(response);
            } else {
                // no queue for some reason....why ???
                System.out.println(String.format("No queue found in the response map*: %s", requestId));
            }
        } catch (InterruptedException e) {
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
                                uri, WebSocketVersion.V13, null, false, HttpHeaders.EMPTY_HEADERS, 1280000), this);

        b.group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast("http-codec", new HttpClientCodec());
                        pipeline.addLast("aggregator", new HttpObjectAggregator(65536));
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

    public <T> Stream<T> eval(final String gremlin) throws IOException {
        final RequestMessage msg = new RequestMessage.Builder(Tokens.OPS_EVAL).build();
        msg.args = new HashMap<String, Object>() {{
            put(Tokens.ARGS_GREMLIN, gremlin);
            put(Tokens.ARGS_ACCEPT, "application/json");
        }};

        final ArrayBlockingQueue<Optional<JsonNode>> responseQueue = new ArrayBlockingQueue<>(256);
        final UUID requestId = msg.requestId;
        responses.put(requestId, responseQueue);

        ch.writeAndFlush(new TextWebSocketFrame("application/json|-" + serializer.serialize(msg)));

        return StreamSupport.stream(Spliterators.<T>spliteratorUnknownSize(new BlockingIterator<>(requestId), Spliterator.IMMUTABLE), false);
    }

    /**
     * The BlockingIterator iterates over the queue of results returned from a request.  It will wait for a result
     * to appear in the queue and block on hasNext() until it does.  It will watch for a termination event to
     * learn that the results have finished streaming.
     */
    class BlockingIterator<T> implements Iterator<T> {
        private final ArrayBlockingQueue<Optional<JsonNode>> queue;
        private final UUID requestId;
        private T current;

        public BlockingIterator(final UUID requestId) {
            this.requestId = requestId;
            this.queue = responses.get(requestId);
        }

        @Override
        public boolean hasNext() {
            try {
                final Optional<JsonNode> node = queue.poll(30000, TimeUnit.MILLISECONDS);
                if (node == null) {
                    System.out.println("time elapsed before a result was ready");
                }

                if (!node.isPresent()) {
                    responses.remove(requestId);
                    return false;
                }

                // todo: better job with types
                this.current = (T) node.get().get("result").asText();
                return true;
            } catch (InterruptedException ie) {
                //ie.printStackTrace();
                System.out.println("hmmm...interrupted while waiting");

                // todo: this isn't right...at least not exactly.  how do we terate out of a timeout.
                return false;
            }
        }

        @Override
        public T next() {
            return current;
        }
    }
}