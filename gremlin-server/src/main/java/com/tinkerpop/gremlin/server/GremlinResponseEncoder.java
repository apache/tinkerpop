package com.tinkerpop.gremlin.server;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GremlinResponseEncoder extends MessageToMessageEncoder<Pair<RequestMessage, Object>> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinResponseEncoder.class);

    private final Settings settings;
    private final Graphs graphs;

    private final GremlinExecutor gremlinExecutor;

    public GremlinResponseEncoder(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }

    @Override
    protected void encode(ChannelHandlerContext channelHandlerContext, Pair<RequestMessage, Object> o, List<Object> objects) throws Exception {
        final Context gremlinServerContext = new Context(o.getValue0(), channelHandlerContext, settings, graphs, gremlinExecutor);
        final MessageSerializer serializer = MessageSerializer.select(
                o.getValue0().<String>optionalArgs(Tokens.ARGS_ACCEPT).orElse("text/plain"),
                MessageSerializer.DEFAULT_RESULT_SERIALIZER);

            try {
                objects.add(new TextWebSocketFrame(true, 0, serializer.serializeResult(o.getValue1(), gremlinServerContext)));
            } catch (Exception ex) {
                // sending the requestId acts as a termination message for this request.
                final ByteBuf uuidBytes = Unpooled.directBuffer(16);
                uuidBytes.writeLong(o.getValue0().requestId.getMostSignificantBits());
                uuidBytes.writeLong(o.getValue0().requestId.getLeastSignificantBits());
                final BinaryWebSocketFrame terminator = new BinaryWebSocketFrame(uuidBytes);

                logger.warn("The result [{}] in the request {} could not be serialized and returned.", o.getValue1(), o.getValue0(), ex);
                final String errorMessage = String.format("Error during serialization: %s",
                        ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
                channelHandlerContext.write(new TextWebSocketFrame(serializer.serializeResult(errorMessage, ResultCode.SERVER_ERROR_SERIALIZATION, gremlinServerContext)));
                channelHandlerContext.writeAndFlush(terminator);
            }
    }
}
