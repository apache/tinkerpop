package com.tinkerpop.gremlin.server;

import com.codahale.metrics.Meter;
import com.tinkerpop.gremlin.server.op.OpLoader;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Adapted from https://github.com/netty/netty/tree/netty-4.0.10.Final/example/src/main/java/io/netty/example/http/websocketx/server
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class GremlinOpHandler extends SimpleChannelInboundHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinOpHandler.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    private final Settings settings;
    private final Graphs graphs;

    private final GremlinExecutor gremlinExecutor;

    public GremlinOpHandler(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final RequestMessage msg) throws Exception {
        try {
            // choose a processor to do the work based on the request message.
            final Optional<OpProcessor> processor = OpLoader.getProcessor(msg.processor);
            final Context gremlinServerContext = new Context(msg, ctx, settings, graphs, gremlinExecutor);

            if (processor.isPresent()) {
                // the processor is known so use it to evaluate the message
                processor.get().select(gremlinServerContext).accept(gremlinServerContext);
            } else {
                // invalid op processor selected so write back an error by way of OpProcessorException.
                final String errorMessage = String.format("Invalid OpProcessor requested [%s]", msg.processor);
                final MessageSerializer serializer = MessageSerializer.select(
                        msg.<String>optionalArgs(Tokens.ARGS_ACCEPT).orElse("text/plain"),
                        MessageSerializer.DEFAULT_RESULT_SERIALIZER);
                throw new OpProcessorException(errorMessage, serializer.serializeResult(msg, ResultCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS, gremlinServerContext));
            }
        } catch (OpProcessorException ope) {
            errorMeter.mark();
            logger.warn(ope.getMessage(), ope);
            ctx.channel().write(ope.getFrame()); // todo: just do ctx.write() ???
        } finally {
            // sending the requestId acts as a termination message for this request.
            final ByteBuf uuidBytes = Unpooled.directBuffer(16);
            uuidBytes.writeLong(msg.requestId.getMostSignificantBits());
            uuidBytes.writeLong(msg.requestId.getLeastSignificantBits());
            ctx.channel().write(new BinaryWebSocketFrame(uuidBytes));     // todo: just do ctx.write() ???
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) throws Exception {
        // this only happens if an exception fires that isn't handled.  A good example would be if a frame
        // was sent that was not covered.  bad stuff if we get here.
        logger.error("Message handler caught an exception fatal to this request. Closing connection.", cause);
        errorMeter.mark();
        ctx.close();
    }
}