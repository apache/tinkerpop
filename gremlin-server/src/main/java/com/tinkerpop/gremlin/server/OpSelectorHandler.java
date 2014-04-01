package com.tinkerpop.gremlin.server;

import com.codahale.metrics.Meter;
import com.tinkerpop.gremlin.server.op.OpLoader;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpSelectorHandler extends MessageToMessageDecoder<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinOpHandler.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    private final Settings settings;
    private final Graphs graphs;

    private final GremlinExecutor gremlinExecutor;

    public OpSelectorHandler(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final RequestMessage msg, final List<Object> objects) throws Exception {
        try {
            // choose a processor to do the work based on the request message.
            final Optional<OpProcessor> processor = OpLoader.getProcessor(msg.processor);
            final Context gremlinServerContext = new Context(msg, channelHandlerContext, settings, graphs, gremlinExecutor);

            if (processor.isPresent())
                // the processor is known so use it to evaluate the message
                objects.add(Pair.with(msg, processor.get().select(gremlinServerContext)));
            else {
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
            channelHandlerContext.writeAndFlush(ope.getFrame());
        }
    }
}
