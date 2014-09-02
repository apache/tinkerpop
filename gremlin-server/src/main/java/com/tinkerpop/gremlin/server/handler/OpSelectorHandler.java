package com.tinkerpop.gremlin.server.handler;

import com.codahale.metrics.Meter;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.server.op.OpLoader;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpSelectorHandler extends MessageToMessageDecoder<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(OpSelectorHandler.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    private final Settings settings;
    private final Graphs graphs;

    private final GremlinExecutor gremlinExecutor;
    private final ScheduledExecutorService scheduledExecutorService;

    public OpSelectorHandler(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor,
                             final ScheduledExecutorService scheduledExecutorService) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    protected void decode(final ChannelHandlerContext channelHandlerContext, final RequestMessage msg,
                          final List<Object> objects) throws Exception {
        final Context gremlinServerContext = new Context(msg, channelHandlerContext, settings,
                graphs, gremlinExecutor, this.scheduledExecutorService);
        try {
            // choose a processor to do the work based on the request message.
            final Optional<OpProcessor> processor = OpLoader.getProcessor(msg.getProcessor());

            if (processor.isPresent())
                // the processor is known so use it to evaluate the message
                objects.add(Pair.with(msg, processor.get().select(gremlinServerContext)));
            else {
                // invalid op processor selected so write back an error by way of OpProcessorException.
                final String errorMessage = String.format("Invalid OpProcessor requested [%s]", msg.getProcessor());
                throw new OpProcessorException(errorMessage, ResponseMessage.build(msg).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(errorMessage).create());
            }
        } catch (OpProcessorException ope) {
            errorMeter.mark();
            logger.warn(ope.getMessage(), ope);
            channelHandlerContext.writeAndFlush(ope.getResponseMessage());
        }
    }
}
