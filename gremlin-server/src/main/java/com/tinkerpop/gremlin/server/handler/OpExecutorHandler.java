package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.Graphs;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.ReferenceCountUtil;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpExecutorHandler extends SimpleChannelInboundHandler<Pair<RequestMessage, ThrowingConsumer<Context>>> {
    private static final Logger logger = LoggerFactory.getLogger(OpExecutorHandler.class);

    private final Settings settings;
    private final Graphs graphs;
    private final ScheduledExecutorService scheduledExecutorService;
    private final GremlinExecutor gremlinExecutor;

    public OpExecutorHandler(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor,
                             final ScheduledExecutorService scheduledExecutorService) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final Pair<RequestMessage, ThrowingConsumer<Context>> objects) throws Exception {
        final RequestMessage msg = objects.getValue0();
        final ThrowingConsumer<Context> op = objects.getValue1();
        final Context gremlinServerContext = new Context(msg, channelHandlerContext,
                settings, graphs, gremlinExecutor, scheduledExecutorService);

        try {
            op.accept(gremlinServerContext);
        } catch (OpProcessorException ope) {
            // Ops may choose to throw OpProcessorException or write the error ResponseMessage down the line
            // themselves
            logger.warn(ope.getMessage(), ope);
            channelHandlerContext.writeAndFlush(ope.getResponseMessage());
        } finally {
            ReferenceCountUtil.release(objects);
        }
    }
}
