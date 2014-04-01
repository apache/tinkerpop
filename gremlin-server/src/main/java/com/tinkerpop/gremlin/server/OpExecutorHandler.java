package com.tinkerpop.gremlin.server;

import com.codahale.metrics.Meter;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import com.tinkerpop.gremlin.util.function.ThrowingConsumer;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class OpExecutorHandler extends SimpleChannelInboundHandler<Pair<RequestMessage, ThrowingConsumer<Context>>> {
    private static final Logger logger = LoggerFactory.getLogger(GremlinOpHandler.class);
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    private final Settings settings;
    private final Graphs graphs;

    private final GremlinExecutor gremlinExecutor;

    public OpExecutorHandler(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }

    @Override
    protected void channelRead0(final ChannelHandlerContext channelHandlerContext, final Pair<RequestMessage, ThrowingConsumer<Context>> objects) throws Exception {
        final RequestMessage msg = objects.getValue0();
        final ThrowingConsumer<Context> op = objects.getValue1();
        final Context gremlinServerContext = new Context(msg, channelHandlerContext, settings, graphs, gremlinExecutor);

        try {
            op.accept(gremlinServerContext);
        } catch (OpProcessorException ope) {
            errorMeter.mark();
            logger.warn(ope.getMessage(), ope);
            channelHandlerContext.write(ope.getFrame());
        }
    }

    @Override
    public void channelReadComplete(final ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }
}
