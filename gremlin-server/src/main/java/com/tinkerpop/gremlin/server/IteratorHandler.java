package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.op.OpProcessorException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.apache.commons.lang.time.StopWatch;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorHandler extends ChannelOutboundHandlerAdapter  {
    private static final Logger logger = LoggerFactory.getLogger(IteratorHandler.class);

    private final Settings settings;
    private final Graphs graphs;

    private final GremlinExecutor gremlinExecutor;

    public IteratorHandler(final Settings settings, final Graphs graphs, final GremlinExecutor gremlinExecutor) {
        this.settings = settings;
        this.graphs = graphs;
        this.gremlinExecutor = gremlinExecutor;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
        if (msg instanceof Pair) {
            final Pair pair = (Pair) msg;
            final Iterator itty = (Iterator) pair.getValue1();
            final RequestMessage requestMessage = (RequestMessage) pair.getValue0();

            final Context context = new Context(requestMessage, ctx, settings, graphs, gremlinExecutor);
            final MessageSerializer serializer = MessageSerializer.select(
                    requestMessage.<String>optionalArgs(Tokens.ARGS_ACCEPT).orElse("text/plain"),
                    MessageSerializer.DEFAULT_RESULT_SERIALIZER);

            // timer for the total serialization time
            final StopWatch stopWatch = new StopWatch();

            final EventExecutorGroup executorService = ctx.channel().eventLoop().next();
            final Future<?> iteration = executorService.submit((Callable<Void>) () -> {

                stopWatch.start();

                int counter = 0;
                while (itty.hasNext()) {
                    ctx.write(Pair.with(requestMessage, itty.next()));

                    // iteration listener will call the final flush for any leftovers on completion.
                    counter++;
                    if (counter % settings.resultIterationBatchSize == 0)
                        ctx.flush();

                    stopWatch.split();
                    if (stopWatch.getSplitTime() > settings.serializedResponseTimeout)
                        throw new TimeoutException("Serialization of the entire response exceeded the serializeResponseTimeout setting");

                    stopWatch.unsplit();
                }

                return null;
            });

            iteration.addListener(f->{
                stopWatch.stop();

                if (!f.isSuccess()) {
                    final String errorMessage = String.format("Response iteration and serialization exceeded the configured threshold for request [%s] - %s", msg, f.cause().getMessage());
                    logger.warn(errorMessage);
                    ctx.write(new TextWebSocketFrame(serializer.serializeResult(errorMessage, ResultCode.SERVER_ERROR_TIMEOUT, context)));
                }

                ctx.writeAndFlush(new TextWebSocketFrame(serializer.serializeResult(requestMessage.requestId, ResultCode.SUCCESS_TERMINATOR, context)));
            });

        } else {
            ctx.write(msg, promise);
        }
    }
}
