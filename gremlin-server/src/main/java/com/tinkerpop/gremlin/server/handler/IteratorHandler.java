package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.server.ResultCode;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.server.message.RequestMessage;
import com.tinkerpop.gremlin.server.message.ResponseMessage;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import org.apache.commons.lang.time.StopWatch;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorHandler extends ChannelOutboundHandlerAdapter  {
    private static final Logger logger = LoggerFactory.getLogger(IteratorHandler.class);

    private final Settings settings;

    public IteratorHandler(final Settings settings) {
        this.settings = settings;
    }

    @Override
    public void write(final ChannelHandlerContext ctx, final Object msg, final ChannelPromise promise) throws Exception {
        if (msg instanceof Pair) {
            try {
                final Pair pair = (Pair) msg;
                final Iterator itty = (Iterator) pair.getValue1();
                final RequestMessage requestMessage = (RequestMessage) pair.getValue0();

                // timer for the total serialization time
                final StopWatch stopWatch = new StopWatch();

                final EventExecutorGroup executorService = ctx.channel().eventLoop().next();
                final Future<?> iteration = executorService.submit((Callable<Void>) () -> {

                    stopWatch.start();

                    int counter = 0;
                    while (itty.hasNext()) {
                        ctx.write(ResponseMessage.create(requestMessage).code(ResultCode.SUCCESS).result(itty.next()).build());

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
                        ctx.writeAndFlush(ResponseMessage.create(requestMessage).code(ResultCode.SERVER_ERROR_TIMEOUT).result(errorMessage).build());
                    }

                    ctx.writeAndFlush(ResponseMessage.create(requestMessage).code(ResultCode.SUCCESS_TERMINATOR).result(Optional.empty()).build());
                });
            } finally {
                ReferenceCountUtil.release(msg);
            }

        } else {
            ctx.write(msg, promise);
        }
    }
}
