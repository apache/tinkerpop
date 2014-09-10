package com.tinkerpop.gremlin.server.handler;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.server.Settings;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class IteratorHandler extends ChannelOutboundHandlerAdapter {
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

                // the batch size can be overriden by the request
                final int resultIterationBatchSize = (Integer) requestMessage.optionalArgs(Tokens.ARGS_BATCH_SIZE).orElse(settings.resultIterationBatchSize);

                // timer for the total serialization time
                final StopWatch stopWatch = new StopWatch();

                final EventExecutorGroup executorService = ctx.executor();
                final Future<?> iteration = executorService.submit((Callable<Void>) () -> {
                    logger.debug("Preparing to iterate results from - {} - in thread [{}]", requestMessage, Thread.currentThread().getName());

                    stopWatch.start();

                    List<Object> aggregate = new ArrayList<>(resultIterationBatchSize);
                    while (itty.hasNext()) {
                        aggregate.add(itty.next());

                        // send back a page of results if batch size is met or if it's the end of the results being
                        // iterated
                        if (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                            ctx.writeAndFlush(ResponseMessage.build(requestMessage)
                                    .code(ResponseStatusCode.SUCCESS)
                                    .result(aggregate).create());
                            aggregate = new ArrayList<>(resultIterationBatchSize);
                        }

                        stopWatch.split();
                        if (stopWatch.getSplitTime() > settings.serializedResponseTimeout)
                            throw new TimeoutException("Serialization of the entire response exceeded the serializeResponseTimeout setting");

                        stopWatch.unsplit();
                    }

                    return null;
                });

                iteration.addListener(f -> {
                    stopWatch.stop();

                    if (!f.isSuccess()) {
                        final String errorMessage = String.format("Response iteration and serialization exceeded the configured threshold for request [%s] - %s", msg, f.cause().getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(requestMessage).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                    }

                    ctx.writeAndFlush(ResponseMessage.build(requestMessage).code(ResponseStatusCode.SUCCESS_TERMINATOR).create());
                });
            } finally {
                ReferenceCountUtil.release(msg);
            }

        } else {
            ctx.write(msg, promise);
        }
    }
}
