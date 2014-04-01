package com.tinkerpop.gremlin.server;

import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.LocalExecutorService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.timeout.WriteTimeoutException;
import io.netty.util.concurrent.EventExecutorGroup;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;
import org.apache.commons.lang.time.StopWatch;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.Iterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

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
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
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

            //final ExecutorService executorService = LocalExecutorService.getLocal();
            final EventExecutorGroup executorService = ctx.channel().eventLoop().next();

            // sending the requestId acts as a termination message for this request.
            final ByteBuf uuidBytes = Unpooled.directBuffer(16);
            uuidBytes.writeLong(requestMessage.requestId.getMostSignificantBits());
            uuidBytes.writeLong(requestMessage.requestId.getLeastSignificantBits());
            final BinaryWebSocketFrame terminator = new BinaryWebSocketFrame(uuidBytes);

            final Future<?> iteration = executorService.submit((Callable<Void>) () -> {

                stopWatch.start();

                // todo: batch flush?
                while (itty.hasNext()) {
                    ctx.write(Pair.with(requestMessage, itty.next()));

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

                    final OpProcessorException ope = new OpProcessorException(errorMessage, serializer.serializeResult(errorMessage, ResultCode.SERVER_ERROR_TIMEOUT, context));
                    logger.warn(ope.getMessage(), ope);
                    ctx.write(ope.getFrame());
                }

                ctx.write(terminator);
                ctx.flush();
            });

        } else {
            ctx.write(msg, promise);
        }
    }
}
