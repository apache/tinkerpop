package com.tinkerpop.gremlin.server.op.session;

import com.codahale.metrics.Timer;
import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.Settings;
import com.tinkerpop.gremlin.server.handler.StateKey;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import com.tinkerpop.gremlin.util.function.TriConsumer;
import com.tinkerpop.gremlin.util.iterator.IteratorUtils;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang.time.StopWatch;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Operations to be used by the {@link SessionOpProcessor}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class SessionOps {
    private static final Logger logger = LoggerFactory.getLogger(SessionOps.class);

    /**
     * Script engines are evaluated in a per session context where imports/scripts are isolated per session.
     */
    private static ConcurrentHashMap<String, Session> sessions = new ConcurrentHashMap<>();

    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    static {
        MetricManager.INSTANCE.getGuage(sessions::size, name(GremlinServer.class, "sessions"));
    }

    public static void evalOp(final Context context) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();

        final Session session = getSession(context, msg);
        final ExecutorService executor = session.getGremlinExecutor().getExecutorService();

        // place the session on the channel context so that it can be used during serialization.  in this way
        // the serialization can occur on the same thread used to execute the gremlin within the session.  this
        // is important given the threadlocal nature of Graph implementation transactions.
        context.getChannelHandlerContext().channel().attr(StateKey.SESSION).set(session);

        final String script = (String) msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Optional<String> language = Optional.ofNullable((String) msg.getArgs().get(Tokens.ARGS_LANGUAGE));
        final Bindings bindings = session.getBindings();
        final Map<String, Object> requestBindings = Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).orElse(new HashMap<>());

        // parameter bindings override session bindings
        bindings.putAll(requestBindings);

        final CompletableFuture<Object> future = session.getGremlinExecutor().eval(script, language, bindings);
        future.handle((v, t) -> timerContext.stop());
        future.exceptionally(se -> {
            logger.warn(String.format("Exception processing a script on request [%s].", msg), se);
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION).statusMessage(se.getMessage()).create());
            return null;
        });

        final CompletableFuture<Void> iterationFuture = future.thenAcceptAsync(o -> {
            final Iterator itty = IteratorUtils.convertToIterator(o);
            // the batch size can be overriden by the request
            final int resultIterationBatchSize = (Integer) msg.optionalArgs(Tokens.ARGS_BATCH_SIZE).orElse(settings.resultIterationBatchSize);

            // timer for the total serialization time
            final StopWatch stopWatch = new StopWatch();

            logger.debug("Preparing to iterate results from - {} - in thread [{}]", msg, Thread.currentThread().getName());

            stopWatch.start();

            List<Object> aggregate = new ArrayList<>(resultIterationBatchSize);
            while (itty.hasNext()) {
                aggregate.add(itty.next());

                // send back a page of results if batch size is met or if it's the end of the results being
                // iterated
                if (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    ctx.writeAndFlush(ResponseMessage.build(msg)
                            .code(ResponseStatusCode.SUCCESS)
                            .result(aggregate).create());
                    aggregate = new ArrayList<>(resultIterationBatchSize);
                }

                stopWatch.split();
                if (stopWatch.getSplitTime() > settings.serializedResponseTimeout)
                    throw new RuntimeException(new TimeoutException("Serialization of the entire response exceeded the serializeResponseTimeout setting"));

                stopWatch.unsplit();
            }

            stopWatch.stop();
        }, executor);

        iterationFuture.handleAsync((r, ex) -> {
            // iteration has completed - if there was an exception then write it
            if (ex != null) {
                final String errorMessage = String.format("Response iteration and serialization exceeded the configured threshold for request [%s] - %s", msg, ex.getCause().getMessage());
                logger.warn(errorMessage);
                ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
            }

            // either way - terminate the request
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS_TERMINATOR).create());
            return null;
        }, executor);

    }

    public static void evalOp(final Context context, TriConsumer<Context, RequestMessage, Object> handler) {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();

        final Session session = getSession(context, msg);

        // place the session on the channel context so that it can be used during serialization.  in this way
        // the serialization can occur on the same thread used to execute the gremlin within the session.  this
        // is important given the threadlocal nature of Graph implementation transactions.
        context.getChannelHandlerContext().channel().attr(StateKey.SESSION).set(session);

        final String script = (String) msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Optional<String> language = Optional.ofNullable((String) msg.getArgs().get(Tokens.ARGS_LANGUAGE));
        final Bindings bindings = session.getBindings();
        final Map<String, Object> requestBindings = Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).orElse(new HashMap<>());

        // parameter bindings override session bindings
        bindings.putAll(requestBindings);

        final CompletableFuture<Object> future = session.getGremlinExecutor().eval(script, language, bindings);
        future.handle((v, t) -> timerContext.stop());
        future.thenAccept(o -> handler.accept(context, msg, o));
        future.exceptionally(se -> {
            logger.warn(String.format("Exception processing a script on request [%s].", msg), se);
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION).statusMessage(se.getMessage()).create());
            return null;
        });
    }

    private static Session getSession(final Context context, final RequestMessage msg) {
        final String sessionId = (String) msg.getArgs().get(Tokens.ARGS_SESSION);
        final Session session = sessions.computeIfAbsent(sessionId, k -> new Session(k, context, sessions));
        session.touch();
        return session;
    }
}
