package com.tinkerpop.gremlin.server.op.standard;

import com.codahale.metrics.Timer;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.GremlinExecutor;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.server.ScriptEngines;
import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.util.Serializer;
import com.tinkerpop.gremlin.util.function.SFunction;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Operations to be used by the {@link StandardOpProcessor}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class StandardOps {
    private static final Logger logger = LoggerFactory.getLogger(StandardOps.class);
    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));
    private static final Timer traverseOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "traverse"));

    public static void evalOp(final Context context) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        try {
            final CompletableFuture<Object> future = context.getGremlinExecutor().eval(msg, context);
            future.thenAccept(o -> {
                ctx.write(Pair.with(msg, convertToIterator(o)));
            }).thenRun(timerContext::stop);

            future.exceptionally(se -> {
                logger.warn("Exception from ScriptException error.", se);
                ctx.writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SERVER_ERROR_SCRIPT_EVALUATION).result(se.getMessage()).build());
                return null;
            }).thenRun(timerContext::stop);

        } catch (Exception ex) {
            // todo: necessary? - exceptionally above may already be handling this
            throw new OpProcessorException(String.format("Error while evaluating a script on request [%s]", msg),
                    ResponseMessage.create(msg).code(ResultCode.SERVER_ERROR_SCRIPT_EVALUATION).result(ex.getMessage()).build());
        }
    }

    public static void traverseOp(final Context context) throws OpProcessorException {
        final Timer.Context timerContext = traverseOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final CompletableFuture<Traversal> future = CompletableFuture.supplyAsync(() -> {
            try {
                // todo: maybe this should always eval out in the scriptengine - basically a sandbox
                final Map<String,Object> args = msg.getArgs();
                final SFunction<Graph, Traversal> traversal = (SFunction<Graph, Traversal>) Serializer.deserializeObject((byte[]) args.get(Tokens.ARGS_GREMLIN));

                // previously validated that the graph was present
                return traversal.apply(context.getGraphs().getGraphs().get(args.get(Tokens.ARGS_GRAPH_NAME)));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        });

        future.thenAccept(o -> {
            ctx.write(Pair.with(msg, convertToIterator(o)));
        }).thenRun(timerContext::stop);

        future.exceptionally(se -> {
            logger.warn(String.format("Exception processing a traversal on request [%s].", msg), se);
            ctx.writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SERVER_ERROR_TRAVERSAL_EVALUATION).result(se.getMessage()).build());
            return null;
        }).thenRun(timerContext::stop);
    }

    private static Iterator convertToIterator(final Object o) {
        final Iterator itty;
        if (o instanceof Iterable)
            itty = ((Iterable) o).iterator();
        else if (o instanceof Iterator)
            itty = (Iterator) o;
        else if (o instanceof Object[])
            itty = new ArrayIterator(o);
        else if (o instanceof Stream)
            itty = ((Stream) o).iterator();
        else if (o instanceof Map)
            itty = ((Map) o).entrySet().iterator();
        else if (o instanceof Throwable)
            itty = new SingleIterator<Object>(((Throwable) o).getMessage());
        else
            itty = new SingleIterator<>(o);
        return itty;
    }
}
