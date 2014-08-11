package com.tinkerpop.gremlin.server.op.standard;

import com.codahale.metrics.Timer;
import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.GremlinServer;
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

import java.util.HashMap;
import java.util.Iterator;
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

        final String script = (String) msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Optional<String> language = Optional.ofNullable((String) msg.getArgs().get(Tokens.ARGS_LANGUAGE));
        final Map<String, Object> bindings = Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).orElse(new HashMap<>());

        final CompletableFuture<Object> future = context.getGremlinExecutor().eval(script, language, bindings);
        future.handle((v, t) -> timerContext.stop());
        future.thenAccept(o -> ctx.write(Pair.with(msg, convertToIterator(o))));
        future.exceptionally(se -> {
            logger.warn(String.format("Exception processing a script on request [%s].", msg), se);
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResultCode.SERVER_ERROR_SCRIPT_EVALUATION).result(se.getMessage()).create());
            return null;
        });
    }

    public static void traverseOp(final Context context) throws OpProcessorException {
        final Timer.Context timerContext = traverseOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();

        final Map<String, Object> args = msg.getArgs();
        final Map<String, Object> bindings = Optional.ofNullable((Map<String, Object>) args.get(Tokens.ARGS_BINDINGS)).orElse(new HashMap<>());

        final SFunction<Graph, Traversal> traversal;
        try {
            // deserialize the traversal and shove it into the bindings so that it can be executed within the
            // scriptengine.  the scriptengine acts as a sandbox within which to execute the traversal.
            traversal = (SFunction<Graph, Traversal>) Serializer.deserializeObject((byte[]) args.get(Tokens.ARGS_GREMLIN));
            bindings.put("____trvrslScrpt", traversal);
        } catch (Exception ex) {
            logger.warn(String.format("Exception processing a traversal on request [%s].", msg), ex);
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResultCode.SERVER_ERROR_TRAVERSAL_EVALUATION).result(ex.getMessage()).create());
            return;
        }

        final String script = String.format("____trvrslScrpt.apply(%s)", args.get(Tokens.ARGS_GRAPH_NAME));
        final CompletableFuture<Object> future = context.getGremlinExecutor().eval(script, bindings);
        future.handle((v, t) -> timerContext.stop());
        future.thenAccept(o -> ctx.write(Pair.with(msg, convertToIterator(o))));
        future.exceptionally(se -> {
            logger.warn(String.format("Exception processing a traversal on request [%s].", msg), se);
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResultCode.SERVER_ERROR_TRAVERSAL_EVALUATION).result(se.getMessage()).create());
            return null;
        });
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
