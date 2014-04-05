package com.tinkerpop.gremlin.server.op.standard;

import com.codahale.metrics.Timer;
import com.tinkerpop.gremlin.process.util.SingleIterator;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.GremlinExecutor;
import com.tinkerpop.gremlin.server.GremlinServer;
import com.tinkerpop.gremlin.server.message.ResultCode;
import com.tinkerpop.gremlin.server.ScriptEngines;
import com.tinkerpop.gremlin.server.Tokens;
import com.tinkerpop.gremlin.server.message.RequestMessage;
import com.tinkerpop.gremlin.server.message.ResponseMessage;
import com.tinkerpop.gremlin.server.op.OpProcessorException;
import com.tinkerpop.gremlin.server.util.MetricManager;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    /**
     * Modify the imports on the {@code ScriptEngine}.
     */
    public static void importOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final List<String> l = (List<String>) msg.getArgs().get(Tokens.ARGS_IMPORTS);
        context.getGremlinExecutor().getSharedScriptEngines().addImports(new HashSet<>(l));
    }

    /**
     * List the dependencies, imports, or variables in the {@code ScriptEngine}.
     */
    public static void showOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final String infoType = msg.<String>optionalArgs(Tokens.ARGS_INFO_TYPE).get();
        final GremlinExecutor executor = context.getGremlinExecutor();
        final ScriptEngines scriptEngines = executor.getSharedScriptEngines();

        final Object infoToShow;
        if (infoType.equals(Tokens.ARGS_INFO_TYPE_DEPDENENCIES))
            infoToShow = scriptEngines.dependencies();
        else if (infoType.equals(Tokens.ARGS_INFO_TYPE_IMPORTS))
            infoToShow = scriptEngines.imports();
        else {
            // this shouldn't happen if validations are working properly.  will bomb and log as error to server logs
            // thus killing the connection
            throw new RuntimeException(String.format("Validation for the show operation is not properly checking the %s", Tokens.ARGS_INFO_TYPE));
        }

        try {
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS).result(infoToShow).build());
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS_TERMINATOR).result(Optional.empty()).build());
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.",
                    infoToShow, context.getRequestMessage(), ex);
        }
    }

    /**
     * Resets the {@code ScriptEngine} thus forcing a reload of scripts and classes.
     */
    public static void resetOp(final Context context) {
        context.getGremlinExecutor().getSharedScriptEngines().reset();
    }

    /**
     * Pull in maven based dependencies and load Gremlin plugins.
     */
    public static void useOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final List<Map<String, String>> usings = (List<Map<String, String>>) msg.getArgs().get(Tokens.ARGS_COORDINATES);
        usings.forEach(c -> {
            final String group = c.get(Tokens.ARGS_COORDINATES_GROUP);
            final String artifact = c.get(Tokens.ARGS_COORDINATES_ARTIFACT);
            final String version = c.get(Tokens.ARGS_COORDINATES_VERSION);
            logger.info("Loading plugin [group={},artifact={},version={}]", group, artifact, version);
            context.getGremlinExecutor().getSharedScriptEngines().use(group, artifact, version);

            final Map<String, String> coords = new HashMap<String, String>() {{
                put("group", group);
                put("artifact", artifact);
                put("version", version);
            }};

            context.getChannelHandlerContext().write(ResponseMessage.create(msg).code(ResultCode.SUCCESS).result(coords).build());
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS_TERMINATOR).result(Optional.empty()).build());
        });
    }

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
                logger.debug("Exception from ScriptException error.", se);
                ctx.writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SERVER_ERROR_SCRIPT_EVALUATION).result(se.getMessage()).build());
                return null;
            }).thenRun(timerContext::stop);

        } catch (Exception ex) {
            // todo: necessary?
            throw new OpProcessorException(String.format("Error while evaluating a script on request [%s]", msg),
                    ResponseMessage.create(msg).code(ResultCode.SERVER_ERROR_SCRIPT_EVALUATION).result(ex.getMessage()).build());
        }
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
