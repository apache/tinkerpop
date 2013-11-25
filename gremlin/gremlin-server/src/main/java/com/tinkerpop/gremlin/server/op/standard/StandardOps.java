package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import com.tinkerpop.gremlin.server.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * Operations to be used by the StandardOpProcessor.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
final class StandardOps {
    private static final Logger logger = LoggerFactory.getLogger(StandardOps.class);

    /**
     * Modify the imports on the ScriptEngine.
     */
    public static void importOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final List<String> l = (List<String>) msg.args.get(ServerTokens.ARGS_IMPORTS);
        context.getGremlinExecutor().select(msg).addImports(new HashSet<>(l));
    }

    /**
     * List the dependencies, imports, or variables in the ScriptEngine
     */
    public static void showOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs(ServerTokens.ARGS_ACCEPT).orElse("text/plain"));
        final String infoType = msg.<String>optionalArgs(ServerTokens.ARGS_INFO_TYPE).get();
        final GremlinExecutor executor = context.getGremlinExecutor();
        final ScriptEngineOps seo = executor.select(msg);

        final Object infoToShow;
        if (infoType.equals(ServerTokens.ARGS_INFO_TYPE_DEPDENENCIES))
            infoToShow = seo.dependencies();
        else if (infoType.equals(ServerTokens.ARGS_INFO_TYPE_IMPORTS))
            infoToShow  = seo.imports();
        else if (infoType.equals(ServerTokens.ARGS_INFO_TYPE_VARIABLES))
            infoToShow = executor.getBindingsAsMap(msg);
        else
            throw new RuntimeException(String.format("Validation for the show operation is not properly checking the %s", ServerTokens.ARGS_INFO_TYPE));

        try {
            ctx.channel().write(new TextWebSocketFrame(serializer.serialize(infoToShow, context)));
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.",
                    infoToShow, context.getRequestMessage(), ex);
        }
    }

    /**
     * Resets the ScriptEngine thus forcing a reload of scripts and classes.
     */
    public static void resetOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        context.getGremlinExecutor().select(msg).reset();
    }

    /**
     * Pull in maven based dependencies and load Gremlin plugins.
     */
    public static void useOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final List<Map<String,String>> usings = (List<Map<String,String>>) msg.args.get(ServerTokens.ARGS_COORDINATES);
        usings.forEach(c -> {
            final String group = c.get(ServerTokens.ARGS_COORDINATES_GROUP);
            final String artifact = c.get(ServerTokens.ARGS_COORDINATES_ARTIFACT);
            final String version = c.get(ServerTokens.ARGS_COORDINATES_VERSION);
            logger.info("Loading plugin [group={},artifact={},version={}]", group, artifact, version);
            context.getGremlinExecutor().select(msg).use(group, artifact, version);
            OpProcessor.text(String.format("Plugin loaded - [group=%s,artifact=%s,version=%s]", group, artifact, version)).accept(context);
        });
    }

    /**
     * Evaluate a script in the script engine.
     */
    public static void evalOp(final Context context) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs(ServerTokens.ARGS_ACCEPT).orElse("text/plain"));

        Object o;
        try {
            o = context.getGremlinExecutor().eval(msg, context.getGraphs());

            Iterator itty;
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

            itty.forEachRemaining(j -> {
                try {
                    ctx.channel().write(new TextWebSocketFrame(true, 0, serializer.serialize(j, context)));
                } catch (Exception ex) {
                    logger.warn("The result [{}] in the request {} could not be serialized and returned.", j, context.getRequestMessage(), ex);
                }
            });

        } catch (ScriptException se) {
            logger.warn("Error while evaluating a script on request [{}]", msg);
            logger.debug("Exception from ScriptException error.", se);
            OpProcessor.error(serializer.serialize(se.getMessage(), ResultCode.FAIL, context)).accept(context);
        } catch (InterruptedException ie) {
            logger.warn("Thread interrupted (perhaps script ran for too long) while processing this request [{}]", msg);
            logger.debug("Exception from InterruptedException error.", ie);
            OpProcessor.error(serializer.serialize(ie.getMessage(), ResultCode.FAIL, context)).accept(context);
        } catch (ExecutionException ee) {
            logger.warn("Error while retrieving response from the script evaluated on request [{}]", msg);
            logger.debug("Exception from ExecutionException error.", ee.getCause());
            Throwable inner = ee.getCause();
            if (inner instanceof ScriptException)
                inner = inner.getCause();

            OpProcessor.error(serializer.serialize(inner.getMessage(), ResultCode.FAIL, context)).accept(context);
        } catch (TimeoutException toe) {
            final String errorMessage = String.format("Script evaluation exceeded the configured threshold of %s ms for request [%s]", context.getSettings().scriptEvaluationTimeout, msg);
            logger.warn(errorMessage, context.getSettings().scriptEvaluationTimeout, msg);
            final String json = serializer.serialize(errorMessage, ResultCode.FAIL, context);
            OpProcessor.error(json).accept(context);
        }
        finally {
            // sending the requestId acts as a termination message for this request.
            final ByteBuf uuidBytes = Unpooled.directBuffer(16);
            uuidBytes.writeLong(msg.requestId.getMostSignificantBits());
            uuidBytes.writeLong(msg.requestId.getLeastSignificantBits());
            ctx.channel().write(new BinaryWebSocketFrame(uuidBytes));
        }
    }
}
