package com.tinkerpop.gremlin.server.op.standard;

import com.tinkerpop.gremlin.pipes.util.SingleIterator;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.server.OpProcessor;
import com.tinkerpop.gremlin.server.RequestMessage;
import com.tinkerpop.gremlin.server.ResultSerializer;
import com.tinkerpop.gremlin.server.ServerTokens;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.ScriptException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

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
     * List the dependencies in the ScriptEngine ClassLoader.
     */
    public static void depsOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs(ServerTokens.ARGS_ACCEPT).orElse("text/plain"));
        final Map dependencies = context.getGremlinExecutor().select(msg).dependencies();
        try {
            ctx.channel().write(new TextWebSocketFrame(serializer.serialize(dependencies, context)));
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.",
                    dependencies, context.getRequestMessage(), ex);
        }
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
        Object o;
        try {
            o = context.getGremlinExecutor().eval(msg, context.getGraphs());
        } catch (ScriptException se) {
            logger.warn("Error while evaluating a script on request [{}]", msg);
            logger.debug("Exception from ScriptException error.", se);
            OpProcessor.error(se.getMessage()).accept(context);
            return;
        } catch (InterruptedException ie) {
            logger.warn("Thread interrupted (perhaps script ran for too long) while processing this request [{}]", msg);
            logger.debug("Exception from InterruptedException error.", ie);
            OpProcessor.error(ie.getMessage()).accept(context);
            return;
        } catch (ExecutionException ee) {
            logger.warn("Error while retrieving response from the script evaluated on request [{}]", msg);
            logger.debug("Exception from ExecutionException error.", ee.getCause());
            Throwable inner = ee.getCause();
            if (inner instanceof ScriptException)
                inner = inner.getCause();

            OpProcessor.error(inner.getMessage()).accept(context);
            return;
        }

        Iterator itty;
        if (o instanceof Iterable)
            itty = ((Iterable) o).iterator();
        else if (o instanceof Iterator)
            itty = (Iterator) o;
        else if (o instanceof Object[])
            itty = new ArrayIterator(o);
        else if (o instanceof Map)
            itty = ((Map) o).entrySet().iterator();
        else if (o instanceof Throwable)
            itty = new SingleIterator<Object>(((Throwable) o).getMessage());
        else
            itty = new SingleIterator<>(o);

        final ResultSerializer serializer = ResultSerializer.select(msg.<String>optionalArgs(ServerTokens.ARGS_ACCEPT).orElse("text/plain"));
        itty.forEachRemaining(j -> {
            try {
                ctx.channel().write(new TextWebSocketFrame(true, 0, serializer.serialize(j, context)));
            } catch (Exception ex) {
                logger.warn("The result [{}] in the request {} could not be serialized and returned.", j, context.getRequestMessage(), ex);
            }
        });

        // sending the requestId acts as a termination message for this request.
        final ByteBuf uuidBytes = Unpooled.directBuffer(16);
        uuidBytes.writeLong(msg.requestId.getMostSignificantBits());
        uuidBytes.writeLong(msg.requestId.getLeastSignificantBits());
        ctx.channel().write(new BinaryWebSocketFrame(uuidBytes));
    }
}
