package com.tinkerpop.gremlin.server.op.control;

import com.tinkerpop.gremlin.driver.Tokens;
import com.tinkerpop.gremlin.driver.message.RequestMessage;
import com.tinkerpop.gremlin.driver.message.ResponseMessage;
import com.tinkerpop.gremlin.driver.message.ResultCode;
import com.tinkerpop.gremlin.driver.message.ResultType;
import com.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import com.tinkerpop.gremlin.groovy.engine.ScriptEngines;
import com.tinkerpop.gremlin.server.Context;
import com.tinkerpop.gremlin.util.Gremlin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
class ControlOps {
    private static final Logger logger = LoggerFactory.getLogger(ControlOps.class);

    public static void versionOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS).contents(ResultType.OBJECT).result(Gremlin.version()));
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS_TERMINATOR).build());
    }

    /**
     * Modify the imports on the {@code ScriptEngine}.
     */
    public static void importOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final List<String> l = (List<String>) msg.getArgs().get(Tokens.ARGS_IMPORTS);
        context.getGremlinExecutor().getScriptEngines().addImports(new HashSet<>(l));
    }

    /**
     * List the dependencies, imports, or variables in the {@code ScriptEngine}.
     */
    public static void showOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        final String infoType = msg.<String>optionalArgs(Tokens.ARGS_INFO_TYPE).get();
        final GremlinExecutor executor = context.getGremlinExecutor();
        final ScriptEngines scriptEngines = executor.getScriptEngines();

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
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS).contents(ResultType.OBJECT).result(infoToShow).build());
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS_TERMINATOR).build());
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.",
                    infoToShow, context.getRequestMessage(), ex);
        }
    }

    /**
     * Resets the {@code ScriptEngine} thus forcing a reload of scripts and classes.
     */
    public static void resetOp(final Context context) {
        final RequestMessage msg = context.getRequestMessage();
        context.getGremlinExecutor().getScriptEngines().reset();
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS).contents(ResultType.EMPTY).build());
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS_TERMINATOR).build());
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
            context.getGremlinExecutor().getScriptEngines().use(group, artifact, version);

            final Map<String, String> coords = new HashMap<String, String>() {{
                put("group", group);
                put("artifact", artifact);
                put("version", version);
            }};

            context.getChannelHandlerContext().write(ResponseMessage.create(msg).code(ResultCode.SUCCESS).contents(ResultType.OBJECT).result(coords).build());
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.create(msg).code(ResultCode.SUCCESS_TERMINATOR).build());
        });
    }
}
