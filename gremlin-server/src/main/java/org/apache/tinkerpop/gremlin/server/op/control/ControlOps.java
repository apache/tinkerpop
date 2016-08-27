/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.op.control;

import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.engine.ScriptEngines;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.util.Gremlin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As for release 3.2.2, not replaced as this feature was never really published as official.
 */
@Deprecated
class ControlOps {
    private static final Logger logger = LoggerFactory.getLogger(ControlOps.class);

    public static void versionOp(final Context context) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS).result(Gremlin.version()), ctx.voidPromise());
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
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
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
            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS).result(infoToShow).create(), ctx.voidPromise());
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.",
                    infoToShow, context.getRequestMessage(), ex);
        }
    }

    /**
     * Resets the {@code ScriptEngine} thus forcing a reload of scripts and classes.
     */
    public static void resetOp(final Context context) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        context.getGremlinExecutor().getScriptEngines().reset();
        context.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS).create(), ctx.voidPromise());
    }

    /**
     * Pull in maven based dependencies and load Gremlin plugins.
     */
    public static void useOp(final Context context) {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
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

            context.getChannelHandlerContext().writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS).result(coords).create(), ctx.voidPromise());
        });
    }
}
