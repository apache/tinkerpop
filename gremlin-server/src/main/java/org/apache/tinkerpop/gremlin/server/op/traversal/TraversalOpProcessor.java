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
package org.apache.tinkerpop.gremlin.server.op.traversal;

import com.codahale.metrics.Timer;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.ScriptEngines;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.SideEffectIterator;
import org.apache.tinkerpop.gremlin.server.util.TraversalIterator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Simple {@link OpProcessor} implementation that iterates remotely submitted serialized {@link Traversal} objects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    private static final ObjectMapper mapper = GraphSONMapper.build().create().createMapper();
    public static final String OP_PROCESSOR_NAME = "traversal";
    public static final Timer traversalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "traversal"));

    public static final Settings.ProcessorSettings DEFAULT_SETTINGS = new Settings.ProcessorSettings();

    /**
     * Configuration setting for how long a cached side-effect will be available before it is evicted from the cache.
     */
    public static final String CONFIG_CACHE_EXPIRATION_TIME = "cacheExpirationTime";

    /**
     * Default timeout for a cached side-effect is ten minutes.
     */
    public static final long DEFAULT_CACHE_EXPIRATION_TIME = 600000;

    /**
     * Configuration setting for the maximum number of entries the cache will have.
     */
    public static final String CONFIG_CACHE_MAX_SIZE = "cacheMaxSize";

    /**
     * Default size of the max size of the cache.
     */
    public static final long DEFAULT_CACHE_MAX_SIZE = 1000;

    static {
        DEFAULT_SETTINGS.className = TraversalOpProcessor.class.getCanonicalName();
        DEFAULT_SETTINGS.config = new HashMap<String, Object>() {{
            put(CONFIG_CACHE_EXPIRATION_TIME, DEFAULT_CACHE_EXPIRATION_TIME);
            put(CONFIG_CACHE_MAX_SIZE, DEFAULT_CACHE_MAX_SIZE);
        }};
    }

    private static Cache<UUID, TraversalSideEffects> cache = null;

    public TraversalOpProcessor() {
        super(true);
    }

    @Override
    public String getName() {
        return OP_PROCESSOR_NAME;
    }

    @Override
    public void close() throws Exception {
        // do nothing = no resources to release
    }

    @Override
    public void init(final Settings settings) {
        final Settings.ProcessorSettings processorSettings = settings.processors.stream()
                .filter(p -> p.className.equals(TraversalOpProcessor.class.getCanonicalName()))
                .findAny().orElse(TraversalOpProcessor.DEFAULT_SETTINGS);
        final long maxSize = Long.parseLong(processorSettings.config.get(TraversalOpProcessor.CONFIG_CACHE_MAX_SIZE).toString());
        final long expirationTime = Long.parseLong(processorSettings.config.get(TraversalOpProcessor.CONFIG_CACHE_EXPIRATION_TIME).toString());

        cache = Caffeine.newBuilder()
                .expireAfterWrite(expirationTime, TimeUnit.MILLISECONDS)
                .maximumSize(maxSize)
                .build();

        logger.info("Initialized cache for {} with size {} and expiration time of {} ms",
                TraversalOpProcessor.class.getSimpleName(), maxSize, expirationTime);
    }

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                final Map<String, String> bytecodeAliases = validateTraversalRequest(message);
                final Map.Entry<String, String> bytecodeKv = bytecodeAliases.entrySet().iterator().next();
                if (!ctx.getGraphManager().getTraversalSources().containsKey(bytecodeKv.getValue())) {
                    final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", bytecodeKv.getValue(), bytecodeKv.getKey());
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = this::iterateBytecodeTraversal;
                break;
            case Tokens.OPS_GATHER:
                final Optional<String> sideEffectForGather = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                if (!sideEffectForGather.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_GATHER, Tokens.ARGS_SIDE_EFFECT);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                final Optional<String> sideEffectKey = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT_KEY);
                if (!sideEffectKey.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_GATHER, Tokens.ARGS_SIDE_EFFECT_KEY);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                validatedAliases(message);

                op = this::gatherSideEffect;

                break;
            case Tokens.OPS_KEYS:
                final Optional<String> sideEffectForKeys = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                if (!sideEffectForKeys.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_GATHER, Tokens.ARGS_SIDE_EFFECT);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = context -> {
                    final RequestMessage msg = context.getRequestMessage();
                    logger.debug("Close request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

                    final Optional<UUID> sideEffect = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                    final TraversalSideEffects sideEffects = cache.getIfPresent(sideEffect.get());

                    if (null == sideEffects) {
                        final String msgDefault = String.format("Could not find side-effects for %s.", sideEffect.get());
                        throw new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.SERVER_ERROR).statusMessage(msgDefault).create());
                    }

                    handleIterator(context, sideEffects.keys().iterator());
                };

                break;
            case Tokens.OPS_CLOSE:
                final Optional<String> sideEffectForClose = message.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                if (!sideEffectForClose.isPresent()) {
                    final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_CLOSE, Tokens.ARGS_SIDE_EFFECT);
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = context -> {
                    final RequestMessage msg = context.getRequestMessage();
                    logger.debug("Close request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

                    final Optional<UUID> sideEffect = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
                    cache.invalidate(sideEffect.get());
                };

                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid = String.format("Message could not be parsed.  Check the format of the request. [%s]", message);
                throw new OpProcessorException(msgInvalid, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgInvalid).create());
            default:
                final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgDefault).create());
        }

        return op;
    }

    private static Map<String, String> validateTraversalRequest(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        final Optional<Map<String, String>> aliases = validatedAliases(message);

        return aliases.get();
    }

    private static Optional<Map<String, String>> validatedAliases(RequestMessage message) throws OpProcessorException {
        final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (aliases.get().size() != 1) {
            final String msg = String.format("A message with [%s] op code requires the [%s] argument to be a Map containing one alias assignment.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }
        return aliases;
    }

    private void gatherSideEffect(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Side-effect request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        // earlier validation in selection of this op method should free us to cast this without worry
        final Optional<UUID> sideEffect = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT);
        final Optional<String> sideEffectKey = msg.optionalArgs(Tokens.ARGS_SIDE_EFFECT_KEY);
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSources().get(traversalSourceName);

        final Timer.Context timerContext = traversalOpTimer.time();
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final Graph graph = g.getGraph();

            context.getGremlinExecutor().getExecutorService().submit(() -> {
                try {
                    beforeProcessing(graph, context);

                    try {
                        final TraversalSideEffects sideEffects = cache.getIfPresent(sideEffect.get());

                        if (null == sideEffects) {
                            final String errorMessage = String.format("Could not find side-effects for %s.", sideEffect.get());
                            logger.warn(errorMessage);
                            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                            onError(graph, context);
                            return;
                        }

                        handleIterator(context, new SideEffectIterator(sideEffects.get(sideEffectKey.get()), sideEffectKey.get()));
                    } catch (TimeoutException ex) {
                        final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg.getRequestId(), ex.getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                        onError(graph, context);
                        return;
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a side-effect on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                        onError(graph, context);
                        return;
                    }

                    onSideEffectSuccess(graph, context);
                } catch (Exception ex) {
                    logger.warn(String.format("Exception processing a side-effect on request [%s].", msg.getRequestId()), ex);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                    onError(graph, context);
                } finally {
                    timerContext.stop();
                }
            });

        } catch (Exception ex) {
            timerContext.stop();
            throw new OpProcessorException("Could not iterate the side-effect instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
        }
    }

    private void iterateBytecodeTraversal(final Context context) throws OpProcessorException, Exception {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        // TODO: Look to polish this up in GraphSON 2.0 when we don't have type lossiness anymore
        // right now the TraversalOpPorcessor can take a direct GraphSON representation of Bytecode or directly take
        // deserialized Bytecode object.
        final Object bytecodeObj = msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Bytecode bytecode = bytecodeObj instanceof Bytecode ? (Bytecode) bytecodeObj :
            mapper.readValue(bytecodeObj.toString(), Bytecode.class);

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSources().get(traversalSourceName);

        final Traversal.Admin<?, ?> traversal;
        try {
            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent())
                traversal = JavaTranslator.of(g).translate(bytecode);
            else {
                final ScriptEngines engines = context.getGremlinExecutor().getScriptEngines();
                final SimpleBindings b = new SimpleBindings();
                b.put("g", g);
                traversal = engines.eval(bytecode, b, lambdaLanguage.get());
            }
        } catch (Exception ex) {
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage()).create());
        }

        final Timer.Context timerContext = traversalOpTimer.time();
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final Graph graph = g.getGraph();

            context.getGremlinExecutor().getExecutorService().submit(() -> {
                try {
                    beforeProcessing(graph, context);

                    try {
                        // compile the traversal - without it getEndStep() has nothing in it
                        traversal.applyStrategies();
                        handleIterator(context, new TraversalIterator(traversal));

                        if (!traversal.getSideEffects().isEmpty())
                            cache.put(msg.getRequestId(), traversal.getSideEffects());
                    } catch (TimeoutException ex) {
                        final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg.getRequestId(), ex.getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                        onError(graph, context);
                        return;
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                        onError(graph, context);
                        return;
                    }

                    onTraversalSuccess(graph, context);
                } catch (Exception ex) {
                    logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), ex);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                    onError(graph, context);
                } finally {
                    timerContext.stop();
                }
            });

        } catch (Exception ex) {
            timerContext.stop();
            throw new OpProcessorException("Could not iterate the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
        }
    }

    protected void beforeProcessing(final Graph graph, final Context ctx) {
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
    }

    protected void onError(final Graph graph, final Context ctx) {
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
    }

    protected void onTraversalSuccess(final Graph graph, final Context ctx) {
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().commit();
    }

    protected void onSideEffectSuccess(final Graph graph, final Context ctx) {
        // there was no "writing" here, just side-effect retrieval, so if a transaction was opened then
        // just close with rollback
        if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
    }

    @Override
    protected Map<String, Object> generateMetaData(final ChannelHandlerContext ctx, final RequestMessage msg,
                                                   final ResponseStatusCode code, final Iterator itty) {
        Map<String, Object> metaData = Collections.emptyMap();
        if (itty instanceof SideEffectIterator) {
            final SideEffectIterator traversalIterator = (SideEffectIterator) itty;
            final String key = traversalIterator.getSideEffectKey();
            if (key != null) {
                metaData = new HashMap<>();
                metaData.put(Tokens.ARGS_SIDE_EFFECT_KEY, key);
                metaData.put(Tokens.ARGS_AGGREGATE_TO, traversalIterator.getSideEffectAggregator());
            }
        }

        return metaData;
    }
}
