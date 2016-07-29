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
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.ScriptEngines;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TraversalIterator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.Serializer;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.SimpleBindings;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeoutException;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Simple {@link OpProcessor} implementation that iterates remotely submitted serialized {@link Traversal} objects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    public static final String OP_PROCESSOR_NAME = "traversal";
    public static final Timer traversalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "traversal"));

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
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_TRAVERSE:
                validateTraversalRequest(ctx, message);

                final Optional<Map<String, String>> traverseAliases = message.optionalArgs(Tokens.ARGS_ALIASES);
                final Map.Entry<String, String> traverserKv = traverseAliases.get().entrySet().iterator().next();
                if (!ctx.getGraphManager().getGraphs().containsKey(traverserKv.getValue())) {
                    final String msg = String.format("The graph [%s] for alias [%s] is not configured on the server.", traverserKv.getValue(), traverserKv.getKey());
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = this::iterateSerializedTraversal;
                break;
            case Tokens.OPS_BYTECODE:
                final Map<String, String> bytecodeAliases = validateTraversalRequest(ctx, message);
                final Map.Entry<String, String> bytecodeKv = bytecodeAliases.entrySet().iterator().next();
                if (!ctx.getGraphManager().getTraversalSources().containsKey(bytecodeKv.getValue())) {
                    final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", bytecodeKv.getValue(), bytecodeKv.getKey());
                    throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
                }

                op = this::iterateBytecodeTraversal;
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

    private static Map<String,String> validateTraversalRequest(final Context ctx, final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_TRAVERSE, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_TRAVERSE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (aliases.get().size() != 1) {
            final String msg = String.format("A message with an [%s] op code requires the [%s] argument to be a Map containing one alias assignment.", Tokens.OPS_TRAVERSE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return aliases.get();
    }

    private void iterateBytecodeTraversal(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        final Bytecode bytecode = (Bytecode) msg.getArgs().get(Tokens.ARGS_GREMLIN);

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSources().get(traversalSourceName);

        final Traversal.Admin<?, ?> traversal;
        try {
            // TODO: hardcoded to gremlin-groovy translation for now
            final ScriptEngines engines = context.getGremlinExecutor().getScriptEngines();
            final SimpleBindings b = new SimpleBindings();
            b.put("g", g);

            traversal = engines.eval(bytecode, b, "gremlin-groovy");
        } catch (Exception ex) {
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage()).create());
        }

        final Timer.Context timerContext = traversalOpTimer.time();
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final Graph graph = g.getGraph();
            final boolean supportsTransactions = graph.features().graph().supportsTransactions();

            context.getGremlinExecutor().getExecutorService().submit(() -> {
                try {
                    if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();

                    try {
                        // compile the traversal - without it getEndStep() has nothing in it
                        traversal.applyStrategies();
                        handleIterator(context, new TraversalIterator(traversal));
                    } catch (TimeoutException ex) {
                        final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg.getRequestId(), ex.getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                        if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();
                        return;
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                        if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();
                        return;
                    }

                    if (graph.features().graph().supportsTransactions()) graph.tx().commit();
                } catch (Exception ex) {
                    logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), ex);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                    if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
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

    @Override
    protected boolean isForceFlushed(final ChannelHandlerContext ctx, final RequestMessage msg, final Iterator itty) {
        return itty instanceof TraversalIterator && ((TraversalIterator) itty).isNextBatchComingUp();
    }

    @Override
    protected Map<String, Object> generateMetaData(final ChannelHandlerContext ctx, final RequestMessage msg,
                                                   final ResponseStatusCode code, final Iterator itty) {
        Map<String,Object> metaData = Collections.emptyMap();
        if (itty instanceof TraversalIterator) {
            final TraversalIterator traversalIterator = (TraversalIterator) itty;
            final String key = traversalIterator.getCurrentSideEffectKey();
            if (key != null) {
                metaData = new HashMap<>();
                metaData.put(Tokens.ARGS_SIDE_EFFECT, key);
                metaData.put(Tokens.ARGS_AGGREGATE_TO, traversalIterator.getCurrentSideEffectAggregator());
            }
        }

        return metaData;
    }

    private void iterateSerializedTraversal(final Context context) throws OpProcessorException {
        final RequestMessage msg = context.getRequestMessage();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        final byte[] serializedTraversal = (byte[]) msg.getArgs().get(Tokens.ARGS_GREMLIN);

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        final Traversal.Admin<?, ?> traversal;
        try {
            traversal = (Traversal.Admin) Serializer.deserializeObject(serializedTraversal);
        } catch (Exception ex) {
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage()).create());
        }

        if (traversal.isLocked())
            throw new OpProcessorException("Locked Traversals cannot be processed by the server",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage("Locked Traversals cannot be processed by the server").create());

        final Timer.Context timerContext = traversalOpTimer.time();
        try {
            final ChannelHandlerContext ctx = context.getChannelHandlerContext();
            final GraphManager graphManager = context.getGraphManager();
            final String graphName = aliases.entrySet().iterator().next().getValue();
            final Graph graph = graphManager.getGraphs().get(graphName);
            final boolean supportsTransactions = graph.features().graph().supportsTransactions();

            configureTraversal(traversal, graph);

            context.getGremlinExecutor().getExecutorService().submit(() -> {
                try {
                    if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();

                    try {
                        // compile the traversal - without it getEndStep() has nothing in it
                        traversal.applyStrategies();
                        handleIterator(context, new DetachingIterator<>(traversal));
                    } catch (TimeoutException ex) {
                        final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg.getRequestId(), ex.getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                        if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();
                        return;
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                        if (supportsTransactions && graph.tx().isOpen()) graph.tx().rollback();
                        return;
                    }

                    if (graph.features().graph().supportsTransactions()) graph.tx().commit();
                } catch (Exception ex) {
                    logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), ex);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
                    if (graph.features().graph().supportsTransactions() && graph.tx().isOpen()) graph.tx().rollback();
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

    private static void configureTraversal(final Traversal.Admin<?, ?> traversal, final Graph graph) {
        traversal.setGraph(graph);
        final List<TraversalStrategy<?>> strategies = TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).toList();
        final TraversalStrategy[] arrayOfStrategies = new TraversalStrategy[strategies.size()];
        strategies.toArray(arrayOfStrategies);
        traversal.getStrategies().addStrategies(arrayOfStrategies);
    }

    static class DetachingIterator<E> implements Iterator<Traverser.Admin<E>> {

        private Iterator<Traverser.Admin<E>> inner;
        private HaltedTraverserStrategy haltedTraverserStrategy;

        public DetachingIterator(final Traversal.Admin<?, E> traversal) {
            this.inner = traversal.getEndStep();
            this.haltedTraverserStrategy = traversal.getStrategies().getStrategy(HaltedTraverserStrategy.class).orElse(
                    Boolean.valueOf(System.getProperty("is.testing", "false")) ?
                            HaltedTraverserStrategy.detached() :
                            HaltedTraverserStrategy.reference());
        }

        @Override
        public boolean hasNext() {
            return this.inner.hasNext();
        }

        @Override
        public Traverser.Admin<E> next() {
            return this.haltedTraverserStrategy.halt(this.inner.next());
        }
    }
}
