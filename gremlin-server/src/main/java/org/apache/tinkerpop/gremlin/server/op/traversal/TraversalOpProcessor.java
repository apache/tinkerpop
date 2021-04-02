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
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.op.AbstractOpProcessor;
import org.apache.tinkerpop.gremlin.server.op.OpProcessorException;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * Simple {@link OpProcessor} implementation that iterates remotely submitted serialized {@link Traversal} objects.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class TraversalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(TraversalOpProcessor.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);
    private static final ObjectMapper mapper = GraphSONMapper.build().version(GraphSONVersion.V2_0).create().createMapper();
    public static final String OP_PROCESSOR_NAME = "traversal";
    public static final Timer traversalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "traversal"));

    private static final Bindings EMPTY_BINDINGS = new SimpleBindings();

    public TraversalOpProcessor() {
        super(false);
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
    public ThrowingConsumer<Context> select(final Context context) throws OpProcessorException {
        final RequestMessage message = context.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_BYTECODE:
                validateTraversalSourceAlias(context, message, validateTraversalRequest(message));
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

    private static void validateTraversalSourceAlias(final Context ctx, final RequestMessage message, final Map<String, String> aliases) throws OpProcessorException {
        final String traversalSourceBindingForAlias = aliases.values().iterator().next();
        if (!ctx.getGraphManager().getTraversalSourceNames().contains(traversalSourceBindingForAlias)) {
            final String msg = String.format("The traversal source [%s] for alias [%s] is not configured on the server.", traversalSourceBindingForAlias, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }
    }

    private static Map<String, String> validateTraversalRequest(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return validatedAliases(message).get();
    }

    private static Optional<Map<String, String>> validatedAliases(final RequestMessage message) throws OpProcessorException {
        final Optional<Map<String, String>> aliases = message.optionalArgs(Tokens.ARGS_ALIASES);
        if (!aliases.isPresent()) {
            final String msg = String.format("A message with [%s] op code requires a [%s] argument.", Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (aliases.get().size() != 1 || !aliases.get().containsKey(Tokens.VAL_TRAVERSAL_SOURCE_ALIAS)) {
            final String msg = String.format("A message with [%s] op code requires the [%s] argument to be a Map containing one alias assignment named '%s'.",
                    Tokens.OPS_BYTECODE, Tokens.ARGS_ALIASES, Tokens.VAL_TRAVERSAL_SOURCE_ALIAS);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        return aliases;
    }

    private void iterateBytecodeTraversal(final Context context) throws Exception {
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        logger.debug("Traversal request {} for in thread {}", msg.getRequestId(), Thread.currentThread().getName());

        // right now the TraversalOpProcessor can take a direct GraphSON representation of Bytecode or directly take
        // deserialized Bytecode object.
        final Object bytecodeObj = msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Bytecode bytecode = bytecodeObj instanceof Bytecode ? (Bytecode) bytecodeObj :
                mapper.readValue(bytecodeObj.toString(), Bytecode.class);

        // earlier validation in selection of this op method should free us to cast this without worry
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();

        // timeout override - handle both deprecated and newly named configuration. earlier logic should prevent
        // both configurations from being submitted at the same time
        final Map<String, Object> args = msg.getArgs();
        final long seto = args.containsKey(Tokens.ARGS_EVAL_TIMEOUT) ?
                ((Number) args.get(Tokens.ARGS_EVAL_TIMEOUT)).longValue() : context.getSettings().getEvaluationTimeout();

        final GraphManager graphManager = context.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSource(traversalSourceName);

        final Traversal.Admin<?, ?> traversal;
        try {
            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent())
                traversal = JavaTranslator.of(g).translate(bytecode);
            else
                traversal = context.getGremlinExecutor().eval(bytecode, EMPTY_BINDINGS, lambdaLanguage.get(), traversalSourceName);
        } catch (ScriptException ex) {
            logger.error("Traversal contains a lambda that cannot be compiled", ex);
            throw new OpProcessorException("Traversal contains a lambda that cannot be compiled",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        } catch (Exception ex) {
            logger.error("Could not deserialize the Traversal instance", ex);
            throw new OpProcessorException("Could not deserialize the Traversal instance",
                    ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
        }
        if (settings.enableAuditLog) {
            AuthenticatedUser user = context.getChannelHandlerContext().channel().attr(StateKey.AUTHENTICATED_USER).get();
            if (null == user) {    // This is expected when using the AllowAllAuthenticator
                user = AuthenticatedUser.ANONYMOUS_USER;
            }
            String address = context.getChannelHandlerContext().channel().remoteAddress().toString();
            if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
            auditLogger.info("User {} with address {} requested: {}", user.getName(), address, bytecode);
        }
        if (settings.authentication.enableAuditLog) {
            String address = context.getChannelHandlerContext().channel().remoteAddress().toString();
            if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
            auditLogger.info("User with address {} requested: {}", address, bytecode);
        }

        final Timer.Context timerContext = traversalOpTimer.time();
        final FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            final Graph graph = g.getGraph();

            try {
                beforeProcessing(graph, context);

                try {
                    // compile the traversal - without it getEndStep() has nothing in it
                    traversal.applyStrategies();
                    handleIterator(context, new TraverserIterator(traversal), graph);
                } catch (Exception ex) {
                    Throwable t = ex;
                    if (ex instanceof UndeclaredThrowableException)
                        t = t.getCause();

                    // if any exception in the chain is TemporaryException then we should respond with the right error
                    // code so that the client knows to retry
                    final Optional<Throwable> possibleTemporaryException = determineIfTemporaryException(ex);
                    if (possibleTemporaryException.isPresent()) {
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TEMPORARY)
                                .statusMessage(possibleTemporaryException.get().getMessage())
                                .statusAttributeException(possibleTemporaryException.get()).create());
                    } else if (t instanceof InterruptedException || t instanceof TraversalInterruptedException) {
                        final String errorMessage = String.format("A timeout occurred during traversal evaluation of [%s] - consider increasing the limit given to evaluationTimeout", msg);
                        logger.warn(errorMessage);
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                                                             .statusMessage(errorMessage)
                                                             .statusAttributeException(ex).create());
                    } else {
                        logger.warn(String.format("Exception processing a Traversal on iteration for request [%s].", msg.getRequestId()), ex);
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                                                             .statusMessage(ex.getMessage())
                                                             .statusAttributeException(ex).create());
                    }
                    onError(graph, context);
                }
            } catch (Exception ex) {
                final Optional<Throwable> possibleTemporaryException = determineIfTemporaryException(ex);
                if (possibleTemporaryException.isPresent()) {
                    context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TEMPORARY)
                            .statusMessage(possibleTemporaryException.get().getMessage())
                            .statusAttributeException(possibleTemporaryException.get()).create());
                } else {
                    logger.warn(String.format("Exception processing a Traversal on request [%s].", msg.getRequestId()), ex);
                    context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                            .statusMessage(ex.getMessage())
                            .statusAttributeException(ex).create());
                }
                onError(graph, context);
            } finally {
                timerContext.stop();
            }

            return null;
        });

        final Future<?> executionFuture = context.getGremlinExecutor().getExecutorService().submit(evalFuture);
        if (seto > 0) {
            // Schedule a timeout in the thread pool for future execution
            context.getScheduledExecutorService().schedule(() -> executionFuture.cancel(true), seto, TimeUnit.MILLISECONDS);
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

    protected void handleIterator(final Context context, final Iterator itty, final Graph graph) throws InterruptedException {
        final ChannelHandlerContext nettyContext = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        final MessageSerializer<?> serializer = nettyContext.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = nettyContext.channel().attr(StateKey.USE_BINARY).get();
        boolean warnOnce = false;

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            final Map<String, Object> attributes = generateStatusAttributes(nettyContext, msg, ResponseStatusCode.NO_CONTENT, itty, settings);

            // as there is nothing left to iterate if we are transaction managed then we should execute a
            // commit here before we send back a NO_CONTENT which implies success
            onTraversalSuccess(graph, context);
            context.writeAndFlush(ResponseMessage.build(msg)
                    .code(ResponseStatusCode.NO_CONTENT)
                    .statusAttributes(attributes)
                    .create());
            return;
        }

        // the batch size can be overridden by the request
        final int resultIterationBatchSize = (Integer) msg.optionalArgs(Tokens.ARGS_BATCH_SIZE)
                .orElse(settings.resultIterationBatchSize);
        List<Object> aggregate = new ArrayList<>(resultIterationBatchSize);

        // use an external control to manage the loop as opposed to just checking hasNext() in the while.  this
        // prevent situations where auto transactions create a new transaction after calls to commit() withing
        // the loop on calls to hasNext().
        boolean hasMore = itty.hasNext();

        while (hasMore) {
            if (Thread.interrupted()) throw new InterruptedException();

            // check if an implementation needs to force flush the aggregated results before the iteration batch
            // size is reached.
            final boolean forceFlush = isForceFlushed(nettyContext, msg, itty);

            // have to check the aggregate size because it is possible that the channel is not writeable (below)
            // so iterating next() if the message is not written and flushed would bump the aggregate size beyond
            // the expected resultIterationBatchSize.  Total serialization time for the response remains in
            // effect so if the client is "slow" it may simply timeout.
            //
            // there is a need to check hasNext() on the iterator because if the channel is not writeable the
            // previous pass through the while loop will have next()'d the iterator and if it is "done" then a
            // NoSuchElementException will raise its head. also need a check to ensure that this iteration doesn't
            // require a forced flush which can be forced by sub-classes.
            //
            // this could be placed inside the isWriteable() portion of the if-then below but it seems better to
            // allow iteration to continue into a batch if that is possible rather than just doing nothing at all
            // while waiting for the client to catch up
            if (aggregate.size() < resultIterationBatchSize && itty.hasNext() && !forceFlush) aggregate.add(itty.next());

            // Don't keep executor busy if client has already given up; there is no way to catch up if the channel is
            // not active, and hence we should break the loop.
            if (!nettyContext.channel().isActive()) {
                onError(graph, context);
                break;
            }

            // send back a page of results if batch size is met or if it's the end of the results being iterated.
            // also check writeability of the channel to prevent OOME for slow clients.
            //
            // clients might decide to close the Netty channel to the server with a CloseWebsocketFrame after errors
            // like CorruptedFrameException. On the server, although the channel gets closed, there might be some
            // executor threads waiting for watermark to clear which will not clear in these cases since client has
            // already given up on these requests. This leads to these executors waiting for the client to consume
            // results till the timeout. checking for isActive() should help prevent that.
            if (nettyContext.channel().isActive() && nettyContext.channel().isWritable()) {
                if (forceFlush || aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    final ResponseStatusCode code = itty.hasNext() ? ResponseStatusCode.PARTIAL_CONTENT : ResponseStatusCode.SUCCESS;

                    // serialize here because in sessionless requests the serialization must occur in the same
                    // thread as the eval.  as eval occurs in the GremlinExecutor there's no way to get back to the
                    // thread that processed the eval of the script so, we have to push serialization down into that
                    final Map<String, Object> metadata = generateResultMetaData(nettyContext, msg, code, itty, settings);
                    final Map<String, Object> statusAttrb = generateStatusAttributes(nettyContext, msg, code, itty, settings);
                    Frame frame = null;
                    try {
                        frame = makeFrame(context, msg, serializer, useBinary, aggregate, code,
                                          metadata, statusAttrb);
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        onError(graph, context);
                        break;
                    }

                    try {
                        // only need to reset the aggregation list if there's more stuff to write
                        if (itty.hasNext())
                            aggregate = new ArrayList<>(resultIterationBatchSize);
                        else {
                            // iteration and serialization are both complete which means this finished successfully. note that
                            // errors internal to script eval or timeout will rollback given GremlinServer's global configurations.
                            // local errors will get rolledback below because the exceptions aren't thrown in those cases to be
                            // caught by the GremlinExecutor for global rollback logic. this only needs to be committed if
                            // there are no more items to iterate and serialization is complete
                            onTraversalSuccess(graph, context);

                            // exit the result iteration loop as there are no more results left.  using this external control
                            // because of the above commit.  some graphs may open a new transaction on the call to
                            // hasNext()
                            hasMore = false;
                        }
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();
                        throw ex;
                    }

                    if (!itty.hasNext()) iterateComplete(nettyContext, msg, itty);

                    // the flush is called after the commit has potentially occurred.  in this way, if a commit was
                    // required then it will be 100% complete before the client receives it. the "frame" at this point
                    // should have completely detached objects from the transaction (i.e. serialization has occurred)
                    // so a new one should not be opened on the flush down the netty pipeline
                    context.writeAndFlush(code, frame);
                }
            } else {
                // don't keep triggering this warning over and over again for the same request
                if (!warnOnce) {
                    logger.warn("Pausing response writing as writeBufferHighWaterMark exceeded on {} - writing will continue once client has caught up", msg);
                    warnOnce = true;
                }

                // since the client is lagging we can hold here for a period of time for the client to catch up.
                // this isn't blocking the IO thread - just a worker.
                TimeUnit.MILLISECONDS.sleep(10);
            }
        }
    }
}
