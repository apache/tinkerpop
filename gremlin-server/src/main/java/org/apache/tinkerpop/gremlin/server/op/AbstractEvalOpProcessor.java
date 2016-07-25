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
package org.apache.tinkerpop.gremlin.server.op;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.GremlinResponseFrameEncoder;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A base {@link org.apache.tinkerpop.gremlin.server.OpProcessor} implementation that helps with operations that deal
 * with script evaluation functions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractEvalOpProcessor implements OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEvalOpProcessor.class);
    public static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    /**
     * Captures the "error" count as a reportable metric for Gremlin Server.
     *
     * @deprecated As of release 3.1.1-incubating, not replaced. Direct usage is discouraged with sub-classes as
     * error counts are captured more globally for error messages written down the pipeline to
     * {@link GremlinResponseFrameEncoder}.
     */
    @Deprecated
    static final Meter errorMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "errors"));

    /**
     * Regex for validating that binding variables.
     *
     * @deprecated As of release 3.1.2-incubating, not replaced. This {@code Pattern} is not used internally.
     * Deprecated rather than just removing as it's possible that someone else might be using it when developing
     * custom {@link OpProcessor} implementations.
     */
    @Deprecated
    protected static final Pattern validBindingName = Pattern.compile("[a-zA-Z$_][a-zA-Z0-9$_]*");

    /**
     * This may or may not be the full set of invalid binding keys.  It is dependent on the static imports made to
     * Gremlin Server.  This should get rid of the worst offenders though and provide a good message back to the
     * calling client.
     * <p/>
     * Use of {@code toUpperCase()} on the accessor values of {@link T} solves an issue where the {@code ScriptEngine}
     * ignores private scope on {@link T} and imports static fields.
     */
    protected static final Set<String> INVALID_BINDINGS_KEYS = new HashSet<>();

    static {
        INVALID_BINDINGS_KEYS.addAll(Arrays.asList(
                T.id.name(), T.key.name(),
                T.label.name(), T.value.name(),
                T.id.getAccessor(), T.key.getAccessor(),
                T.label.getAccessor(), T.value.getAccessor(),
                T.id.getAccessor().toUpperCase(), T.key.getAccessor().toUpperCase(),
                T.label.getAccessor().toUpperCase(), T.value.getAccessor().toUpperCase()));

        for (Column enumItem : Column.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Order enumItem : Order.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Operator enumItem : Operator.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Scope enumItem : Scope.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }

        for (Pop enumItem : Pop.values()) {
            INVALID_BINDINGS_KEYS.add(enumItem.name());
        }
    }

    protected final boolean manageTransactions;

    protected AbstractEvalOpProcessor(final boolean manageTransactions) {
        this.manageTransactions = manageTransactions;
    }

    /**
     * Provides an operation for evaluating a Gremlin script.
     */
    public abstract ThrowingConsumer<Context> getEvalOp();

    /**
     * A sub-class may have additional "ops" that it will service.  Calls to {@link #select(Context)} that are not
     * handled will be passed to this method to see if the sub-class can service the requested op code.
     */
    public abstract Optional<ThrowingConsumer<Context>> selectOther(final RequestMessage requestMessage) throws OpProcessorException;

    @Override
    public ThrowingConsumer<Context> select(final Context ctx) throws OpProcessorException {
        final RequestMessage message = ctx.getRequestMessage();
        logger.debug("Selecting processor for RequestMessage {}", message);

        final ThrowingConsumer<Context> op;
        switch (message.getOp()) {
            case Tokens.OPS_EVAL:
                op = validateEvalMessage(message).orElse(getEvalOp());
                break;
            case Tokens.OPS_INVALID:
                final String msgInvalid = String.format("Message could not be parsed.  Check the format of the request. [%s]", message);
                throw new OpProcessorException(msgInvalid, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgInvalid).create());
            default:
                op = selectOther(message).orElseThrow(() -> {
                    final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                    return new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).statusMessage(msgDefault).create());
                });
        }

        return op;
    }

    protected Optional<ThrowingConsumer<Context>> validateEvalMessage(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
        }

        if (message.optionalArgs(Tokens.ARGS_BINDINGS).isPresent()) {
            final Map bindings = (Map) message.getArgs().get(Tokens.ARGS_BINDINGS);
            if (bindings.keySet().stream().anyMatch(k -> null == k || !(k instanceof String))) {
                final String msg = String.format("The [%s] message is using one or more invalid binding keys - they must be of type String and cannot be null", Tokens.OPS_EVAL);
                throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            final Set<String> badBindings = IteratorUtils.set(IteratorUtils.<String>filter(bindings.keySet().iterator(), INVALID_BINDINGS_KEYS::contains));
            if (!badBindings.isEmpty()) {
                final String msg = String.format("The [%s] message supplies one or more invalid parameters key of [%s] - these are reserved names.", Tokens.OPS_EVAL, badBindings);
                throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }
        }

        return Optional.empty();
    }

    /**
     * A generalized implementation of the "eval" operation.  It handles script evaluation and iteration of results
     * so as to write {@link ResponseMessage} objects down the Netty pipeline.  It also handles script timeouts,
     * iteration timeouts, metrics and building bindings.  Note that result iteration is delegated to the
     * {@link #handleIterator} method, so those extending this class could override that method for better control
     * over result iteration.
     *
     * @param context The current Gremlin Server {@link Context}
     * @param gremlinExecutorSupplier A function that returns the {@link GremlinExecutor} to use in executing the
     *                                script evaluation.
     * @param bindingsSupplier A function that returns the {@link Bindings} to provide to the
     *                         {@link GremlinExecutor#eval} method.
     */
    protected void evalOpInternal(final Context context, final Supplier<GremlinExecutor> gremlinExecutorSupplier,
                                  final BindingSupplier bindingsSupplier) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        final Settings settings = context.getSettings();

        final Map<String, Object> args = msg.getArgs();

        final String script = (String) args.get(Tokens.ARGS_GREMLIN);
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : null;
        final Bindings bindings = new SimpleBindings();

        // sessionless requests are always transaction managed, but in-session requests are configurable.
        final boolean managedTransactionsForRequest = manageTransactions ?
                true : (Boolean) args.getOrDefault(Tokens.ARGS_MANAGE_TRANSACTION, false);

        // timeout override
        final long seto = args.containsKey(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT) ?
                Long.parseLong(args.get(Tokens.ARGS_SCRIPT_EVAL_TIMEOUT).toString()) : settings.scriptEvaluationTimeout;

        final GremlinExecutor.LifeCycle lifeCycle = GremlinExecutor.LifeCycle.build()
                .scriptEvaluationTimeoutOverride(seto)
                .afterFailure((b,t) -> {
                    if (managedTransactionsForRequest) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                })
                .beforeEval(b -> {
                    try {
                        b.putAll(bindingsSupplier.get());
                    } catch (OpProcessorException ope) {
                        // this should bubble up in the GremlinExecutor properly as the RuntimeException will be
                        // unwrapped and the root cause thrown
                        throw new RuntimeException(ope);
                    }
                })
                .withResult(o -> {
                    final Iterator itty = IteratorUtils.asIterator(o);

                    logger.debug("Preparing to iterate results from - {} - in thread [{}]", msg, Thread.currentThread().getName());

                    try {
                        handleIterator(context, itty);
                    } catch (TimeoutException ex) {
                        final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg, ex.getMessage());
                        logger.warn(errorMessage);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
                        if (managedTransactionsForRequest) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                    } catch (InterruptedException ex) {
                        logger.warn(String.format("Interruption during result iteration on request [%s].", msg), ex);
                        final String exceptionMsg = ex.getMessage();
                        final String err = "Interruption of result iteration" + (null == exceptionMsg || exceptionMsg.isEmpty() ? "" : " - " + exceptionMsg);
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(err).create());
                        if (managedTransactionsForRequest) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                    } catch (Exception ex) {
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), ex);
                        final String err = ex.getMessage();
                        ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR)
                                .statusMessage(null == err || err.isEmpty() ? ex.getClass().getSimpleName() : err).create());
                        if (managedTransactionsForRequest) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                    }
                }).create();

        final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings, lifeCycle);

        evalFuture.handle((v, t) -> {
            timerContext.stop();

            if (t != null) {
                if (t instanceof OpProcessorException) {
                    ctx.writeAndFlush(((OpProcessorException) t).getResponseMessage());
                } else if (t instanceof TimedInterruptTimeoutException) {
                    // occurs when the TimedInterruptCustomizerProvider is in play
                    final String errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                    logger.warn(errorMessage);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider").create());
                } else if (t instanceof TimeoutException) {
                    final String errorMessage = String.format("Response evaluation exceeded the configured threshold for request [%s] - %s", msg, t.getMessage());
                    logger.warn(errorMessage, t);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(t.getMessage()).create());
                } else {
                    logger.warn(String.format("Exception processing a script on request [%s].", msg), t);
                    ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION).statusMessage(t.getMessage()).create());
                }
            }

            return null;
        });
    }

    /**
     * Called by {@link #evalOpInternal} when iterating a result set. Implementers should respect the
     * {@link Settings#serializedResponseTimeout} configuration and break the serialization process if
     * it begins to take too long to do so, throwing a {@link java.util.concurrent.TimeoutException} in such
     * cases.
     *
     * @param context The Gremlin Server {@link Context} object containing settings, request message, etc.
     * @param itty The result to iterator
     * @throws TimeoutException if the time taken to serialize the entire result set exceeds the allowable time.
     */
    protected void handleIterator(final Context context, final Iterator itty) throws TimeoutException, InterruptedException {
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        final MessageSerializer serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = ctx.channel().attr(StateKey.USE_BINARY).get();
        boolean warnOnce = false;

        // sessionless requests are always transaction managed, but in-session requests are configurable.
        final boolean managedTransactionsForRequest = manageTransactions ?
                true : (Boolean) msg.getArgs().getOrDefault(Tokens.ARGS_MANAGE_TRANSACTION, false);

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            // as there is nothing left to iterate if we are transaction managed then we should execute a
            // commit here before we send back a NO_CONTENT which implies success
            if (managedTransactionsForRequest) attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);
            ctx.writeAndFlush(ResponseMessage.build(msg)
                    .code(ResponseStatusCode.NO_CONTENT)
                    .create());
            return;
        }

        // timer for the total serialization time
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

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

            // have to check the aggregate size because it is possible that the channel is not writeable (below)
            // so iterating next() if the message is not written and flushed would bump the aggregate size beyond
            // the expected resultIterationBatchSize.  Total serialization time for the response remains in
            // effect so if the client is "slow" it may simply timeout.
            //
            // there is a need to check hasNext() on the iterator because if the channel is not writeable the
            // previous pass through the while loop will have next()'d the iterator and if it is "done" then a
            // NoSuchElementException will raise its head.
            //
            // this could be placed inside the isWriteable() portion of the if-then below but it seems better to
            // allow iteration to continue into a batch if that is possible rather than just doing nothing at all
            // while waiting for the client to catch up
            if (aggregate.size() < resultIterationBatchSize && itty.hasNext()) aggregate.add(itty.next());

            // send back a page of results if batch size is met or if it's the end of the results being iterated.
            // also check writeability of the channel to prevent OOME for slow clients.
            if (ctx.channel().isWritable()) {

                if (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    final ResponseStatusCode code = itty.hasNext() ? ResponseStatusCode.PARTIAL_CONTENT : ResponseStatusCode.SUCCESS;

                    // serialize here because in sessionless requests the serialization must occur in the same
                    // thread as the eval.  as eval occurs in the GremlinExecutor there's no way to get back to the
                    // thread that processed the eval of the script so, we have to push serialization down into that
                    Frame frame = null;
                    try {
                        frame = makeFrame(ctx, msg, serializer, useBinary, aggregate, code);
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        if (manageTransactions) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
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
                            if (managedTransactionsForRequest) attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);

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

                    // the flush is called after the commit has potentially occurred.  in this way, if a commit was
                    // required then it will be 100% complete before the client receives it. the "frame" at this point
                    // should have completely detached objects from the transaction (i.e. serialization has occurred)
                    // so a new one should not be opened on the flush down the netty pipeline
                    ctx.writeAndFlush(frame);
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

            stopWatch.split();
            if (stopWatch.getSplitTime() > settings.serializedResponseTimeout) {
                final String timeoutMsg = String.format("Serialization of the entire response exceeded the 'serializeResponseTimeout' setting %s",
                        warnOnce ? "[Gremlin Server paused writes to client as messages were not being consumed quickly enough]" : "");
                throw new TimeoutException(timeoutMsg.trim());
            }

            stopWatch.unsplit();
        }

        stopWatch.stop();
    }

    private static Frame makeFrame(final ChannelHandlerContext ctx, final RequestMessage msg,
                                   final MessageSerializer serializer, final boolean useBinary, List<Object> aggregate,
                                   final ResponseStatusCode code) throws Exception {
        try {
            if (useBinary) {
                return new Frame(serializer.serializeResponseAsBinary(ResponseMessage.build(msg)
                                .code(code)
                                .result(aggregate).create(), ctx.alloc()));
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                return new Frame(textSerializer.serializeResponseAsString(ResponseMessage.build(msg)
                                .code(code)
                                .result(aggregate).create()));
            }
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", aggregate, msg.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s",
                    ex.getCause() != null ? ex.getCause().getMessage() : ex.getMessage());
            final ResponseMessage error = ResponseMessage.build(msg.getRequestId())
                    .statusMessage(errorMessage)
                    .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create();
            ctx.writeAndFlush(error);
            throw ex;
        }
    }

    private static void attemptCommit(final RequestMessage msg, final GraphManager graphManager, final boolean strict) {
        if (strict) {
            // validations should have already been performed in StandardOpProcessor, but a failure in bindings maker
            // at the time of the eval might raise through here at which point the validation didn't yet happen. better
            // to just check again
            final boolean hasRebindings = msg.getArgs().containsKey(Tokens.ARGS_REBINDINGS);
            final String rebindingOrAliasParameter = hasRebindings ? Tokens.ARGS_REBINDINGS : Tokens.ARGS_ALIASES;
            if (msg.getArgs().containsKey(rebindingOrAliasParameter)) {
                final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(rebindingOrAliasParameter);
                graphManager.commit(new HashSet<>(aliases.values()));
            } else {
                graphManager.commitAll();
            }
        } else {
            graphManager.commitAll();
        }
    }

    private static void attemptRollback(final RequestMessage msg, final GraphManager graphManager, final boolean strict) {
        if (strict) {
            // validations should have already been performed in StandardOpProcessor, but a failure in bindings maker
            // at the time of the eval might raise through here at which point the validation didn't yet happen. better
            // to just check again
            final boolean hasRebindings = msg.getArgs().containsKey(Tokens.ARGS_REBINDINGS);
            final String rebindingOrAliasParameter = hasRebindings ? Tokens.ARGS_REBINDINGS : Tokens.ARGS_ALIASES;
            if (msg.getArgs().containsKey(rebindingOrAliasParameter)) {
                final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(rebindingOrAliasParameter);
                graphManager.rollback(new HashSet<>(aliases.values()));
            } else {
                graphManager.rollbackAll();
            }
        } else {
            graphManager.rollbackAll();
        }
    }

    @FunctionalInterface
    public interface BindingSupplier {
        public Bindings get() throws OpProcessorException;
    }
}
