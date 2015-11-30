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

import com.codahale.metrics.Timer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

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
     * This may or may not be the full set of invalid binding keys.  It is dependent on the static imports made to
     * Gremlin Server.  This should get rid of the worst offenders though and provide a good message back to the
     * calling client.
     */
    private static final List<String> invalidBindingsKeys = Arrays.asList(
            T.id.getAccessor(), T.key.getAccessor(),
            T.label.getAccessor(), T.value.getAccessor());
    private static final String invalidBindingKeysJoined = String.join(",", invalidBindingsKeys);

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
                throw new OpProcessorException(msgInvalid, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgInvalid).create());
            default:
                op = selectOther(message).orElseThrow(() -> {
                    final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                    return new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgDefault).create());
                });
        }

        return op;
    }

    protected Optional<ThrowingConsumer<Context>> validateEvalMessage(final RequestMessage message) throws OpProcessorException {
        if (!message.optionalArgs(Tokens.ARGS_GREMLIN).isPresent()) {
            final String msg = String.format("A message with an [%s] op code requires a [%s] argument.", Tokens.OPS_EVAL, Tokens.ARGS_GREMLIN);
            throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).create());
        }

        if (message.optionalArgs(Tokens.ARGS_BINDINGS).isPresent()) {
            final Map<String, Object> bindings = (Map<String, Object>) message.getArgs().get(Tokens.ARGS_BINDINGS);
            if (bindings.keySet().stream().anyMatch(invalidBindingsKeys::contains)) {
                final String msg = String.format("The [%s] message is using at least one of the invalid binding key of [%s]. It conflicts with standard static imports to Gremlin Server.", Tokens.OPS_EVAL, invalidBindingKeysJoined);
                throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).result(msg).create());
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

        final Map<String, Object> args = msg.getArgs();

        final String script = (String) args.get(Tokens.ARGS_GREMLIN);
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : null;
        final Bindings bindings = bindingsSupplier.get();

        final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings, null, o -> {
            final Iterator itty = IteratorUtils.asIterator(o);

            logger.debug("Preparing to iterate results from - {} - in thread [{}]", msg, Thread.currentThread().getName());

            try {
                handleIterator(context, itty);
            } catch (TimeoutException ex) {
                final String errorMessage = String.format("Response iteration exceeded the configured threshold for request [%s] - %s", msg, ex.getMessage());
                logger.warn(errorMessage);
                ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
            } catch (Exception ex) {
                logger.warn(String.format("Exception processing a script on request [%s].", msg), ex);
                ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR).statusMessage(ex.getMessage()).create());
            }
        });

        evalFuture.handle((v, t) -> {
            timerContext.stop();

            if (t != null) {
                if (t instanceof TimeoutException) {
                    final String errorMessage = String.format("Response evaluation exceeded the configured threshold for request [%s] - %s", msg, t.getMessage());
                    logger.warn(errorMessage);
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
        boolean warnOnce = false;

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            // as there is nothing left to iterate if we are transaction managed then we should execute a
            // commit here before we send back a NO_CONTENT which implies success
            if (manageTransactions) context.getGraphManager().commitAll();
            ctx.writeAndFlush(ResponseMessage.build(msg)
                    .code(ResponseStatusCode.NO_CONTENT)
                    .create());
        }

        // timer for the total serialization time
        final StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        // the batch size can be overridden by the request
        final int resultIterationBatchSize = (Integer) msg.optionalArgs(Tokens.ARGS_BATCH_SIZE)
                .orElse(settings.resultIterationBatchSize);
        List<Object> aggregate = new ArrayList<>(resultIterationBatchSize);
        while (itty.hasNext()) {
            if (Thread.interrupted()) throw new InterruptedException();

            // have to check the aggregate size because it is possible that the channel is not writeable (below)
            // so iterating next() if the message is not written and flushed would bump the aggregate size beyond
            // the expected resultIterationBatchSize.  Total serialization time for the response remains in
            // effect so if the client is "slow" it may simply timeout.
            if (aggregate.size() < resultIterationBatchSize) aggregate.add(itty.next());

            // if there's no more items in the iterator then we've aggregated everything and are thus ready to
            // commit stuff if transaction management is on.  exceptions should bubble up and be handle in the normal
            // manner of things.  a final SUCCESS message will not have been sent (below) and we ship back an error.
            // if transaction management is not enabled, then returning SUCCESS below is OK as this is a different
            // usage context.  without transaction management enabled, the user is responsible for maintaining
            // the transaction and will want a SUCCESS to know their eval and iteration was ok.  they would then
            // potentially have a failure on commit on the next request.
            if (!itty.hasNext() && manageTransactions)
                context.getGraphManager().commitAll();

            // send back a page of results if batch size is met or if it's the end of the results being iterated.
            // also check writeability of the channel to prevent OOME for slow clients.
            if (ctx.channel().isWritable()) {
                if  (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    final ResponseStatusCode code = itty.hasNext() ? ResponseStatusCode.PARTIAL_CONTENT : ResponseStatusCode.SUCCESS;
                    ctx.writeAndFlush(ResponseMessage.build(msg)
                            .code(code)
                            .result(aggregate).create());

                    aggregate = new ArrayList<>(resultIterationBatchSize);
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
                final String timeoutMsg = String.format("Serialization of the entire response exceeded the serializeResponseTimeout setting %s",
                        warnOnce ? "[Gremlin Server paused writes to client as messages were not being consumed quickly enough]" : "");
                throw new TimeoutException(timeoutMsg.trim());
            }

            stopWatch.unsplit();
        }

        stopWatch.stop();
    }

    @FunctionalInterface
    public interface BindingSupplier {
        public Bindings get() throws OpProcessorException;
    }
}
