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
import org.apache.tinkerpop.gremlin.process.T;
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
import java.util.concurrent.ExecutorService;
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
    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    /**
     * This may or may not be the full set of invalid binding keys.  It is dependent on the static imports made to
     * Gremlin Server.  This should get rid of the worst offenders though and provide a good message back to the
     * calling client.
     */
    private static final List<String> invalidBindingsKeys = Arrays.asList(
            T.id.getAccessor(), T.key.getAccessor(),
            T.label.getAccessor(), T.value.getAccessor());
    private static final String invalidBindingKeysJoined = String.join(",", invalidBindingsKeys);

    /**
     * Provides an operation for evaluating a Gremlin script.
     */
    public abstract ThrowingConsumer<Context> getEvalOp();

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
                final String msgDefault = String.format("Message with op code [%s] is not recognized.", message.getOp());
                throw new OpProcessorException(msgDefault, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_MALFORMED_REQUEST).result(msgDefault).create());
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
     * iteration timeouts, metrics and building bindings.
     *
     * @param context The current Gremlin Server {@link Context}
     * @param gremlinExecutorSupplier A function that returns the {@link GremlinExecutor} to use in executing the
     *                                script evaluation.
     * @param bindingsSupplier A function that returns the {@link Bindings} to provide to the
     *                         {@link GremlinExecutor#eval} method.
     */
    public static void evalOp(final Context context, final Supplier<GremlinExecutor> gremlinExecutorSupplier,
                              final Supplier<Bindings> bindingsSupplier) throws OpProcessorException {
        final Timer.Context timerContext = evalOpTimer.time();
        final ChannelHandlerContext ctx = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        final GremlinExecutor gremlinExecutor = gremlinExecutorSupplier.get();
        final ExecutorService executor = gremlinExecutor.getExecutorService();

        final String script = (String) msg.getArgs().get(Tokens.ARGS_GREMLIN);
        final Optional<String> language = Optional.ofNullable((String) msg.getArgs().get(Tokens.ARGS_LANGUAGE));
        final Bindings bindings = bindingsSupplier.get();

        final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings);
        evalFuture.handle((v, t) -> timerContext.stop());
        evalFuture.exceptionally(se -> {
            logger.warn(String.format("Exception processing a script on request [%s].", msg), se);
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION).statusMessage(se.getMessage()).create());
            return null;
        });

        final CompletableFuture<Void> iterationFuture = evalFuture.thenAcceptAsync(o -> {
            final Iterator itty = IteratorUtils.convertToIterator(o);
            // the batch size can be overridden by the request
            final int resultIterationBatchSize = (Integer) msg.optionalArgs(Tokens.ARGS_BATCH_SIZE).orElse(settings.resultIterationBatchSize);

            // timer for the total serialization time
            final StopWatch stopWatch = new StopWatch();

            logger.debug("Preparing to iterate results from - {} - in thread [{}]", msg, Thread.currentThread().getName());

            stopWatch.start();

            List<Object> aggregate = new ArrayList<>(resultIterationBatchSize);
            while (itty.hasNext()) {
                aggregate.add(itty.next());

                // send back a page of results if batch size is met or if it's the end of the results being
                // iterated
                if (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    ctx.writeAndFlush(ResponseMessage.build(msg)
                            .code(ResponseStatusCode.SUCCESS)
                            .result(aggregate).create());
                    aggregate = new ArrayList<>(resultIterationBatchSize);
                }

                stopWatch.split();
                if (stopWatch.getSplitTime() > settings.serializedResponseTimeout)
                    throw new RuntimeException(new TimeoutException("Serialization of the entire response exceeded the serializeResponseTimeout setting"));

                stopWatch.unsplit();
            }

            stopWatch.stop();
        }, executor);

        iterationFuture.handleAsync((r, ex) -> {
            // iteration has completed - if there was an exception then write it
            if (ex != null) {
                final String errorMessage = String.format("Response iteration and serialization exceeded the configured threshold for request [%s] - %s", msg, ex.getCause().getMessage());
                logger.warn(errorMessage);
                ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT).statusMessage(errorMessage).create());
            }

            // either way - terminate the request
            ctx.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SUCCESS_TERMINATOR).create());
            return null;
        }, executor);
    }
}
