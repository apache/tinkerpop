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
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.customizer.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.handler.GremlinResponseFrameEncoder;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.util.function.ThrowingConsumer;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import io.netty.channel.ChannelHandlerContext;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.SimpleBindings;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * A base {@link org.apache.tinkerpop.gremlin.server.OpProcessor} implementation that helps with operations that deal
 * with script evaluation functions.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractEvalOpProcessor extends AbstractOpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEvalOpProcessor.class);
    public static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    /**
     * The maximum number of parameters that can be passed on a script evaluation request.
     */
    public static final String CONFIG_MAX_PARAMETERS = "maxParameters";

    /**
     * Default number of parameters allowed on a script evaluation request.
     */
    public static final int DEFAULT_MAX_PARAMETERS = 16;

    protected int maxParameters = DEFAULT_MAX_PARAMETERS;

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

    protected AbstractEvalOpProcessor(final boolean manageTransactions) {
        super(manageTransactions);
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
            if (IteratorUtils.anyMatch(bindings.keySet().iterator(), k -> null == k || !(k instanceof String))) {
                final String msg = String.format("The [%s] message is using one or more invalid binding keys - they must be of type String and cannot be null", Tokens.OPS_EVAL);
                throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            final Set<String> badBindings = IteratorUtils.set(IteratorUtils.<String>filter(bindings.keySet().iterator(), INVALID_BINDINGS_KEYS::contains));
            if (!badBindings.isEmpty()) {
                final String msg = String.format("The [%s] message supplies one or more invalid parameters key of [%s] - these are reserved names.", Tokens.OPS_EVAL, badBindings);
                throw new OpProcessorException(msg, ResponseMessage.build(message).code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(msg).create());
            }

            // ignore control bindings that get passed in with the "#jsr223" prefix - those aren't used in compilation
            if (IteratorUtils.count(IteratorUtils.filter(bindings.keySet().iterator(), k -> !k.toString().startsWith("#jsr223"))) > maxParameters) {
                final String msg = String.format("The [%s] message contains %s bindings which is more than is allowed by the server %s configuration",
                        Tokens.OPS_EVAL, bindings.size(), maxParameters);
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
                    } catch (Exception ex) {
                        if (managedTransactionsForRequest) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);

                        // wrap up the exception and rethrow. the error will be written to the client by the evalFuture
                        // as it will completeExceptionally in the GremlinExecutor
                        throw new RuntimeException(ex);
                    }
                }).create();

        final CompletableFuture<Object> evalFuture = gremlinExecutor.eval(script, language, bindings, lifeCycle);

        evalFuture.handle((v, t) -> {
            timerContext.stop();

            if (t != null) {
                if (t instanceof OpProcessorException) {
                    context.writeAndFlush(((OpProcessorException) t).getResponseMessage());
                } else if (t instanceof TimedInterruptTimeoutException) {
                    // occurs when the TimedInterruptCustomizerProvider is in play
                    final String errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                    logger.warn(errorMessage);
                    context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                            .statusAttributeException(t).create());
                } else if (t instanceof org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException) {
                    // occurs when the TimedInterruptCustomizerProvider is in play
                    final String errorMessage = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", msg);
                    logger.warn(errorMessage);
                    context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                            .statusAttributeException(t).create());
                } else if (t instanceof TimeoutException) {
                    final String errorMessage = String.format("Script evaluation exceeded the configured threshold for request [%s]", msg);
                    logger.warn(errorMessage, t);
                    context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                            .statusMessage(t.getMessage())
                            .statusAttributeException(t).create());
                } else {
                    // try to trap the specific jvm error of "Method code too large!" to re-write it as something nicer,
                    // but only re-write if it's the only error because otherwise we might lose some other important
                    // information related to the failure. at this point, there hasn't been a scenario that has
                    // presented itself where the "Method code too large!" comes with other compilation errors so
                    // it seems that this message trumps other compilation errors to some reasonable degree that ends
                    // up being favorable for this problem
                    if (t instanceof MultipleCompilationErrorsException && t.getMessage().contains("Method code too large!") &&
                            ((MultipleCompilationErrorsException) t).getErrorCollector().getErrorCount() == 1) {
                        final String errorMessage = String.format("The Gremlin statement that was submitted exceed the maximum compilation size allowed by the JVM, please split it into multiple smaller statements - %s", trimMessage(msg));
                        logger.warn(errorMessage);
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION)
                                .statusMessage(errorMessage)
                                .statusAttributeException(t).create());
                    } else {
                        logger.warn(String.format("Exception processing a script on request [%s].", msg), t);
                        context.writeAndFlush(ResponseMessage.build(msg).code(ResponseStatusCode.SERVER_ERROR_SCRIPT_EVALUATION)
                                .statusMessage(t.getMessage())
                                .statusAttributeException(t).create());
                    }
                }
            }

            return null;
        });
    }

    /**
     * Used to decrease the size of a Gremlin script that triggered a "method code too large" exception so that it
     * doesn't log a massive text string nor return a large error message.
     */
    private RequestMessage trimMessage(final RequestMessage msg) {
        final RequestMessage trimmedMsg = RequestMessage.from(msg).create();
        if (trimmedMsg.getArgs().containsKey(Tokens.ARGS_GREMLIN))
            trimmedMsg.getArgs().put(Tokens.ARGS_GREMLIN, trimmedMsg.getArgs().get(Tokens.ARGS_GREMLIN).toString().substring(0, 1021) + "...");

        return trimmedMsg;
    }

    @FunctionalInterface
    public interface BindingSupplier {
        public Bindings get() throws OpProcessorException;
    }
}
