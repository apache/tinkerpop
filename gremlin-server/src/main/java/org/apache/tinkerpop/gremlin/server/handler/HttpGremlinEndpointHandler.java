/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.server.handler;

import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.language.grammar.GremlinParserException;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Scope;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.ProcessingException;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.util.GremlinError;
import org.apache.tinkerpop.gremlin.server.util.MetricManager;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;
import org.apache.tinkerpop.gremlin.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.util.ser.GraphBinaryMessageSerializerV4;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static com.codahale.metrics.MetricRegistry.name;
import static io.netty.handler.codec.http.HttpHeaderNames.ACCEPT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_ENCODING;
import static io.netty.handler.codec.http.HttpHeaderValues.DEFLATE;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.FINISHED;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.FINISHING;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.NOT_STARTED;
import static org.apache.tinkerpop.gremlin.server.handler.HttpGremlinEndpointHandler.RequestState.STREAMING;
import static org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil.sendHttpResponse;
import static org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil.sendLastHttpContent;
import static org.apache.tinkerpop.gremlin.server.handler.HttpHandlerUtil.writeError;

/**
 * Handler that processes RequestMessage. This handler will attempt to execute the query and stream the results back
 * in HTTP chunks to the client.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@ChannelHandler.Sharable
public class HttpGremlinEndpointHandler extends SimpleChannelInboundHandler<RequestMessage> {
    private static final Logger logger = LoggerFactory.getLogger(HttpGremlinEndpointHandler.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    private static final Timer evalOpTimer = MetricManager.INSTANCE.getTimer(name(GremlinServer.class, "op", "eval"));

    /**
     * Length of time to pause writes in milliseconds when the high watermark is exceeded.
     */
    public static final long WRITE_PAUSE_TIME_MS = 10;

    /**
     * Tracks the rate of pause to writes when the high watermark is exceeded.
     */
    public static final Meter writePausesMeter = MetricManager.INSTANCE.getMeter(name(GremlinServer.class, "channels", "write-pauses"));

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

    private final GremlinExecutor gremlinExecutor;
    private final GraphManager graphManager;
    private final Settings settings;
    private final TransactionManager transactionManager;

    public HttpGremlinEndpointHandler(final GremlinExecutor gremlinExecutor,
                                      final GraphManager graphManager,
                                      final Settings settings,
                                      final TransactionManager transactionManager) {
        this.gremlinExecutor = gremlinExecutor;
        this.graphManager = graphManager;
        this.settings = settings;
        this.transactionManager = transactionManager;
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final RequestMessage requestMessage) {
        ctx.channel().attr(StateKey.HTTP_RESPONSE_SENT).set(false);
        final Pair<String, MessageSerializer<?>> serializer = ctx.channel().attr(StateKey.SERIALIZER).get();

        final Context requestCtx = new Context(requestMessage, ctx, settings, graphManager, gremlinExecutor,
                gremlinExecutor.getScheduledExecutorService(), NOT_STARTED);

        final Timer.Context timerContext = evalOpTimer.time();
        // timeout override - handle both deprecated and newly named configuration. earlier logic should prevent
        // both configurations from being submitted at the same time
        final Long timeoutMs = requestMessage.getField(Tokens.TIMEOUT_MS);
        final long seto = (null != timeoutMs) ? timeoutMs : requestCtx.getSettings().getEvaluationTimeout();

        final FutureTask<Void> evalFuture = new FutureTask<>(() -> {
            requestCtx.setStartedResponse();

            try {
                logger.debug("Processing request containing script [{}] and bindings of [{}] on {}",
                        requestMessage.getFieldOrDefault(Tokens.ARGS_GREMLIN, ""),
                        requestMessage.getFieldOrDefault(Tokens.ARGS_BINDINGS, Collections.emptyMap()),
                        Thread.currentThread().getName());
                if (settings.enableAuditLog) {
                    AuthenticatedUser user = ctx.channel().attr(StateKey.AUTHENTICATED_USER).get();
                    if (null == user) {    // This is expected when using the AllowAllAuthenticator
                        user = AuthenticatedUser.ANONYMOUS_USER;
                    }
                    String address = ctx.channel().remoteAddress().toString();
                    if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
                    auditLogger.info("User {} with address {} requested: {}", user.getName(), address,
                            requestMessage.getGremlin());
                }

                // These guards prevent any obvious failures from returning 200 OK early by detecting them here and
                // throwing before any other processing starts so the user gets a better error code.
                final String txId = requestCtx.getTransactionId();
                final String gremlin = requestMessage.getGremlin();
                if (isTransactionBegin(gremlin)) {
                    // If this is a begin transaction request then we need to create the Transaction ID first since the
                    // dual-transmission expectation means the response header below should contain it.

                    // This prevents accidentally re-opening the underlying transaction.
                    if (txId != null) throw new ProcessingException(GremlinError.beginHasTransactionId());

                    doBegin(requestCtx);
                } else if (txId != null) {
                    // This check makes sure that the underlying Graph is already open to stop a closed transaction
                    // from re-opening due to the default autostart nature of transactions. This occurs in cases where a
                    // transactional traversal is submitted after a commit/rollback.
                    final Graph g = graphManager.getTraversalSource(requestMessage.getField(Tokens.ARGS_G)).getGraph();
                    if ((!g.tx().isOpen())) {
                        throw new ProcessingException(GremlinError.transactionNotFound(txId));
                    }
                } else if ((txId == null) && (isTransactionCommit(gremlin) || isTransactionRollback(gremlin))) {
                    // Logically, commit/rollback should only be allowed on a transactional request.
                    throw new ProcessingException(GremlinError.transactionalControlRequiresTransaction());
                }

                // Send back the 200 OK response header here since the response is always chunk transfer encoded. Any
                // failures that follow this will show up in the response body instead.
                sendHttpResponse(ctx, OK, createResponseHeaders(ctx, serializer, requestCtx).toArray(CharSequence[]::new));
                sendHttpContents(ctx, requestCtx);
                sendLastHttpContent(ctx, HttpResponseStatus.OK, "");
            } catch (Throwable t) {
                writeError(requestCtx, formErrorResponseMessage(t, requestMessage), serializer.getValue1());
            } finally {
                timerContext.stop();

                // There is a race condition that this query may have finished before the timeoutFuture was created,
                // though this is very unlikely. This is handled in the settor, if this has already been grabbed.
                // If we passed this point and the setter hasn't been called, it will cancel the timeoutFuture inside
                // the setter to compensate.
                final ScheduledFuture<?> timeoutFuture = requestCtx.getTimeoutExecutor();
                if (null != timeoutFuture)
                    timeoutFuture.cancel(true);
            }

            return null;
        });

        try {
            final boolean isBeginTransactionRequest = isTransactionBegin(requestMessage.getGremlin());
            final Future<?> executionFuture = ((requestCtx.getTransactionId() != null) && !isBeginTransactionRequest) ?
                    transactionManager.get(requestCtx.getTransactionId()).get().submit(evalFuture) :
                    requestCtx.getGremlinExecutor().getExecutorService().submit(evalFuture);
            if (seto > 0) {
                // Schedule a timeout in the thread pool for future execution
                requestCtx.setTimeoutExecutor(requestCtx.getScheduledExecutorService().schedule(() -> {
                    executionFuture.cancel(true);
                    if (!requestCtx.getStartedResponse()) {
                        writeError(requestCtx, GremlinError.timeout(requestMessage), serializer.getValue1());
                    }
                }, seto, TimeUnit.MILLISECONDS));
            }
        } catch (RejectedExecutionException ree) {
            writeError(requestCtx, GremlinError.rateLimiting(), serializer.getValue1());
        } catch (NoSuchElementException nsee) {
            writeError(requestCtx, GremlinError.transactionNotFound(requestCtx.getTransactionId()), serializer.getValue1());
        }
    }

    private List<CharSequence> createResponseHeaders(final ChannelHandlerContext ctx,
                                                     final Pair<String, MessageSerializer<?>> serializer,
                                                     final Context requestCtx) {
        final List<CharSequence> headers = new ArrayList<>();
        headers.add(HttpHeaderNames.CONTENT_TYPE);
        headers.add(serializer.getValue0());
        if (acceptsDeflateEncoding(ctx.attr(StateKey.REQUEST_HEADERS).get().getAll(ACCEPT_ENCODING))) {
            headers.add(CONTENT_ENCODING);
            headers.add(DEFLATE);
        }
        if (requestCtx.getTransactionId() != null) {
            headers.add(Tokens.Headers.TRANSACTION_ID);
            headers.add(requestCtx.getTransactionId());
        }
        return headers;
    }

    private void sendHttpContents(final ChannelHandlerContext ctx, final Context requestContext) throws Exception {
        final Pair<String, MessageSerializer<?>> serializer = ctx.channel().attr(StateKey.SERIALIZER).get();
        final RequestMessage request = requestContext.getRequestMessage();
        final String txId = requestContext.getTransactionId();
        final Optional<UnmanagedTransaction> transaction = transactionManager.get(txId);

        // Early guard against fake or incorrect transaction IDs.
        if ((txId != null) && transaction.isEmpty()) throw new ProcessingException(GremlinError.transactionNotFound(txId));

        if (isTransactionBegin(request.getGremlin())) {
            runBegin(requestContext, transaction.get(), serializer);
        } else if (isTransactionCommit(request.getGremlin())) {
            handleGraphOp(requestContext, txId, Transaction::commit, serializer);
        } else if (isTransactionRollback(requestContext.getRequestMessage().getGremlin())) {
            handleGraphOp(requestContext, txId, Transaction::rollback, serializer);
        } else {
            // Both transactional and non-transactional traversals follow this path for response chunking.
            iterateScriptEvalResult(requestContext, serializer.getValue1(), request);
        }
    }

    private GremlinError formErrorResponseMessage(Throwable t, RequestMessage requestMessage) {
        if (t instanceof UndeclaredThrowableException) t = t.getCause();

        // if any exception in the chain is TemporaryException or Failure then we should respond with the
        // right error code so that the client knows to retry
        final Optional<Throwable> possibleSpecialException = determineIfSpecialException(t);
        if (possibleSpecialException.isPresent()) {
            final Throwable special = possibleSpecialException.get();
            if (special instanceof TemporaryException) {
                return GremlinError.temporary(special);
            }
            if (special instanceof Failure) {
                return GremlinError.failStep((Failure) special);
            }
            return GremlinError.general(special);
        }
        if (t instanceof ProcessingException) {
            return ((ProcessingException) t).getError();
        }
        t = ExceptionHelper.getRootCause(t);

        if (t instanceof TooLongFrameException) {
            return GremlinError.longFrame(t);
        }
        if (t instanceof InterruptedException || t instanceof TraversalInterruptedException) {
            return GremlinError.timeout(requestMessage);
        }
        if (t instanceof TimedInterruptTimeoutException) {
            // occurs when the TimedInterruptCustomizerProvider is in play
            logger.warn(String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider", requestMessage));
            return GremlinError.timedInterruptTimeout();
        }
        if (t instanceof TimeoutException) {
            logger.warn(String.format("Script evaluation exceeded the configured threshold for request [%s]", requestMessage));
            return GremlinError.timeout(requestMessage);
        }
        if (t instanceof MultipleCompilationErrorsException && t.getMessage().contains("Method too large") &&
                ((MultipleCompilationErrorsException) t).getErrorCollector().getErrorCount() == 1) {
            final GremlinError error = GremlinError.longRequest(requestMessage);
            logger.warn(error.getMessage());
            return error;
        }
        if (t instanceof GremlinParserException) {
            return GremlinError.parsing((GremlinParserException) t);
        }

        logger.warn(String.format("Exception processing request [%s].", requestMessage));
        return GremlinError.general(t);
    }

    private void iterateScriptEvalResult(final Context context, MessageSerializer<?> serializer, final RequestMessage message)
            throws ProcessingException, InterruptedException, ScriptException {
        if (message.optionalField(Tokens.ARGS_BINDINGS).isPresent()) {
            final Map bindings = (Map) message.getFields().get(Tokens.ARGS_BINDINGS);
            if (IteratorUtils.anyMatch(bindings.keySet().iterator(), k -> null == k || !(k instanceof String))) {
                throw new ProcessingException(GremlinError.binding());
            }

            final Set<String> badBindings = IteratorUtils.set(IteratorUtils.<String>filter(bindings.keySet().iterator(), INVALID_BINDINGS_KEYS::contains));
            if (!badBindings.isEmpty()) {
                throw new ProcessingException(GremlinError.binding(badBindings));
            }

            // ignore control bindings that get passed in with the "#jsr223" prefix - those aren't used in compilation
            if (IteratorUtils.count(IteratorUtils.filter(bindings.keySet().iterator(), k -> !k.toString().startsWith("#jsr223"))) > settings.maxParameters) {
                throw new ProcessingException(GremlinError.binding(bindings.size(), settings.maxParameters));
            }
        }

        final Map<String, Object> args = message.getFields();
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : "gremlin-lang";
        final GremlinScriptEngine scriptEngine = gremlinExecutor.getScriptEngineManager().getEngineByName(language);

        final Bindings mergedBindings = mergeBindingsFromRequest(context, new SimpleBindings(graphManager.getAsBindings()));
        final Object result = scriptEngine.eval(message.getGremlin(), mergedBindings);

        final String bulkingSetting = context.getChannelHandlerContext().channel().attr(StateKey.REQUEST_HEADERS).get().get(Tokens.BULK_RESULTS);
        // bulking only applies if it's gremlin-lang, and per request token setting takes precedence over header setting.
        // The serializer check is temporarily needed because GraphSON hasn't been removed yet and doesn't support bulking.
        final boolean bulking = language.equals("gremlin-lang") && serializer instanceof GraphBinaryMessageSerializerV4 ?
                (args.containsKey(Tokens.BULK_RESULTS) ?
                        Objects.equals(args.get(Tokens.BULK_RESULTS), "true") :
                        Objects.equals(bulkingSetting, "true")) :
                false;

        Iterator itty = null;
        try {
            if (bulking) {
                // optimization for driver requests
                ((Traversal.Admin<?, ?>) result).applyStrategies();
                itty = new TraverserIterator((Traversal.Admin<?, ?>) result);
                handleIterator(context, itty, serializer, true);
            } else {
                itty = IteratorUtils.asIterator(result);
                handleIterator(context, itty, serializer, false);
            }
        } catch (Exception ex) {
            // TINKERPOP-3144 ensure Traversals are closed when exception thrown.
            if (itty instanceof TraverserIterator) {
                CloseableIterator.closeIterator(((TraverserIterator) itty).getTraversal());
            } else if (itty != null) {
                CloseableIterator.closeIterator(itty);
            }

            throw ex;
        }
    }

    @Override
    public void exceptionCaught(final ChannelHandlerContext ctx, final Throwable cause) {
        logger.error("Error processing HTTP Request", cause);

        if (ctx.channel().isActive()) {
            HttpHandlerUtil.sendError(ctx, INTERNAL_SERVER_ERROR, cause.getMessage());
        }
    }

    /**
     * Detects if the gremlin script is a transaction begin command.
     */
    private boolean isTransactionBegin(final String gremlin) {
        if (gremlin == null) return false;
        return gremlin.trim().equalsIgnoreCase("g.tx().begin()");
    }

    /**
     * Detects if the gremlin script is a transaction commit command.
     */
    private boolean isTransactionCommit(final String gremlin) {
        if (gremlin == null) return false;
        return gremlin.trim().equalsIgnoreCase("g.tx().commit()");
    }

    /**
     * Detects if the gremlin script is a transaction rollback command.
     */
    private boolean isTransactionRollback(final String gremlin) {
        if (gremlin == null) return false;
        return gremlin.trim().equalsIgnoreCase("g.tx().rollback()");
    }

    /**
     * Handle begin by creating an {@link UnmanagedTransaction} and submitting the open to its executor.
     */
    private void doBegin(final Context ctx) throws Exception {
        final String traversalSourceName = ctx.getRequestMessage().getField(Tokens.ARGS_G);

        final UnmanagedTransaction txCtx;
        try {
            txCtx = transactionManager.create(traversalSourceName);
            ctx.setTransactionId(txCtx.getTransactionId());
            final Graph graph = graphManager.getTraversalSource(traversalSourceName).getGraph();
            txCtx.submit(new FutureTask<>(() -> {
                graph.tx().open();
                return null;
            })).get(5000, TimeUnit.MILLISECONDS); // Not an option for now, but 5s should be plenty.
        } catch (IllegalStateException ise) {
            throw new ProcessingException(GremlinError.maxTransactionsExceeded(ise.getMessage()));
        } catch (IllegalArgumentException iae) {
            throw new ProcessingException(GremlinError.binding(traversalSourceName));
        } catch (UnsupportedOperationException uoe) {
            throw new ProcessingException(GremlinError.transactionNotSupported(uoe));
        } catch (ExecutionException | TimeoutException e) {
            throw new ProcessingException(GremlinError.transactionUnableToStart(e.getMessage()));
        }
    }

    private void runBegin(final Context ctx, UnmanagedTransaction tx, final Pair<String, MessageSerializer<?>> serializer) throws Exception {
        final ByteBuf chunk = makeChunk(ctx, serializer.getValue1(), List.of(Map.of(Tokens.ARGS_TRANSACTION_ID, tx.getTransactionId())), false, false);
        ctx.getChannelHandlerContext().writeAndFlush(new DefaultHttpContent(chunk));
    }

    private void handleGraphOp(final Context ctx,
                               final String transactionId,
                               final Consumer<Transaction> graphOp,
                               final Pair<String, MessageSerializer<?>> serializer) throws Exception {
        final Graph graph = graphManager.getTraversalSource(ctx.getRequestMessage().getField(Tokens.ARGS_G)).getGraph();
        graphOp.accept(graph.tx());
        transactionManager.destroy(transactionId);
        final ByteBuf chunk = makeChunk(ctx, serializer.getValue1(), List.of(Map.of(Tokens.ARGS_TRANSACTION_ID, transactionId)), false, false);
        ctx.getChannelHandlerContext().writeAndFlush(new DefaultHttpContent(chunk));
    }

    private Bindings mergeBindingsFromRequest(final Context ctx, final Bindings bindings) throws ProcessingException {
        // alias any global bindings to a different variable.
        final RequestMessage msg = ctx.getRequestMessage();

        // add any bindings to override any other supplied
        Optional.ofNullable((Map<String, Object>) msg.getFields().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);

        if (msg.getFields().containsKey(Tokens.ARGS_G)) {
            final String aliased = msg.getField(Tokens.ARGS_G);
            boolean found = false;

            // first check if the alias refers to a Graph instance
            final Graph graph = ctx.getGraphManager().getGraph(aliased);
            if (null != graph) {
                bindings.put(Tokens.ARGS_G, graph);
                found = true;
            }

            // if the alias wasn't found as a Graph then perhaps it is a TraversalSource - it needs to be
            // something
            if (!found) {
                final TraversalSource ts = ctx.getGraphManager().getTraversalSource(aliased);
                if (null != ts) {
                    bindings.put(Tokens.ARGS_G, ts);
                    found = true;
                }
            }

            // this validation is important to calls to GraphManager.commit() and rollback() as they both
            // expect that the aliases supplied are valid
            if (!found) {
                throw new ProcessingException(GremlinError.binding(aliased));
            }
        }

        return bindings;
    }

    private void handleIterator(final Context context, final Iterator itty, final MessageSerializer<?> serializer, final boolean bulking) throws InterruptedException {
        final ChannelHandlerContext nettyContext = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();

        // used to limit warnings for when netty fills the buffer and hits the high watermark - prevents
        // over-logging of the same message.
        long lastWarningTime = 0;
        int warnCounter = 0;

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            ByteBuf chunk = null;
            try {
                chunk = makeChunk(context, serializer, new ArrayList<>(), false, bulking);
                nettyContext.writeAndFlush(new DefaultHttpContent(chunk));
            } catch (Exception ex) {
                // Bytebuf is a countable release - if it does not get written downstream
                // it needs to be released here
                if (chunk != null) chunk.release();
            }
            return;
        }

        // the batch size can be overridden by the request
        final int resultIterationBatchSize = (Integer) msg.optionalField(Tokens.ARGS_BATCH_SIZE)
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
            // NoSuchElementException will raise its head. also need a check to ensure that this iteration doesn't
            // require a forced flush which can be forced by sub-classes.
            //
            // this could be placed inside the isWriteable() portion of the if-then below but it seems better to
            // allow iteration to continue into a batch if that is possible rather than just doing nothing at all
            // while waiting for the client to catch up
            if (aggregate.size() < resultIterationBatchSize && itty.hasNext()) {
                if (bulking) {
                    Traverser traverser = (Traverser) itty.next();
                    aggregate.add(traverser.get());
                    aggregate.add(traverser.bulk());
                } else {
                    aggregate.add(itty.next());
                }
            }

            // Don't keep executor busy if client has already given up; there is no way to catch up if the channel is
            // not active, and hence we should break the loop.
            if (!nettyContext.channel().isActive()) {
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
                if (aggregate.size() == resultIterationBatchSize || !itty.hasNext()) {
                    ByteBuf chunk = null;
                    try {
                        chunk = makeChunk(context, serializer, aggregate, itty.hasNext(), bulking);
                    } catch (Exception ex) {
                        // Bytebuf is a countable release - if it does not get written downstream
                        // it needs to be released here
                        if (chunk != null) chunk.release();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        break;
                    }

                    // track whether there is anything left in the iterator because it needs to be accessed after
                    // the transaction could be closed - in that case a call to hasNext() could open a new transaction
                    // unintentionally
                    hasMore = itty.hasNext();

                    try {
                        // only need to reset the aggregation list if there's more stuff to write
                        if (hasMore) {
                            aggregate = new ArrayList<>(resultIterationBatchSize);
                        }
                    } catch (Exception ex) {
                        // Bytebuf is a countable release - if it does not get written downstream
                        // it needs to be released here
                        if (chunk != null) chunk.release();
                        throw ex;
                    }

                    nettyContext.writeAndFlush(new DefaultHttpContent(chunk));
                }
            } else {
                final long currentTime = System.currentTimeMillis();

                // exponential delay between warnings. don't keep triggering this warning over and over again for the
                // same request. totalPendingWriteBytes is volatile so it is possible that by the time this warning
                // hits the log the low watermark may have been hit
                long interval = (long) Math.pow(2, warnCounter) * 1000;
                if (currentTime - lastWarningTime >= interval) {
                    final Channel ch = context.getChannelHandlerContext().channel();
                    logger.warn("Warning {}: Outbound buffer size={}, pausing response writing as writeBufferHighWaterMark exceeded on request {} for channel {} - writing will continue once client has caught up",
                            warnCounter,
                            ch.unsafe().outboundBuffer().totalPendingWriteBytes(),
                            ch.attr(StateKey.REQUEST_ID),
                            ch.id());

                    lastWarningTime = currentTime;
                    warnCounter++;
                }

                // since the client is lagging we can hold here for a period of time for the client to catch up.
                // this isn't blocking the IO thread - just a worker.
                TimeUnit.MILLISECONDS.sleep(WRITE_PAUSE_TIME_MS);
                writePausesMeter.mark();
            }
        }
    }

    /**
     * Check if any exception in the chain is {@link TemporaryException} or {@link Failure} then respond with the
     * right error code so that the client knows to retry.
     */
    private Optional<Throwable> determineIfSpecialException(final Throwable ex) {
        return Stream.of(ExceptionUtils.getThrowables(ex)).
                filter(i -> i instanceof TemporaryException || i instanceof Failure).findFirst();
    }

    private boolean acceptsDeflateEncoding(List<String> encodings) {
        for (String encoding : encodings) {
            if (encoding.contains(DEFLATE.toString())) {
                return true;
            }
        }

        return false;
    }

    private static ByteBuf makeChunk(final Context ctx, final MessageSerializer<?> serializer,
                                     final List<Object> aggregate, final boolean hasMore,
                                     final boolean bulking) throws Exception {
        try {
            final ChannelHandlerContext nettyContext = ctx.getChannelHandlerContext();

            ctx.handleDetachment(aggregate);

            if (!hasMore && ctx.getRequestState() == STREAMING) {
                ctx.setRequestState(FINISHING);
            }

            ResponseMessage responseMessage = null;

            // for this state no need to build full ResponseMessage
            if (ctx.getRequestState() != STREAMING) {
                final ResponseMessage.Builder builder = ResponseMessage.build().result(aggregate);

                // need to put status in last message
                if (ctx.getRequestState() == FINISHING) {
                    builder.code(HttpResponseStatus.OK);
                }

                builder.bulked(bulking);

                responseMessage = builder.create();
            }

            switch (ctx.getRequestState()) {
                case NOT_STARTED:
                    if (hasMore) {
                        ctx.setRequestState(STREAMING);
                        return serializer.writeHeader(responseMessage, nettyContext.alloc());
                    }
                    ctx.setRequestState(FINISHED);

                    return serializer.serializeResponseAsBinary(ResponseMessage.build()
                            .result(aggregate)
                            .bulked(bulking)
                            .code(HttpResponseStatus.OK)
                            .create(), nettyContext.alloc());

                case STREAMING:
                    return serializer.writeChunk(aggregate, nettyContext.alloc());
                case FINISHING:
                    ctx.setRequestState(FINISHED);
                    return serializer.writeFooter(responseMessage, nettyContext.alloc());
            }

            return serializer.serializeResponseAsBinary(responseMessage, nettyContext.alloc());

        } catch (Exception ex) {
            final UUID requestId = ctx.getChannelHandlerContext().attr(StateKey.REQUEST_ID).get();
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", aggregate, requestId, ex);
            writeError(ctx, GremlinError.serialization(ex), serializer);
            throw ex;
        }
    }

    public enum RequestState {
        NOT_STARTED,
        STREAMING,
        // last portion of data
        FINISHING,
        FINISHED,
        ERROR
    }
}
