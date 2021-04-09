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
package org.apache.tinkerpop.gremlin.server.handler;

import groovy.lang.GroovyRuntimeException;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.groovy.jsr223.TimedInterruptTimeoutException;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.GraphOp;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.server.util.TraverserIterator;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.codehaus.groovy.control.MultipleCompilationErrorsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.script.Bindings;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_COMMIT;
import static org.apache.tinkerpop.gremlin.process.traversal.GraphOp.TX_ROLLBACK;

/**
 * A base implementation of {@link Session} which offers some common functionality that matches typical Gremlin Server
 * request response expectations for script, bytecode and graph operations. The class is designed to be extended but
 * take care in understanding the way that different methods are called as they do depend on one another a bit. It
 * maybe best to examine the source code to determine how best to use this class or to extend from the higher order
 * classes of {@link SingleTaskSession} or {@link MultiTaskSession}.
 */
public abstract class AbstractSession implements Session, AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(AbstractSession.class);
    private static final Logger auditLogger = LoggerFactory.getLogger(GremlinServer.AUDIT_LOGGER_NAME);

    private final boolean sessionIdOnRequest;
    private final Channel initialChannel;
    private final boolean transactionManaged;
    private final String sessionId;
    private final AtomicReference<ScheduledFuture<?>> sessionCancelFuture = new AtomicReference<>();
    private final AtomicReference<Future<?>> sessionFuture = new AtomicReference<>();
    private long actualTimeoutLengthWhenClosed = 0;

    /**
     * The session thread is a reference to the thread that is running the session and should be set by an
     * implementation as the first line of the {@link #run()} method.
     */
    protected Thread sessionThread;
    protected final boolean maintainStateAfterException;
    protected final AtomicReference<CloseReason> closeReason = new AtomicReference<>();
    protected final GraphManager graphManager;
    protected final ConcurrentMap<String, Session> sessions;
    protected final Set<String> aliasesUsedBySession = new HashSet<>();

    /**
     * The reason that a particular session closed. The reason for the close is generally not important as a
     * final disposition for the {@link Session} instance and is more useful in aiding flow control during the
     * close process.
     */
    protected enum CloseReason {

        /**
         * The session exits in a fashion that did not precipitate from some form of interruption, timeout or
         * exception, i.e. it is simply allowed to process to an exit through its normal execution flow. This status
         * may or may not be possible given the context of the implementation. For example, a {@link MultiTaskSession}
         * needs to be interrupted to stop processing.
         */
        EXIT_PROCESSING,

        /**
         * The session was interrupted by the channel closing, which can be something initiated by closing the
         * {@link Client} or might be triggered by the server. This may not be considered an error situation and
         * depending on context, might be similar to a {@link #EXIT_PROCESSING} termination.
         */
        CHANNEL_CLOSED,

        /**
         * The session encountered an exception related to execution like a script error, traversal iteration problem,
         * serialization issue, etc.
         */
        PROCESSING_EXCEPTION,

        /**
         * The session was interrupted by the session lifetime timeout.
         */
        SESSION_TIMEOUT,

        /**
         * The session was interrupted by the request timeout.
         */
        REQUEST_TIMEOUT
    }

    AbstractSession(final SessionTask sessionTask, final String sessionId,
                    final boolean transactionManaged,
                    final ConcurrentMap<String, Session> sessions) {
        // this only applies to sessions
        this.maintainStateAfterException = (boolean) sessionTask.getRequestMessage().
                optionalArgs(Tokens.ARGS_MAINTAIN_STATE_AFTER_EXCEPTION).orElse(false);
        this.sessionIdOnRequest = sessionTask.getRequestMessage().optionalArgs(Tokens.ARGS_SESSION).isPresent();
        this.transactionManaged = transactionManaged;
        this.sessionId = sessionId;
        this.initialChannel = sessionTask.getChannelHandlerContext().channel();

        // close session if the channel closes to cleanup and close transactions
        this.initialChannel.closeFuture().addListener(f -> {
            if (closeReason.compareAndSet(null, CloseReason.CHANNEL_CLOSED)) {
                close();
            }
        });
        this.sessions = sessions;
        this.graphManager = sessionTask.getGraphManager();
    }

    protected synchronized void cancel(final boolean mayInterruptIfRunning) {
        final FutureTask<?> sf = (FutureTask) sessionFuture.get();
        if (sf != null && !sf.isDone()) {
            sf.cancel(mayInterruptIfRunning);
        }
    }

    public boolean isTransactionManaged() {
        return transactionManaged;
    }

    @Override
    public String getSessionId() {
        return sessionId;
    }

    public boolean isBoundTo(final Channel channel) {
        return channel == initialChannel;
    }

    public long getActualTimeoutLengthWhenClosed() {
        return actualTimeoutLengthWhenClosed;
    }

    public Optional<CloseReason> getCloseReason() {
        return Optional.ofNullable(closeReason.get());
    }

    /**
     * Gets the script engine from the cached one in the {@link GremlinExecutor}.
     */
    public GremlinScriptEngine getScriptEngine(final SessionTask sessionTask, final String language) {
        return sessionTask.getGremlinExecutor().getScriptEngineManager().getEngineByName(language);
    }

    @Override
    public void setSessionCancelFuture(final ScheduledFuture<?> f) {
        if (!sessionCancelFuture.compareAndSet(null, f))
            throw new IllegalStateException("Session cancellation future is already set");
    }

    @Override
    public void setSessionFuture(final Future<?> f) {
        if (!sessionFuture.compareAndSet(null, f))
            throw new IllegalStateException("Session future is already set");
    }

    @Override
    public synchronized void triggerTimeout(final long timeout, final boolean causedBySession) {
        // triggering timeout triggers the stop of the session which will end in close()
        // for final cleanup
        final Future<?> f = sessionFuture.get();
        if (f != null && !f.isDone()) {
            if (closeReason.compareAndSet(null, causedBySession ? CloseReason.SESSION_TIMEOUT : CloseReason.REQUEST_TIMEOUT)) {
                actualTimeoutLengthWhenClosed = timeout;

                // if caused by a session timeout for a MultiTaskSession OR if it is a request timeout for a
                // SingleTaskSession request then we can just straight cancel() the session instance
                if (causedBySession || !sessionIdOnRequest)
                    cancel(true);
                else {
                    // in both MultiTaskSession and SingleTaskSession the thread gets set immediately at the start
                    // of run() as it should (though "single" has little need for it). As triggerTimeout() for a
                    // request MultiTaskSession can only be called AFTER we are deep in the run() there should be
                    // no chance of race conditions or situations where the sessionThread is null at this point.
                    if (sessionThread != null) {
                        sessionThread.interrupt();
                    } else {
                        logger.debug("{} is a {} which is not interruptable as the thread running the session has not " +
                                        "been set - please check the implementation if this is not desirable",
                                sessionId, this.getClass().getSimpleName());
                    }
                }
            }
        }
    }

    protected void process(final SessionTask sessionTask) throws SessionException {
        final RequestMessage msg = sessionTask.getRequestMessage();
        final Map<String, Object> args = msg.getArgs();
        final Object gremlinToExecute = args.get(Tokens.ARGS_GREMLIN);

        // for strict transactions track the aliases used so that we can commit them and only them on close()
        if (sessionTask.getSettings().strictTransactionManagement)
            msg.optionalArgs(Tokens.ARGS_ALIASES).ifPresent(m -> aliasesUsedBySession.addAll(((Map<String,String>) m).values()));

        try {
            // itty is optional as Bytecode could be a "graph operation" rather than a Traversal. graph operations
            // don't need to be iterated and handle their own lifecycle
            final Optional<Iterator<?>> itty = gremlinToExecute instanceof Bytecode ?
                    fromBytecode(sessionTask, (Bytecode) gremlinToExecute) :
                    Optional.of(fromScript(sessionTask, (String) gremlinToExecute));

            processAuditLog(sessionTask.getSettings(), sessionTask.getChannelHandlerContext(), gremlinToExecute);

            if (itty.isPresent())
                handleIterator(sessionTask, itty.get());
        } catch (Exception ex) {
            handleException(sessionTask, ex);
        }
    }

    protected void handleException(final SessionTask sessionTask, final Throwable t) throws SessionException {
        if (t instanceof SessionException) throw (SessionException) t;

        final Optional<Throwable> possibleTemporaryException = determineIfTemporaryException(t);
        if (possibleTemporaryException.isPresent()) {
            final Throwable temporaryException = possibleTemporaryException.get();
            throw new SessionException(temporaryException.getMessage(), t,
                    ResponseMessage.build(sessionTask.getRequestMessage())
                            .code(ResponseStatusCode.SERVER_ERROR_TEMPORARY)
                            .statusMessage(temporaryException.getMessage())
                            .statusAttributeException(temporaryException).create());
        }

        final Throwable root = ExceptionUtils.getRootCause(t);

        if (root instanceof TimedInterruptTimeoutException) {
            // occurs when the TimedInterruptCustomizerProvider is in play
            final String msg = String.format("A timeout occurred within the script during evaluation of [%s] - consider increasing the limit given to TimedInterruptCustomizerProvider",
                    sessionTask.getRequestMessage().getRequestId());
            throw new SessionException(msg, root, ResponseMessage.build(sessionTask.getRequestMessage())
                    .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                    .statusMessage("Timeout during script evaluation triggered by TimedInterruptCustomizerProvider")
                    .create());
        }

        if (root instanceof TimeoutException) {
            final String errorMessage = String.format("Script evaluation exceeded the configured threshold for request [%s]",
                    sessionTask.getRequestMessage().getRequestId());
            throw new SessionException(errorMessage, root, ResponseMessage.build(sessionTask.getRequestMessage())
                    .code(ResponseStatusCode.SERVER_ERROR_TIMEOUT)
                    .statusMessage(t.getMessage())
                    .create());
        }

        if (root instanceof InterruptedException ||
                root instanceof TraversalInterruptedException ||
                root instanceof InterruptedIOException) {
            String msg = "Processing interrupted but the reason why was not known";
            switch (closeReason.get()) {
                case CHANNEL_CLOSED:
                    msg = "Processing interrupted because the channel was closed";
                    break;
                case SESSION_TIMEOUT:
                    msg = String.format("Session closed - %s - sessionLifetimeTimeout of %s ms exceeded", sessionId, actualTimeoutLengthWhenClosed);
                    break;
                case REQUEST_TIMEOUT:
                    msg = String.format("Evaluation exceeded timeout threshold of %s ms", actualTimeoutLengthWhenClosed);
                    break;
            }
            final ResponseStatusCode code = closeReason.get() == CloseReason.SESSION_TIMEOUT || closeReason.get() == CloseReason.REQUEST_TIMEOUT ?
                    ResponseStatusCode.SERVER_ERROR_TIMEOUT : ResponseStatusCode.SERVER_ERROR;
            throw new SessionException(msg, root, ResponseMessage.build(sessionTask.getRequestMessage())
                    .code(code)
                    .statusMessage(msg).create());
        }

        if (root instanceof MultipleCompilationErrorsException && root.getMessage().contains("Method too large") &&
                ((MultipleCompilationErrorsException) root).getErrorCollector().getErrorCount() == 1) {
            final String errorMessage = String.format("The Gremlin statement that was submitted exceeds the maximum compilation size allowed by the JVM, please split it into multiple smaller statements - %s", trimMessage(sessionTask.getRequestMessage()));
            logger.warn(errorMessage);
            throw new SessionException(errorMessage, root, ResponseMessage.build(sessionTask.getRequestMessage())
                    .code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                    .statusMessage(errorMessage)
                    .statusAttributeException(root).create());
        }

        // GroovyRuntimeException will hit a pretty wide range of eval type errors, like MissingPropertyException,
        // CompilationFailedException, MissingMethodException, etc. If more specific handling is required then
        // try to catch it earlier above.
        if (root instanceof GroovyRuntimeException ||
                root instanceof VerificationException ||
                root instanceof ScriptException) {
            throw new SessionException(root.getMessage(), root, ResponseMessage.build(sessionTask.getRequestMessage())
                    .code(ResponseStatusCode.SERVER_ERROR_EVALUATION)
                    .statusMessage(root.getMessage())
                    .statusAttributeException(root).create());
        }

        throw new SessionException(root.getClass().getSimpleName() + ": " + root.getMessage(), root,
                ResponseMessage.build(sessionTask.getRequestMessage())
                        .code(ResponseStatusCode.SERVER_ERROR)
                        .statusAttributeException(root)
                        .statusMessage(root.getMessage()).create());
    }

    /**
     * Used to decrease the size of a Gremlin script that triggered a "method too large" exception so that it
     * doesn't log a massive text string nor return a large error message.
     */
    private RequestMessage trimMessage(final RequestMessage msg) {
        final RequestMessage trimmedMsg = RequestMessage.from(msg).create();
        if (trimmedMsg.getArgs().containsKey(Tokens.ARGS_GREMLIN))
            trimmedMsg.getArgs().put(Tokens.ARGS_GREMLIN, trimmedMsg.getArgs().get(Tokens.ARGS_GREMLIN).toString().substring(0, 1021) + "...");

        return trimmedMsg;
    }

    /**
     * Check if any exception in the chain is TemporaryException then we should respond with the right error code so
     * that the client knows to retry.
     */
    protected Optional<Throwable> determineIfTemporaryException(final Throwable ex) {
        return Stream.of(ExceptionUtils.getThrowables(ex)).
                filter(i -> i instanceof TemporaryException).findFirst();
    }

    @Override
    public synchronized void close() {
        // already closing/closed
        if (!sessions.containsKey(sessionId)) return;

        sessions.remove(sessionId);

        if (sessionCancelFuture.get() != null) {
            final ScheduledFuture<?> f = sessionCancelFuture.get();
            if (!f.isDone()) f.cancel(true);
        }
    }

    /**
     * Constructs an {@code Iterator} from the results of a script evaluation provided in the {@link SessionTask}.
     *
     * @param sessionTask The session task which can be used as a context in constructing the {@code Iterator}
     * @param script The script extracted by the calling method from the {@code sessionTask}
     */
    protected Iterator<?> fromScript(final SessionTask sessionTask, final String script) throws Exception {
        final RequestMessage msg = sessionTask.getRequestMessage();
        final Map<String, Object> args = msg.getArgs();
        final String language = args.containsKey(Tokens.ARGS_LANGUAGE) ? (String) args.get(Tokens.ARGS_LANGUAGE) : "gremlin-groovy";
        return IteratorUtils.asIterator(getScriptEngine(sessionTask, language).eval(
                script, mergeBindingsFromRequest(sessionTask, getWorkerBindings())));
    }

    /**
     * Constructs an {@code Iterator} from {@link Bytecode} provided in the {@link SessionTask}. If the {@link Bytecode}
     * is found to evalute to a {@link GraphOp} then it is processed and an empty {@code Optional} is returned.
     *
     * @param sessionTask The session task which can be used as a context in constructing the {@code Iterator}
     * @param bytecode The {@link Bytecode} extracted by the calling method from the {@code sessionTask}
     */
    protected Optional<Iterator<?>> fromBytecode(final SessionTask sessionTask, final Bytecode bytecode) throws Exception {
        final RequestMessage msg = sessionTask.getRequestMessage();

        final Traversal.Admin<?, ?> traversal;
        final Map<String, String> aliases = (Map<String, String>) msg.optionalArgs(Tokens.ARGS_ALIASES).get();
        final GraphManager graphManager = sessionTask.getGraphManager();
        final String traversalSourceName = aliases.entrySet().iterator().next().getValue();
        final TraversalSource g = graphManager.getTraversalSource(traversalSourceName);

        // handle bytecode based graph operations like commit/rollback commands
        if (BytecodeHelper.isGraphOperation(bytecode)) {
            handleGraphOperation(sessionTask, bytecode, g.getGraph());
            return Optional.empty();
        } else {

            final Optional<String> lambdaLanguage = BytecodeHelper.getLambdaLanguage(bytecode);
            if (!lambdaLanguage.isPresent())
                traversal = JavaTranslator.of(g).translate(bytecode);
            else {
                final SimpleBindings bindings = new SimpleBindings();
                bindings.put(traversalSourceName, g);
                traversal = sessionTask.getGremlinExecutor().getScriptEngineManager().
                        getEngineByName(lambdaLanguage.get()).eval(bytecode, bindings, traversalSourceName);
            }

            // compile the traversal - without it getEndStep() has nothing in it
            traversal.applyStrategies();

            return Optional.of(new TraverserIterator(traversal));
        }
    }

    protected Bindings getWorkerBindings() throws SessionException {
        return new SimpleBindings(graphManager.getAsBindings());
    }

    protected Bindings mergeBindingsFromRequest(final SessionTask sessionTask, final Bindings bindings) throws SessionException {
        // alias any global bindings to a different variable.
        final RequestMessage msg = sessionTask.getRequestMessage();
        if (msg.getArgs().containsKey(Tokens.ARGS_ALIASES)) {
            final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(Tokens.ARGS_ALIASES);
            for (Map.Entry<String,String> aliasKv : aliases.entrySet()) {
                boolean found = false;

                // first check if the alias refers to a Graph instance
                final Graph graph = sessionTask.getGraphManager().getGraph(aliasKv.getValue());
                if (null != graph) {
                    bindings.put(aliasKv.getKey(), graph);
                    found = true;
                }

                // if the alias wasn't found as a Graph then perhaps it is a TraversalSource - it needs to be
                // something
                if (!found) {
                    final TraversalSource ts = sessionTask.getGraphManager().getTraversalSource(aliasKv.getValue());
                    if (null != ts) {
                        bindings.put(aliasKv.getKey(), ts);
                        found = true;
                    }
                }

                // this validation is important to calls to GraphManager.commit() and rollback() as they both
                // expect that the aliases supplied are valid
                if (!found) {
                    final String error = String.format("Could not alias [%s] to [%s] as [%s] not in the Graph or TraversalSource global bindings",
                            aliasKv.getKey(), aliasKv.getValue(), aliasKv.getValue());
                    throw new SessionException(error, ResponseMessage.build(msg)
                            .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(error).create());
                }
            }
        } else {
            // there's no bindings so determine if that's ok with Gremlin Server
            if (sessionTask.getSettings().strictTransactionManagement) {
                final String error = "Gremlin Server is configured with strictTransactionManagement as 'true' - the 'aliases' arguments must be provided";
                throw new SessionException(error, ResponseMessage.build(msg)
                        .code(ResponseStatusCode.REQUEST_ERROR_INVALID_REQUEST_ARGUMENTS).statusMessage(error).create());
            }
        }

        // add any bindings to override any other supplied
        Optional.ofNullable((Map<String, Object>) msg.getArgs().get(Tokens.ARGS_BINDINGS)).ifPresent(bindings::putAll);
        return bindings;
    }

    /**
     * Provides a generic way of iterating a result set back to the client.
     *
     * @param sessionTask The Gremlin Server {@link SessionTask} object containing settings, request message, etc.
     * @param itty The result to iterator
     */
    protected void handleIterator(final SessionTask sessionTask, final Iterator<?> itty) throws InterruptedException {
        final ChannelHandlerContext nettyContext = sessionTask.getChannelHandlerContext();
        final RequestMessage msg = sessionTask.getRequestMessage();
        final Settings settings = sessionTask.getSettings();
        boolean warnOnce = false;

        // sessionless requests are always transaction managed, but in-session requests are configurable.
        final boolean managedTransactionsForRequest = transactionManaged ?
                true : (Boolean) msg.getArgs().getOrDefault(Tokens.ARGS_MANAGE_TRANSACTION, false);

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            final Map<String, Object> attributes = generateStatusAttributes(sessionTask,ResponseStatusCode.NO_CONTENT, itty);
            // as there is nothing left to iterate if we are transaction managed then we should execute a
            // commit here before we send back a NO_CONTENT which implies success
            if (managedTransactionsForRequest)
                closeTransaction(sessionTask, Transaction.Status.COMMIT);

            sessionTask.writeAndFlush(ResponseMessage.build(msg)
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
            // todo: what implementation does this?! can we kill it going forward - seems always false
            // final boolean forceFlush = isForceFlushed(nettyContext, msg, itty);
            final boolean forceFlush = false;

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
                    Frame frame = null;
                    try {
                        frame = makeFrame(sessionTask, aggregate, code, itty);
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        if (managedTransactionsForRequest)
                            closeTransaction(sessionTask, Transaction.Status.ROLLBACK);
                        break;
                    }

                    // track whether there is anything left in the iterator because it needs to be accessed after
                    // the transaction could be closed - in that case a call to hasNext() could open a new transaction
                    // unintentionally
                    final boolean moreInIterator = itty.hasNext();

                    try {
                        // only need to reset the aggregation list if there's more stuff to write
                        if (moreInIterator)
                            aggregate = new ArrayList<>(resultIterationBatchSize);
                        else {
                            // iteration and serialization are both complete which means this finished successfully. note that
                            // errors internal to script eval or timeout will rollback given GremlinServer's global configurations.
                            // local errors will get rolledback below because the exceptions aren't thrown in those cases to be
                            // caught by the GremlinExecutor for global rollback logic. this only needs to be committed if
                            // there are no more items to iterate and serialization is complete
                            if (managedTransactionsForRequest)
                                closeTransaction(sessionTask, Transaction.Status.COMMIT);

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

                    if (!moreInIterator) iterateComplete(sessionTask, itty);

                    // the flush is called after the commit has potentially occurred.  in this way, if a commit was
                    // required then it will be 100% complete before the client receives it. the "frame" at this point
                    // should have completely detached objects from the transaction (i.e. serialization has occurred)
                    // so a new one should not be opened on the flush down the netty pipeline
                    sessionTask.writeAndFlush(code, frame);
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

    /**
     * If {@link Bytecode} is detected to contain a {@link GraphOp} then it gets processed by this method.
     */
    protected void handleGraphOperation(final SessionTask sessionTask, final Bytecode bytecode, final Graph graph) throws Exception {
        final RequestMessage msg = sessionTask.getRequestMessage();
        if (graph.features().graph().supportsTransactions()) {
            if (TX_COMMIT.equals(bytecode) || TX_ROLLBACK.equals(bytecode)) {
                final boolean commit = TX_COMMIT.equals(bytecode);
                closeTransaction(sessionTask, commit ? Transaction.Status.COMMIT : Transaction.Status.ROLLBACK);

                // write back a no-op for success
                final Map<String, Object> attributes = generateStatusAttributes(sessionTask,
                        ResponseStatusCode.NO_CONTENT, Collections.emptyIterator());
                sessionTask.writeAndFlush(ResponseMessage.build(msg)
                            .code(ResponseStatusCode.NO_CONTENT)
                            .statusAttributes(attributes)
                            .create());
            } else {
                throw new IllegalStateException(String.format(
                        "Bytecode in request is not a recognized graph operation: %s", bytecode.toString()));
            }
        }
    }

    /**
     * Called when iteration within {@link #handleIterator(SessionTask, Iterator)} is on its final pass and the final
     * frame is about to be sent back to the client. This method only gets called on successful iteration of the
     * entire result.
     */
    protected void iterateComplete(final SessionTask sessionTask, final Iterator<?> itty) {
        // do nothing by default
    }

    /**
     * Generates response status meta-data to put on a {@link ResponseMessage}.
     *
     * @param itty a reference to the current {@link Iterator} of results - it is not meant to be forwarded in
     *             this method
     */
    protected Map<String, Object> generateStatusAttributes(final SessionTask sessionTask,
                                                           final ResponseStatusCode code, final Iterator<?> itty) {
        // only return server metadata on the last message
        if (itty.hasNext()) return Collections.emptyMap();

        final Map<String, Object> metaData = new HashMap<>();
        metaData.put(Tokens.ARGS_HOST, sessionTask.getChannelHandlerContext().channel().remoteAddress().toString());

        return metaData;
    }

    /**
     * Generates response result meta-data to put on a {@link ResponseMessage}.
     *
     * @param itty a reference to the current {@link Iterator} of results - it is not meant to be forwarded in
     *             this method
     */
    protected Map<String, Object> generateResponseMetaData(final SessionTask sessionTask,
                                                           final ResponseStatusCode code, final Iterator<?> itty) {
        return Collections.emptyMap();
    }

    protected Frame makeFrame(final SessionTask sessionTask, final List<Object> aggregate,
                              final ResponseStatusCode code, final Iterator<?> itty) throws Exception {
        final RequestMessage msg = sessionTask.getRequestMessage();
        final ChannelHandlerContext nettyContext = sessionTask.getChannelHandlerContext();
        final MessageSerializer serializer = nettyContext.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = nettyContext.channel().attr(StateKey.USE_BINARY).get();

        final Map<String, Object> responseMetaData = generateResponseMetaData(sessionTask, code, itty);
        final Map<String, Object> statusAttributes = generateStatusAttributes(sessionTask, code, itty);
        try {
            if (useBinary) {
                return new Frame(serializer.serializeResponseAsBinary(ResponseMessage.build(msg)
                        .code(code)
                        .statusAttributes(statusAttributes)
                        .responseMetaData(responseMetaData)
                        .result(aggregate).create(), nettyContext.alloc()));
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer textSerializer = (MessageTextSerializer) serializer;
                return new Frame(textSerializer.serializeResponseAsString(ResponseMessage.build(msg)
                        .code(code)
                        .statusAttributes(statusAttributes)
                        .responseMetaData(responseMetaData)
                        .result(aggregate).create()));
            }
        } catch (Exception ex) {
            logger.warn("The result [{}] in the request {} could not be serialized and returned.", aggregate, msg.getRequestId(), ex);
            final String errorMessage = String.format("Error during serialization: %s", ExceptionHelper.getMessageFromExceptionOrCause(ex));
            final ResponseMessage error = ResponseMessage.build(msg.getRequestId())
                    .statusMessage(errorMessage)
                    .statusAttributeException(ex)
                    .code(ResponseStatusCode.SERVER_ERROR_SERIALIZATION).create();
            sessionTask.writeAndFlush(error);
            throw ex;
        }
    }

    /**
     * Called right before a transaction starts within {@link #run()}.
     */
    protected void startTransaction(final SessionTask sessionTask) {
        // check if transactions are open and rollback first to ensure a fresh start.
        graphManager.rollbackAll();
    }

    /**
     * Close the transaction without a {@link SessionTask} which supplies {@code null} to that argument for
     * {@link #closeTransaction(SessionTask, Transaction.Status)}. This method is idempotent.
     */
    protected void closeTransaction(final Transaction.Status status) {
        closeTransaction(null, status);
    }

    /**
     * Tries to close the transaction but will catch exceptions and log them. This method is idempotent.
     */
    protected void closeTransactionSafely(final Transaction.Status status) {
        closeTransactionSafely(null, status);
    }

    /**
     * Tries to close the transaction but will catch exceptions and log them. This method is idempotent.
     */
    protected void closeTransactionSafely(final SessionTask sessionTask, final Transaction.Status status) {
        try {
            closeTransaction(sessionTask, status);
        } catch (Exception ex) {
            logger.error("Failed to close transaction", ex);
        }
    }

    private void processAuditLog(final Settings settings, final ChannelHandlerContext ctx, final Object gremlinToExecute) {
        if (settings.enableAuditLog) {
            AuthenticatedUser user = ctx.channel().attr(StateKey.AUTHENTICATED_USER).get();
            if (null == user) {    // This is expected when using the AllowAllAuthenticator
                user = AuthenticatedUser.ANONYMOUS_USER;
            }
            String address = ctx.channel().remoteAddress().toString();
            if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
            auditLogger.info("User {} with address {} requested: {}", user.getName(), address, gremlinToExecute);
        }

        if (settings.authentication.enableAuditLog) {
            String address = ctx.channel().remoteAddress().toString();
            if (address.startsWith("/") && address.length() > 1) address = address.substring(1);
            auditLogger.info("User with address {} requested: {}", address, gremlinToExecute);
        }
    }

    /**
     * Closes a transaction with commit or rollback. Strict transaction management settings are observed when
     * configured as such in {@link Settings#strictTransactionManagement} and when aliases are present on the
     * request in the current {@link SessionTask}. If the supplied {@link SessionTask} is {@code null} then "strict" is
     * bypassed so this form must be called with care. Bypassing is often useful to ensure that all transactions
     * are cleaned up when multiple graphs are referenced. Prefer calling {@link #closeTransaction(Transaction.Status)}
     * in this case instead. This method is idempotent.
     */
    protected void closeTransaction(final SessionTask sessionTask, final Transaction.Status status) {
        if (status != Transaction.Status.COMMIT && status != Transaction.Status.ROLLBACK)
            throw new IllegalStateException(String.format("Transaction.Status not supported: %s", status));

        final boolean commit = status == Transaction.Status.COMMIT;
        final boolean strict = sessionTask != null && sessionTask.getSettings().strictTransactionManagement;

        if (strict) {
            if (commit)
                graphManager.commit(new HashSet<>(aliasesUsedBySession));
            else
                graphManager.rollback(new HashSet<>(aliasesUsedBySession));
        } else {
            if (commit)
                graphManager.commitAll();
            else
                graphManager.rollbackAll();
        }
    }
}
