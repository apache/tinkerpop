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

import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.driver.ser.MessageTextSerializer;
import org.apache.tinkerpop.gremlin.server.Context;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.server.handler.StateKey;
import org.apache.tinkerpop.gremlin.server.util.ExceptionHelper;
import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * A base {@link OpProcessor} implementation that processes an {@code Iterator} of results in a generalized way while
 * ensuring that graph transactions are properly managed.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public abstract class AbstractOpProcessor implements OpProcessor {
    private static final Logger logger = LoggerFactory.getLogger(AbstractEvalOpProcessor.class);

    /**
     * When set to {@code true}, transactions are always managed otherwise they can be overridden by the request.
     */
    protected final boolean manageTransactions;

    protected AbstractOpProcessor(final boolean manageTransactions) {
        this.manageTransactions = manageTransactions;
    }

    /**
     * Check if any exception in the chain is TemporaryException then we should respond with the right error code so
     * that the client knows to retry.
     */
    protected static Optional<Throwable> determineIfTemporaryException(final Throwable ex) {
        return Stream.of(ExceptionUtils.getThrowables(ex)).
                filter(i -> i instanceof TemporaryException).findFirst();
    }

    /**
     * Provides a generic way of iterating a result set back to the client.
     *
     * @param context The Gremlin Server {@link Context} object containing settings, request message, etc.
     * @param itty The result to iterator
     */
    protected void handleIterator(final Context context, final Iterator itty) throws InterruptedException {
        final ChannelHandlerContext nettyContext = context.getChannelHandlerContext();
        final RequestMessage msg = context.getRequestMessage();
        final Settings settings = context.getSettings();
        final MessageSerializer<?> serializer = nettyContext.channel().attr(StateKey.SERIALIZER).get();
        final boolean useBinary = nettyContext.channel().attr(StateKey.USE_BINARY).get();
        boolean warnOnce = false;

        // sessionless requests are always transaction managed, but in-session requests are configurable.
        final boolean managedTransactionsForRequest = manageTransactions ?
                true : (Boolean) msg.getArgs().getOrDefault(Tokens.ARGS_MANAGE_TRANSACTION, false);

        // we have an empty iterator - happens on stuff like: g.V().iterate()
        if (!itty.hasNext()) {
            final Map<String, Object> attributes = generateStatusAttributes(nettyContext, msg, ResponseStatusCode.NO_CONTENT, itty, settings);
            // as there is nothing left to iterate if we are transaction managed then we should execute a
            // commit here before we send back a NO_CONTENT which implies success
            if (managedTransactionsForRequest) attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);
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
                if (managedTransactionsForRequest) {
                    attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
                }
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
                    Frame frame = null;
                    try {
                        frame = makeFrame(context, msg, serializer, useBinary, aggregate, code,
                                generateResultMetaData(nettyContext, msg, code, itty, settings),
                                generateStatusAttributes(nettyContext, msg, code, itty, settings));
                    } catch (Exception ex) {
                        // a frame may use a Bytebuf which is a countable release - if it does not get written
                        // downstream it needs to be released here
                        if (frame != null) frame.tryRelease();

                        // exception is handled in makeFrame() - serialization error gets written back to driver
                        // at that point
                        if (managedTransactionsForRequest) attemptRollback(msg, context.getGraphManager(), settings.strictTransactionManagement);
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
                                attemptCommit(msg, context.getGraphManager(), settings.strictTransactionManagement);

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

                    if (!moreInIterator) iterateComplete(nettyContext, msg, itty);

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

    /**
     * Called when iteration within {@link #handleIterator(Context, Iterator)} is on its final pass and the final
     * frame is about to be sent back to the client. This method only gets called on successful iteration of the
     * entire result.
     */
    protected void iterateComplete(final ChannelHandlerContext ctx, final RequestMessage msg, final Iterator itty) {
        // do nothing by default
    }

    /**
     * Determines if a {@link Frame} should be force flushed outside of the {@code resultIterationBatchSize} and the
     * termination of the iterator. By default this method return {@code false}.
     *
     * @param itty a reference to the current {@link Iterator} of results - it is not meant to be forwarded in
     *             this method
     */
    protected boolean isForceFlushed(final ChannelHandlerContext ctx, final RequestMessage msg, final Iterator itty) {
        return false;
    }

    /**
     * Generates response result meta-data to put on a {@link ResponseMessage}.
     *
     * @param itty a reference to the current {@link Iterator} of results - it is not meant to be forwarded in
     *             this method
     */
    protected Map<String, Object> generateResultMetaData(final ChannelHandlerContext ctx, final RequestMessage msg,
                                                         final ResponseStatusCode code, final Iterator itty,
                                                         final Settings settings) {
        return Collections.emptyMap();
    }

    /**
     * Generates response status meta-data to put on a {@link ResponseMessage}.
     *
     * @param itty a reference to the current {@link Iterator} of results - it is not meant to be forwarded in
     *             this method
     */
    protected Map<String, Object> generateStatusAttributes(final ChannelHandlerContext ctx, final RequestMessage msg,
                                                           final ResponseStatusCode code, final Iterator itty,
                                                           final Settings settings) {
        // only return server metadata on the last message
        if (itty.hasNext()) return Collections.emptyMap();

        final Map<String, Object> metaData = new HashMap<>();
        metaData.put(Tokens.ARGS_HOST, ctx.channel().remoteAddress().toString());

        return metaData;
    }

    protected static Frame makeFrame(final Context ctx, final RequestMessage msg,
                                     final MessageSerializer<?> serializer, final boolean useBinary, final List<Object> aggregate,
                                     final ResponseStatusCode code, final Map<String,Object> responseMetaData,
                                     final Map<String,Object> statusAttributes) throws Exception {
        try {
            final ChannelHandlerContext nettyContext = ctx.getChannelHandlerContext();

            if (useBinary) {
                return new Frame(serializer.serializeResponseAsBinary(ResponseMessage.build(msg)
                        .code(code)
                        .statusAttributes(statusAttributes)
                        .responseMetaData(responseMetaData)
                        .result(aggregate).create(), nettyContext.alloc()));
            } else {
                // the expectation is that the GremlinTextRequestDecoder will have placed a MessageTextSerializer
                // instance on the channel.
                final MessageTextSerializer<?> textSerializer = (MessageTextSerializer<?>) serializer;
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
            ctx.writeAndFlush(error);
            throw ex;
        }
    }

    protected static void attemptCommit(final RequestMessage msg, final GraphManager graphManager, final boolean strict) {
        if (strict) {
            if (msg.getArgs().containsKey(Tokens.ARGS_ALIASES)) {
                final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(Tokens.ARGS_ALIASES);
                graphManager.commit(new HashSet<>(aliases.values()));
            } else {
                graphManager.commitAll();
            }
        } else {
            graphManager.commitAll();
        }
    }

    protected static void attemptRollback(final RequestMessage msg, final GraphManager graphManager, final boolean strict) {
        if (strict) {
            if (msg.getArgs().containsKey(Tokens.ARGS_ALIASES)) {
                final Map<String, String> aliases = (Map<String, String>) msg.getArgs().get(Tokens.ARGS_ALIASES);
                graphManager.rollback(new HashSet<>(aliases.values()));
            } else {
                graphManager.rollbackAll();
            }
        } else {
            graphManager.rollbackAll();
        }
    }
}
