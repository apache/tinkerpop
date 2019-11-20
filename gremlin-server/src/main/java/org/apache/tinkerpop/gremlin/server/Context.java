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
package org.apache.tinkerpop.gremlin.server;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import io.netty.channel.ChannelHandlerContext;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The context of Gremlin Server within which a particular request is made.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class Context {
    private static final Logger logger = LoggerFactory.getLogger(Context.class);
    private final RequestMessage requestMessage;
    private final ChannelHandlerContext channelHandlerContext;
    private final Settings settings;
    private final GraphManager graphManager;
    private final GremlinExecutor gremlinExecutor;
    private final ScheduledExecutorService scheduledExecutorService;
    private final AtomicBoolean finalResponseWritten = new AtomicBoolean();

    public Context(final RequestMessage requestMessage, final ChannelHandlerContext ctx,
                   final Settings settings, final GraphManager graphManager,
                   final GremlinExecutor gremlinExecutor, final ScheduledExecutorService scheduledExecutorService) {
        this.requestMessage = requestMessage;
        this.channelHandlerContext = ctx;
        this.settings = settings;
        this.graphManager = graphManager;
        this.gremlinExecutor = gremlinExecutor;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public ScheduledExecutorService getScheduledExecutorService() {
        return scheduledExecutorService;
    }

    /**
     * Gets the current request to Gremlin Server.
     */
    public RequestMessage getRequestMessage() {
        return requestMessage;
    }

    /**
     * Gets the Netty context.
     */
    public ChannelHandlerContext getChannelHandlerContext() {
        return channelHandlerContext;
    }


    /**
     * Gets the current configuration of Gremlin Server.
     */
    public Settings getSettings() {
        return settings;
    }

    /**
     * Gets the set of {@link Graph} objects configured in Gremlin Server.
     */
    public GraphManager getGraphManager() {
        return graphManager;
    }

    /**
     * Gets the executor chosen to evaluate incoming Gremlin scripts based on the request.
     */
    public GremlinExecutor getGremlinExecutor() {
        return gremlinExecutor;
    }

    /**
     * Writes a response message to the underlying channel while ensuring that at most one
     * {@link ResponseStatusCode#isFinalResponse() final} response is written.
     * <p>Note: this method should be used instead of writing to the channel directly when multiple threads
     * are expected to produce response messages concurrently.</p>
     * <p>Attempts to write more than one final response message will be ignored.</p>
     * @see #writeAndFlush(ResponseStatusCode, Object)
     */
    public void writeAndFlush(final ResponseMessage message) {
        writeAndFlush(message.getStatus().getCode(), message);
    }

    /**
     * Writes a response message to the underlying channel while ensuring that at most one
     * {@link ResponseStatusCode#isFinalResponse() final} response is written.
     * <p>The caller must make sure that the provided response status code matches the content of the message.</p>
     * <p>Note: this method should be used instead of writing to the channel directly when multiple threads
     * are expected to produce response messages concurrently.</p>
     * <p>Attempts to write more than one final response message will be ignored.</p>
     * @see #writeAndFlush(ResponseMessage)
     */
    public void writeAndFlush(final ResponseStatusCode code, final Object responseMessage) {
        final boolean messageIsFinal = code.isFinalResponse();
        if(finalResponseWritten.compareAndSet(false, messageIsFinal)) {
            this.getChannelHandlerContext().writeAndFlush(responseMessage);
        } else {
            if (responseMessage instanceof Frame) {
                ((Frame) responseMessage).tryRelease();
            }

            final String logMessage = String.format("Another final response message was already written for request %s, ignoring response code: %s",
                                                    this.getRequestMessage().getRequestId(), code);
            logger.warn(logMessage);
        }

    }
}
