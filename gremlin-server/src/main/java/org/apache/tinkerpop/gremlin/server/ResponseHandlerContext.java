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

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.handler.Frame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A context for asynchronously writing response messages related to a particular request.
 * <p>The "write" methods of this class ensure that at most one {@link ResponseStatusCode#isFinalResponse() final}
 * response message is written to the underlying channel. Attempts to write more than one final response message will
 * be ignored with a warning log message.</p>
 * <p>Note: an object of this class should be used instead of writing to the channel directly when multiple threads
 * are expected to produce final response messages concurrently. Callers must ensure that the same
 * {@link ResponseHandlerContext} is used by all threads writing response messages for the same request.</p>
 *
 * @author Dmitri Bourlatchkov
 * @deprecated As of release 3.3.8, replaced by {@link Context}.
 */
@Deprecated
public class ResponseHandlerContext {
    private static final Logger logger = LoggerFactory.getLogger(ResponseHandlerContext.class);

    private final Context context;
    private final AtomicBoolean finalResponseWritten = new AtomicBoolean();

    public ResponseHandlerContext(final Context context) {
        this.context = context;
    }

    public Context getContext() {
        return context;
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
            context.getChannelHandlerContext().writeAndFlush(responseMessage);
        } else {
            if (responseMessage instanceof Frame) {
                ((Frame) responseMessage).tryRelease();
            }

            final String logMessage = String.format("Another final response message was already written for request %s, ignoring response code: %s",
                    context.getRequestMessage().getRequestId(), code);
            logger.warn(logMessage);
        }

    }
}