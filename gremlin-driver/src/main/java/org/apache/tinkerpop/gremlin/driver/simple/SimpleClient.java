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
package org.apache.tinkerpop.gremlin.driver.simple;

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;

import java.io.Closeable;
import java.util.function.Consumer;

/**
 * Interface for a simple implementation of a client for Gremlin Server.  It is meant largely for testing purposes
 * and very simple scenarios where it is better to be closer to the {@link RequestMessage} and
 * {@link ResponseMessage}.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface SimpleClient extends Closeable {

    /**
     * Helper method for constructing a {@link RequestMessage} that requests an evaluation of a Gremlin query.
     *
     * @param gremlin the query to execute
     * @param callback the callback that occurs when the result arrives.
     */
    public default void submit(final String gremlin, final Consumer<ResponseMessage> callback) throws Exception {
        submit(RequestMessage.build(Tokens.OPS_EVAL).addArg(Tokens.ARGS_GREMLIN, gremlin).create(), callback);
    }

    /**
     * Sends a {@link RequestMessage} to the server.
     *
     * @param requestMessage the message to send
     * @param callback the callback that occurs when the result arrives.
     */
    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception;
}
