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

import io.netty.channel.Channel;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.server.Context;

import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;

/**
 * The {@code Rexster} interface is essentially a form of "worker", named for Gremlin's trusty canine robot friend
 * who represents the "server" aspect of TinkerPop. In the most generic sense, {@code Rexster} implementations take
 * tasks, a Gremlin Server {@link Context} and process them, which typically means "execute some Gremlin".
 * Implementations have a fair amount of flexibility in terms of how they go about doing this, but in most cases,
 * the {@link SingleRexster} and the {@link MultiRexster} should handle most cases quite well and are extensible for
 * providers.
 */
public interface Rexster extends Runnable {
    /**
     * Adds a task for Rexster to complete.
     */
    void addTask(final Context gremlinContext);

    /**
     * Sets a reference to the job that will cancel this Rexster if it exceeds its timeout period.
     */
    void setSessionCancelFuture(final ScheduledFuture<?> f);

    /**
     * Sets a reference to the job itself that is running this Rexster.
     */
    void setSessionFuture(final Future<?> f);

    /**
     * Sets a reference to the all powerful thread that is running this Rexster.
     */
    void setSessionThread(final Thread t);

    /**
     * Provides a general way to tell Rexster that it has exceeded some timeout condition.
     */
    void triggerTimeout(final long timeout, boolean causedBySession);

    /**
     * Determines if the supplied {@code Channel} object is the same as the one bound to the {@code Session}.
     */
    boolean isBoundTo(final Channel channel);

    /**
     * Determins if this Rexster can accept additional tasks or not.
     */
    boolean acceptingTasks();

    public class RexsterException extends Exception {
        private final ResponseMessage responseMessage;

        public RexsterException(final String message, final ResponseMessage responseMessage) {
            super(message);
            this.responseMessage = responseMessage;
        }

        public RexsterException(final String message, final Throwable cause, final ResponseMessage responseMessage) {
            super(message, cause);
            this.responseMessage = responseMessage;
        }

        public ResponseMessage getResponseMessage() {
            return this.responseMessage;
        }
    }
}
