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

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.op.session.Session;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.ScheduledFuture;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Keys used in the various handlers to store state in the pipeline.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class StateKey {

    private StateKey() {}

    /**
     * The key for the current serializer requested by the client.
     */
    public static final AttributeKey<MessageSerializer<?>> SERIALIZER = AttributeKey.valueOf("serializer");

    /**
     * The key to indicate if the serializer should use its binary format.
     */
    public static final AttributeKey<Boolean> USE_BINARY = AttributeKey.valueOf("useBinary");

    /**
     * The key for the current {@link Session} object.
     */
    public static final AttributeKey<Session> SESSION = AttributeKey.valueOf("session");

    /**
     * The key for the current SASL negotiator.
     */
    public static final AttributeKey<Authenticator.SaslNegotiator> NEGOTIATOR = AttributeKey.valueOf("negotiator");

    /**
     * The key for the current request.
     */
    public static final AttributeKey<RequestMessage> REQUEST_MESSAGE = AttributeKey.valueOf("request");

    /**
     * The key for the deferred requests.
     */
    public static final AttributeKey<Pair<LocalDateTime, List<RequestMessage>>> DEFERRED_REQUEST_MESSAGES = AttributeKey.valueOf("deferredRequests");

    /**
     * The key for the current {@link AuthenticatedUser}.
     */
    public static final AttributeKey<AuthenticatedUser> AUTHENTICATED_USER = AttributeKey.valueOf("authenticatedUser");

    /**
     * The key for the size in bytes of the frame the current request was decoded from. Not public by design.
     */
    static final AttributeKey<Integer> REQUEST_SIZE = AttributeKey.valueOf("requestSize");

    /**
     * The key for the running total of the sizes of the requests retained pending authentication. Not public by design.
     */
    static final AttributeKey<Long> DEFERRED_REQUEST_BYTES = AttributeKey.valueOf("deferredRequestBytes");

    /**
     * The key for the task that closes the channel if authentication does not complete in time. Not public by design.
     */
    static final AttributeKey<ScheduledFuture<?>> PREAUTH_DEADLINE = AttributeKey.valueOf("preAuthDeadline");
}
