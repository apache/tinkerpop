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

import org.apache.tinkerpop.gremlin.driver.MessageSerializer;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.Authenticator;
import org.apache.tinkerpop.gremlin.server.op.session.Session;
import io.netty.util.AttributeKey;

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
     * The key for the current {@link AuthenticatedUser}.
     */
    public static final AttributeKey<AuthenticatedUser> AUTHENTICATED_USER = AttributeKey.valueOf("authenticatedUser");
}
