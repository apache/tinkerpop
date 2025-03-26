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

import io.netty.handler.codec.http.HttpHeaders;
import io.netty.util.AttributeKey;
import org.apache.tinkerpop.gremlin.util.MessageSerializer;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.javatuples.Pair;

import java.util.UUID;

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
    public static final AttributeKey<Pair<String, MessageSerializer<?>>> SERIALIZER = AttributeKey.valueOf("serializer");

    /**
     * The key for the current request headers.
     */
    public static final AttributeKey<HttpHeaders> REQUEST_HEADERS = AttributeKey.valueOf("requestHeaders");

    /**
     * The key for the current request ID.
     */
    public static final AttributeKey<UUID> REQUEST_ID = AttributeKey.valueOf("requestId");

    /**
     * The key for whether a {@link io.netty.handler.codec.http.HttpResponse} has been sent for the current response.
     */
    public static final AttributeKey<Boolean> HTTP_RESPONSE_SENT = AttributeKey.valueOf("responseSent");

    /**
     * The key for the current {@link AuthenticatedUser}.
     */
    public static final AttributeKey<AuthenticatedUser> AUTHENTICATED_USER = AttributeKey.valueOf("authenticatedUser");
}
