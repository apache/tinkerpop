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
package org.apache.tinkerpop.gremlin.server.auth;

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.server.Channelizer;

import java.net.InetAddress;
import java.util.Map;

/**
 * Provides methods related to authentication of a request.  Implementations should provide a SASL based
 * authentication method, but a handler can choose to use the {@link #authenticate(Map)} method directly if
 * required for protocols that don't easily support SASL.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface Authenticator {
    /**
     * Whether or not the authenticator requires explicit login.
     * If false will instantiate user with AuthenticatedUser.ANONYMOUS_USER.
     */
    public boolean requireAuthentication();

    /**
     * Setup is called once upon system startup to initialize the {@code Authenticator}.
     */
    public void setup(final Map<String,Object> config);

    /**
     * Provide a SASL handler to perform authentication for an single connection. SASL is a stateful protocol, so
     * a new instance must be used for each authentication attempt.
     *
     * @param remoteAddress the IP address of the client to authenticate to authenticate or null if an internal
     *                      client (one not connected over the remote transport).
     */
    public SaslNegotiator newSaslNegotiator(final InetAddress remoteAddress);

    /**
     * A "standard" authentication implementation that can be used more generically without SASL support.  This
     * implementation is used when a particular {@link Channelizer} doesn't support SASL directly (like basic
     * HTTP authentication).
     */
    public AuthenticatedUser authenticate(final Map<String, String> credentials) throws AuthenticationException;

    /**
     * Performs the actual SASL negotiation for a single authentication attempt.
     * SASL is stateful, so a new instance should be used for each attempt.
     * Non-trivial implementations may delegate to an instance of {@link javax.security.sasl.SaslServer}
     */
    public interface SaslNegotiator
    {
        /**
         * Evaluates the client response data and generates a byte[] reply which may be a further challenge or purely
         * informational in the case that the negotiation is completed on this round.
         *
         * This method is called each time a {@link RequestMessage} with an "op" code of "authenticate" is received
         * from a client. After it is called, {@link #isComplete()} is checked to determine whether the negotiation has
         * finished. If so, an {@link AuthenticatedUser} is obtained by calling {@link #getAuthenticatedUser()} and
         * that user associated with the active connection. If the negotiation is not yet complete,
         * the byte[] is returned to the client as a further challenge in an
         * {@link ResponseMessage} with {@link ResponseStatusCode#AUTHENTICATE}. This continues until the negotiation
         * does complete or an error is encountered.
         */
        public byte[] evaluateResponse(final byte[] clientResponse) throws AuthenticationException;

        /**
         * Called after each invocation of {@link #evaluateResponse(byte[])} to determine whether the  authentication has
         * completed successfully or should be continued.
         *
         * @return true if the authentication exchange has completed; false otherwise.
         */
        public boolean isComplete();

        /**
         * Following a successful negotiation, get the AuthenticatedUser representing the logged in subject.
         * This method should only be called if {@link #isComplete()} returns true.
         * Should never return null - always throw AuthenticationException instead.
         * Returning AuthenticatedUser.ANONYMOUS_USER is an option if authentication is not required.
         */
        public AuthenticatedUser getAuthenticatedUser() throws AuthenticationException;
    }
}
