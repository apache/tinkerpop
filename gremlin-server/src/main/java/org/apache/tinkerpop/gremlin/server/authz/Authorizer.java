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
package org.apache.tinkerpop.gremlin.server.authz;

import io.netty.handler.codec.http.FullHttpMessage;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;

import java.util.Map;


/**
 * Provides the interface for authorizing a user per request.
 *
 * @author Marc de Lignie
 */
public interface Authorizer {
    /**
     * Configure is called once upon system startup to initialize the {@code Authorizer}.
     */
    public void setup(final Map<String,Object> config) throws AuthorizationException;

    /**
     * Checks whether a user is authorized to have a websockets request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} Result from the {@link org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser}, to be used for the authorization.
     * @param msg {@link RequestMessage} to authorize the user for.
     */
    public RequestMessage authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException;

    /**
     * Checks whether a user is authorized to have a http request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} Result from the {@link org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser}, to be used for the authorization.
     * @param msg {@link FullHttpMessage} to authorize the user for.
     */
    public FullHttpMessage authorize(final AuthenticatedUser user, final FullHttpMessage msg) throws AuthorizationException;
}
