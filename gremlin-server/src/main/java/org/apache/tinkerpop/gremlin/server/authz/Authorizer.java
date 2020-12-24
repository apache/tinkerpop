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

import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;

import java.util.Map;


/**
 * Provides the interface for authorizing a user per request.
 *
 * @author Marc de Lignie
 */
public interface Authorizer {
    /**
     * This method is called once upon system startup to initialize the {@code Authorizer}.
     */
    public void setup(final Map<String,Object> config) throws AuthorizationException;

    /**
     * Checks whether a user is authorized to have a gremlin bytecode request from a client answered and raises an
     * {@link AuthorizationException} if this is not the case. The returned bytecde is used for further processing of
     * the request.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param bytecode The gremlin {@link Bytecode} request to authorize the user for.
     * @param aliases A {@link Map} with a single key/value pair that maps the name of the {@link TraversalSource} in the
     *                {@link Bytecode} request to name of one configured in Gremlin Server.
     * @return The original or modified {@link Bytecode} to be used for further processing.
     */
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) throws AuthorizationException;

    /**
     * Checks whether a user is authorized to have a script request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param msg {@link RequestMessage} in which the {@link org.apache.tinkerpop.gremlin.driver.Tokens}.ARGS_GREMLIN argument can contain an arbitrary succession of script statements.
     */
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException;

}
