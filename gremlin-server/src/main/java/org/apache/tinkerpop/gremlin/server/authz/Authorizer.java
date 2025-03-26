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

import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

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

    // todo: implement auth for gremlin-lang
    public default String authorize(final AuthenticatedUser user, final String gremlin, final Map<String, String> aliases) throws AuthorizationException {
        return gremlin;
    }

    /**
     * Checks whether a user is authorized to have a script request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param msg {@link RequestMessage} in which the {@link Tokens}.ARGS_GREMLIN argument can contain an arbitrary succession of script statements.
     */
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException;

}
