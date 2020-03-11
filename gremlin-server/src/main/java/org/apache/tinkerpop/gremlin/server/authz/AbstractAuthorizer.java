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

import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeUtil;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;


/**
 * Provides utilities for implementing the {@link Authorizer} interface.
 *
 * @author Marc de Lignie
 */
public abstract class AbstractAuthorizer implements Authorizer{
    /**
     * Authorizes a user for a request. The method may return the {@link RequestMessage} unchanged, modify it or throw
     * an {@link AuthorizationException} if the user cannot be authorized for the request.
     *
     * @param user {@link AuthenticatedUser} to be used for the authorization
     * @param msg RequestMessage to authorize the user for
     */
    public RequestMessage authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        switch (msg.getOp()) {
            case Tokens.OPS_EVAL:
                final String script = (String) msg.getArgs().get(Tokens.ARGS_GREMLIN);
                return authorizeString(user, script, msg);
            case Tokens.OPS_BYTECODE:
                final Bytecode bytecode = (Bytecode) msg.getArgs().get(Tokens.ARGS_GREMLIN);
                return authorizeBytecode(user, bytecode, msg);
            default:
                throw new AuthorizationException("This Authorizer only handles requests with OPS_BYTECODE or OPS_EVAL.");
        }
    }

    /**
     * Authorizes a user for a string-based evaluation request.
     *
     * @param user {@link AuthenticatedUser} to be used for the authorization
     * @param script String with an arbitratry succession of groovy and gremlin-groovy statements
     * @param msg {@link RequestMessage} to authorize the user for
     */
    protected RequestMessage authorizeString(final AuthenticatedUser user, final String script, final RequestMessage msg) throws AuthorizationException {
        throw new AuthorizationException("Configured authorizer does not support string-based evaluation requests.");
    }

    /**
     * Authorizes a user for a gremlin bytecode request.
     *
     * @param user {@link AuthenticatedUser} to be used for the authorization
     * @param bytecode {@link Bytecode} request extracted from the msg parameter
     * @param msg RequestMessage to authorize the user for
     */
    protected RequestMessage authorizeBytecode(final AuthenticatedUser user, Bytecode bytecode, final RequestMessage msg) throws AuthorizationException {
        throw new AuthorizationException("Configured authorizer does not support gremlin bytecode requests.");
    }

    /**
     * Inspects a {@link Bytecode} object for the presence of lambda functions.
     *
     * @param bytecode {@link Bytecode} object to inspect
     * @return a boolean with the inpection result.
     */
    protected boolean runsLambda(final Bytecode bytecode) {
        return BytecodeHelper.getLambdaLanguage(bytecode).isPresent();
    }

    /**
     * Inspects a {@link Bytecode} object for the presence of OLAP computations.
     *
     * @param bytecode {@link Bytecode} object to inspect
     * @return a boolean with the inpection result.
     */
    protected boolean runsVertexProgram(final Bytecode bytecode) {
        return BytecodeUtil.findStrategies(bytecode, VertexProgramStrategy.class).hasNext();
    }
}
