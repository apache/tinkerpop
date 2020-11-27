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
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.BytecodeHelper;
import org.apache.tinkerpop.gremlin.server.Settings.AuthorizationSettings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;

import java.util.*;


/**
 * Authorizes a user per request, based on a list that grants access to {@link TraversalSource} instances for
 * bytecode requests and to gremlin server's sandbox for string requests and lambdas. The {@link
 * AuthorizationSettings}.config must have an authorizationAllowList entry that contains the name of a YAML file.
 * This authorizer is for demonstration purposes only. It does not scale well in the number of users regarding
 * memory usage and administrative burden.
 *
 * @author Marc de Lignie
 */
public class AllowListAuthorizer implements Authorizer {

    // Collections derived from the list with allowed users for fast lookups
    private final Map<String, List<String>> usernamesByTraversalSource = new HashMap<>();
    private final Set<String> usernamesSandbox = new HashSet<>();

    public static final String WILDCARD = "*";
    public static final String SANDBOX = "sandbox";
    public static final String REJECT_BYTECODE = "User not authorized for bytecode requests on %s.";
    public static final String REJECT_LAMBDA = "User not authorized for bytecode requests with lambdas on %s.";
    public static final String REJECT_OLAP = "User not authorized for bytecode OLAP requests on %s.";
    public static final String REJECT_STRING = "User not authorized for string-based requests.";
    public static final String KEY_AUTHORIZATION_ALLOWLIST = "authorizationAllowList";

    @Override
    public void setup(final Map<String,Object> config) {
        AllowList allowList;
        final String file = (String) config.get(KEY_AUTHORIZATION_ALLOWLIST);

        try {
            allowList = AllowList.read(file);
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format("Failed to read list with allowed users from %s", file));
        }
        for (Map.Entry<String, List<String>> entry : allowList.grants.entrySet()) {
            if (!entry.getKey().equals(SANDBOX)) {
                usernamesByTraversalSource.put(entry.getKey(), new ArrayList<>());
            }
            for (final String group : entry.getValue()) {
                if (allowList.groups.get(group) == null) {
                    throw new RuntimeException(String.format("Group '%s' not defined in file with allowed users.", group));
                }
                if (entry.getKey().equals(SANDBOX)) {
                    usernamesSandbox.addAll(allowList.groups.get(group));
                } else {
                    usernamesByTraversalSource.get(entry.getKey()).addAll(allowList.groups.get(group));
                }
            }
        }
    }

    /**
     * Checks whether a user is authorized to have a gremlin bytecode request from a client answered and raises an
     * {@link AuthorizationException} if this is not the case. For a request to be authorized the user must either
     * have a grant for the requested {@link TraversalSource}, without using lambdas or OLAP, or have a sandbox grant.
     *
     * @param user {@link AuthenticatedUser} Result from the {@link org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser}, to be used for the authorization.
     * @param bytecode The gremlin {@link Bytecode} request to authorize the user for.
     * @param aliases {@link Map} with a single key/value pair that maps the name of the {@link TraversalSource} in the
     *                    {@link Bytecode} request to name of one configured in Gremlin Server.
     */
    @Override
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) throws AuthorizationException {
        final Set<String> usernames = new HashSet<>();

        for (final String resource: aliases.values()) {
            usernames.addAll(usernamesByTraversalSource.get(resource));
        }
        final boolean userHasTraversalSourceGrant = usernames.contains(user.getName()) || usernames.contains(WILDCARD);
        final boolean userHasSandboxGrant = usernamesSandbox.contains(user.getName()) || usernamesSandbox.contains(WILDCARD);
        final boolean runsLambda = BytecodeHelper.getLambdaLanguage(bytecode).isPresent();
        final boolean runsVertexProgram = BytecodeHelper.findStrategies(bytecode, VertexProgramStrategy.class).hasNext();

        final String rejectMessage;
        if (runsLambda) {
            rejectMessage = REJECT_LAMBDA;
        } else if (runsVertexProgram){
            rejectMessage = REJECT_OLAP;
        } else {
            rejectMessage = REJECT_BYTECODE;
        }
        if ( (!userHasTraversalSourceGrant || runsLambda || runsVertexProgram) && !userHasSandboxGrant) {
            throw new AuthorizationException(String.format(rejectMessage, aliases.values()));
        }
        return bytecode;
    }

    /**
     * Checks whether a user is authorized to have a script request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} to be used for the authorization
     * @param msg {@link RequestMessage} in which the {@link org.apache.tinkerpop.gremlin.driver.Tokens}.ARGS_GREMLIN argument can contain an arbitratry succession of script statements.
     */
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        if (!usernamesSandbox.contains(user.getName())) {
            throw new AuthorizationException(REJECT_STRING);
        }
    }
}
