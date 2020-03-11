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
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings.AuthorizationSettings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;

import java.util.*;


/**
 * Authorizes a user per request, based on a whitelist that grants access to {@link TraversalSource} instances for
 * bytecode requests and to gremlin server's sandbox for string requests and lambdas. The {@link
 * AuthorizationSettings}.config must have an authorizationAllowList entry that contains the filename of the whitelist.
 * This authorizer is for demonstration purposes only. It does not scale well in the number of users regarding
 * memory usage and administrative burden.
 *
 * @author Marc de Lignie
 */
public class AllowListAuthorizer extends AbstractAuthorizer {

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
     * Authorizes a user for a string-based evaluation request. The request is rejected if the user does not have the "sandbox" grant.
     *
     * @param user {@link AuthenticatedUser} to be used for the authorization
     * @param script String with an arbitratry succession of groovy and gremlin-groovy statements
     * @param msg {@link RequestMessage} to authorize the user for
     */
    @Override
    protected RequestMessage authorizeString(final AuthenticatedUser user, final String script, final RequestMessage msg) throws AuthorizationException {
        if (!usernamesSandbox.contains(user.getName())) {
            throw new AuthorizationException(REJECT_STRING);
        }
        return msg;
    }

    /**
     * Authorizes a user for a gremlin bytecode request. The request is rejected if it contains aliases for {@link TraversalSource}
     * names for which the user has no permission. In addition the request is rejected if it contains
     * lambdas and the user has no "sandbox" permission.
     *
     * @param user {@link AuthenticatedUser} to be used for the authorization
     * @param bytecode {@link Bytecode} request extracted from the msg parameter
     * @param msg RequestMessage to authorize the user for
     */
    @Override
    protected RequestMessage authorizeBytecode(final AuthenticatedUser user, final Bytecode bytecode, final RequestMessage msg) throws AuthorizationException {
        final Map<String, String> aliases = (Map<String, String>) msg.getArgs().getOrDefault(Tokens.ARGS_ALIASES, Collections.emptyMap());
        final Set<String> usernames = new HashSet<>();

        for (final String resource: aliases.values()) {
            usernames.addAll(usernamesByTraversalSource.get(resource));
        }
        final boolean userHasTraversalSourceGrant = usernames.contains(user.getName()) || usernames.contains(WILDCARD);
        final boolean userHasSandboxGrant = usernamesSandbox.contains(user.getName()) || usernamesSandbox.contains(WILDCARD);

        final String rejectMessage;
        if (runsLambda(bytecode)) {
            rejectMessage = REJECT_LAMBDA;
        } else if (runsVertexProgram(bytecode)){
            rejectMessage = REJECT_OLAP;
        } else {
            rejectMessage = REJECT_BYTECODE;
        }
        if ((!userHasTraversalSourceGrant || runsLambda(bytecode) || runsVertexProgram(bytecode)) && !userHasSandboxGrant) {
            throw new AuthorizationException(String.format(rejectMessage, aliases.values()));
        }
        return msg;
    }
}
