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
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.verification.VertexProgramRestrictionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;
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

    public static final String SANDBOX = "sandbox";
    public static final String REJECT_BYTECODE = "User not authorized for bytecode requests on %s";
    public static final String REJECT_LAMBDA = "lambdas";
    public static final String REJECT_MUTATE = "the ReadOnlyStrategy";
    public static final String REJECT_OLAP = "the VertexProgramRestrictionStrategy";
    public static final String REJECT_SUBGRAPH = "the SubgraphStrategy";
    public static final String REJECT_STRING = "User not authorized for string-based requests.";
    public static final String KEY_AUTHORIZATION_ALLOWLIST = "authorizationAllowList";

    // Collections derived from the list with allowed users for fast lookups
    private final Map<String, List<String>> usernamesByTraversalSource = new HashMap<>();
    private final Set<String> usernamesSandbox = new HashSet<>();

    /**
     * This method is called once upon system startup to initialize the {@code AllowListAuthorizer}.
     */
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
     * {@link AuthorizationException} if this is not the case. For a request to be authorized, the user must either
     * have a grant for the requested {@link TraversalSource}, without using lambdas, mutating steps or OLAP, or have a
     * sandbox grant.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param bytecode The gremlin {@link Bytecode} request to authorize the user for.
     * @param aliases A {@link Map} with a single key/value pair that maps the name of the {@link TraversalSource} in the
     *                {@link Bytecode} request to name of one configured in Gremlin Server.
     * @return The original or modified {@link Bytecode} to be used for further processing.
     */
    @Override
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) throws AuthorizationException {
        final Set<String> usernames = new HashSet<>();

        for (final String resource: aliases.values()) {
            usernames.addAll(usernamesByTraversalSource.get(resource));
        }
        final boolean userHasTraversalSourceGrant = usernames.contains(user.getName()) || usernames.contains(AuthenticatedUser.ANONYMOUS_USERNAME);
        final boolean userHasSandboxGrant = usernamesSandbox.contains(user.getName()) || usernamesSandbox.contains(AuthenticatedUser.ANONYMOUS_USERNAME);
        final boolean runsLambda = BytecodeHelper.getLambdaLanguage(bytecode).isPresent();
        final boolean touchesReadOnlyStrategy = bytecode.toString().contains(ReadOnlyStrategy.class.getSimpleName());
        final boolean touchesOLAPRestriction = bytecode.toString().contains(VertexProgramRestrictionStrategy.class.getSimpleName());
        // This element becomes obsolete after resolving TINKERPOP-2473 for allowing only a single instance of each traversal strategy.
        final boolean touchesSubgraphStrategy = bytecode.toString().contains(SubgraphStrategy.class.getSimpleName());

        final List<String> rejections = new ArrayList<>();
        if (runsLambda) {
            rejections.add(REJECT_LAMBDA);
        }
        if (touchesReadOnlyStrategy) {
            rejections.add(REJECT_MUTATE);
        }
        if (touchesOLAPRestriction) {
            rejections.add(REJECT_OLAP);
        }
        if (touchesSubgraphStrategy) {
            rejections.add(REJECT_SUBGRAPH);
        }
        String rejectMessage = REJECT_BYTECODE;
        if (rejections.size() > 0) {
            rejectMessage += " using " + String.join(", ", rejections);
        }
        rejectMessage += ".";

        if ( (!userHasTraversalSourceGrant || runsLambda || touchesOLAPRestriction || touchesReadOnlyStrategy || touchesSubgraphStrategy) && !userHasSandboxGrant) {
            throw new AuthorizationException(String.format(rejectMessage, aliases.values()));
        }
        return bytecode;
    }

    /**
     * Checks whether a user is authorized to have a script request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param msg {@link RequestMessage} in which the {@link org.apache.tinkerpop.gremlin.driver.Tokens}.ARGS_GREMLIN argument can contain an arbitrary succession of script statements.
     */
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        if (!usernamesSandbox.contains(user.getName())) {
            throw new AuthorizationException(REJECT_STRING);
        }
    }
}
