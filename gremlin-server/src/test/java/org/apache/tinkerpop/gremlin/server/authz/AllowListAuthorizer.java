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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.server.Settings.AuthorizationSettings;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.util.Tokens;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Authorizes a user per request, based on a list that grants access to {@link TraversalSource} instances
 * to gremlin server's sandbox for string requests and lambdas. The {@link AuthorizationSettings}.config must have
 * an authorizationAllowList entry that contains the name of a YAML file.
 * This authorizer is for demonstration purposes only. It does not scale well in the number of users regarding
 * memory usage and administrative burden.
 *
 * @author Marc de Lignie
 */
public class AllowListAuthorizer implements Authorizer {

    public static final String SANDBOX = "sandbox";

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
     * Checks whether a user is authorized to have a script request from a gremlin client answered and raises an
     * {@link AuthorizationException} if this is not the case.
     *
     * @param user {@link AuthenticatedUser} that needs authorization.
     * @param msg {@link RequestMessage} in which the {@link Tokens}.ARGS_GREMLIN argument can contain an arbitrary succession of script statements.
     */
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        if (!usernamesSandbox.contains(user.getName())) {
            throw new AuthorizationException(REJECT_STRING);
        }
    }
}
