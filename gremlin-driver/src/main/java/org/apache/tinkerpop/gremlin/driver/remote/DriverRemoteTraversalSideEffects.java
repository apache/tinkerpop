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
package org.apache.tinkerpop.gremlin.driver.remote;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.remote.traversal.AbstractRemoteTraversalSideEffects;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class DriverRemoteTraversalSideEffects extends AbstractRemoteTraversalSideEffects {

    private final Client client;
    private Set<String> keys = Collections.emptySet();
    private final UUID serverSideEffect;
    private final Host host;

    private final Map<String, Object> sideEffects = new HashMap<>();

    private boolean closed = false;

    public DriverRemoteTraversalSideEffects(final Client client, final UUID serverSideEffect, final Host host) {
        this.client = client;
        this.serverSideEffect = serverSideEffect;
        this.host = host;
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        if (!sideEffects.containsKey(key)) {
            if (!closed) {
                // specify the ARGS_HOST so that the LoadBalancingStrategy is subverted and the connection is forced
                // from the specified host (i.e. the host from the previous request as that host will hold the side-effects)
                final RequestMessage msg = RequestMessage.build(Tokens.OPS_GATHER)
                        .addArg(Tokens.ARGS_SIDE_EFFECT, serverSideEffect)
                        .addArg(Tokens.ARGS_SIDE_EFFECT_KEY, key)
                        .addArg(Tokens.ARGS_HOST, host)
                        .processor("traversal").create();
                try {
                    final Result result = client.submitAsync(msg).get().one();
                    sideEffects.put(key, null == result ? null : result.getObject());
                    if (keys.equals(Collections.emptySet()))
                        keys = new HashSet<>();
                    keys.add(key);
                } catch (Exception ex) {
                    final Throwable root = ExceptionUtils.getRootCause(ex);
                    final String exMsg = null == root ? "" : root.getMessage();
                    if (exMsg.equals("Could not find side-effects for " + serverSideEffect + "."))
                        sideEffects.put(key, null);
                    else
                        throw new RuntimeException("Could not get keys", root);
                }
            } else {
                sideEffects.put(key, null);
            }
        }

        return (V) sideEffects.get(key);
    }

    @Override
    public Set<String> keys() {
        if (!closed) {
            // specify the ARGS_HOST so that the LoadBalancingStrategy is subverted and the connection is forced
            // from the specified host (i.e. the host from the previous request as that host will hold the side-effects)
            final RequestMessage msg = RequestMessage.build(Tokens.OPS_KEYS)
                    .addArg(Tokens.ARGS_SIDE_EFFECT, serverSideEffect)
                    .addArg(Tokens.ARGS_HOST, host)
                    .processor("traversal").create();
            try {
                if (keys.equals(Collections.emptySet()))
                    keys = new HashSet<>();

                client.submitAsync(msg).get().all().get().forEach(r -> keys.add(r.getString()));
            } catch (Exception ex) {
                final Throwable root = ExceptionUtils.getRootCause(ex);
                final String exMsg = null == root ? "" : root.getMessage();
                if (!exMsg.equals("Could not find side-effects for " + serverSideEffect + "."))
                    throw new RuntimeException("Could not get keys", root);
            }
        }

        return keys;
    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            final RequestMessage msg = RequestMessage.build(Tokens.OPS_CLOSE)
                    .addArg(Tokens.ARGS_SIDE_EFFECT, serverSideEffect)
                    .addArg(Tokens.ARGS_HOST, host)
                    .processor("traversal").create();
            try {
                client.submitAsync(msg).get();
                closed = true;
            } catch (Exception ex) {
                final Throwable root = ExceptionUtils.getRootCause(ex);
                throw new RuntimeException("Error on closing side effects", root);
            }
        }
    }
}
