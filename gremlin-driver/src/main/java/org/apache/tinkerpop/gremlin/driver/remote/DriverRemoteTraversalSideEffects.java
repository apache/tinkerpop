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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Host;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.message.RequestMessage;
import org.apache.tinkerpop.gremlin.process.remote.traversal.AbstractRemoteTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Java driver implementation of {@link TraversalSideEffects}. This class is not thread safe.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @deprecated As of release 3.3.8, not directly replaced, prefer use of {@link GraphTraversal#cap(String, String...)}
 * to return the result as part of the traversal iteration.
 */
@Deprecated
public class DriverRemoteTraversalSideEffects extends AbstractRemoteTraversalSideEffects {

    private final Client client;
    private Set<String> keys = Collections.emptySet();
    private final UUID serverSideEffect;
    private final Host host;

    private final Map<String, Object> sideEffects = new HashMap<>();

    private boolean closed = false;
    private boolean retrievedAllKeys = false;
    private final CompletableFuture<Void> ready;
    private final CompletableFuture<Map<String,Object>> statusAttributes;

    /**
     * @deprecated As of release 3.4.0, replaced by {@link #DriverRemoteTraversalSideEffects(Client, ResultSet)}
     */
    @Deprecated
    public DriverRemoteTraversalSideEffects(final Client client, final UUID serverSideEffect, final Host host,
                                            final CompletableFuture<Void> ready) {
        this.client = client;
        this.serverSideEffect = serverSideEffect;
        this.host = host;
        this.ready = ready;
        this.statusAttributes = CompletableFuture.completedFuture(Collections.emptyMap());
    }

    public DriverRemoteTraversalSideEffects(final Client client, final ResultSet rs) {
        this.client = client;
        this.serverSideEffect = rs.getOriginalRequestMessage().getRequestId();
        this.host = rs.getHost();
        this.ready = rs.allItemsAvailableAsync();
        this.statusAttributes = rs.statusAttributes();
    }

    /**
     * Gets the status attributes from the response from the server. This method will block until all results have
     * been retrieved.
     */
    public Map<String,Object> statusAttributes() {
        // wait for the read to complete (i.e. iteration on the server) before allowing the caller to get the
        // attribute. simply following the pattern from other methods here for now.
        return statusAttributes.join();
    }

    @Override
    public <V> V get(final String key) throws IllegalArgumentException {
        // wait for the read to complete (i.e. iteration on the server) before allowing the caller to get the
        // side-effect. calling prior to this will result in the side-effect not being found. of course, the
        // bad part here is that the method blocks indefinitely waiting for the result, but it prevents the
        // test failure problems that happen on slower systems. in practice, it's unlikely that a user would
        // try to get a side-effect prior to iteration, but since the API allows it, this at least prevents
        // the error.
        ready.join();

        if (!keys().contains(key)) throw TraversalSideEffects.Exceptions.sideEffectKeyDoesNotExist(key);

        if (!sideEffects.containsKey(key)) {

            if (closed) throw new IllegalStateException("Traversal has been closed - no new side-effects can be retrieved");

            // specify the ARGS_HOST so that the LoadBalancingStrategy is subverted and the connection is forced
            // from the specified host (i.e. the host from the previous request as that host will hold the side-effects)
            final RequestMessage msg = RequestMessage.build(Tokens.OPS_GATHER)
                    .addArg(Tokens.ARGS_SIDE_EFFECT, serverSideEffect)
                    .addArg(Tokens.ARGS_SIDE_EFFECT_KEY, key)
                    .addArg(Tokens.ARGS_HOST, host)
                    .processor("traversal").create();
            try {
                final Result result = client.submitAsync(msg).get().all().get().get(0);
                sideEffects.put(key, null == result ? null : result.getObject());
            } catch (Exception ex) {
                // we use to try to catch "no found" situations returned from the server here and then null the
                // side-effect for the requested key. doesn't seem like there is a need for that now because calls
                // to get() now initially trigger a call the keys() so you would know all of the keys available on
                // the server and would validate them up front throwing sideEffectKeyDoesNotExist(key) which thus
                // produces behavior similar to the non-remote side-effect implementations. if we get an exception
                // here at this point then we likely have a legit error in communicating to the remote server.
                throw new RuntimeException("Could not get side-effect for " + serverSideEffect + " with key of " + key, ex);
            }
        }

        return (V) sideEffects.get(key);
    }

    @Override
    public Set<String> keys() {
        // wait for the read to complete (i.e. iteration on the server) before allowing the caller to get the
        // side-effect. calling prior to this will result in the side-effect not being found. of course, the
        // bad part here is that the method blocks indefinitely waiting for the result, but it prevents the
        // test failure problems that happen on slower systems. in practice, it's unlikely that a user would
        // try to get a side-effect prior to iteration, but since the API allows it, this at least prevents
        // the error.
        ready.join();

        if (closed && !retrievedAllKeys) throw new IllegalStateException("Traversal has been closed - side-effect keys cannot be retrieved");

        if (!retrievedAllKeys) {
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

                // only need to retrieve all keys once
                retrievedAllKeys = true;
            } catch (Exception ex) {
                final Throwable root = ExceptionUtils.getRootCause(ex);
                throw new RuntimeException("Could not get keys", null == root ? ex : root);
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
                throw new RuntimeException("Error on closing side effects", null == root ? ex : root);
            }
        }
    }

    @Override
    public String toString() {
        // have to override the implementation from TraversalSideEffects because it relies on calls to keys() as
        // calling that too early can cause unintended failures (i.e. in the debugger, toString() gets called when
        // introspecting the object from the moment of construction).
        return "sideEffects[size:" + keys.size() + "]";
    }
}
