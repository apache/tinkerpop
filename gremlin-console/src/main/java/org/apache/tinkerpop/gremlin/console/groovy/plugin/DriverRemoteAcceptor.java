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
package org.apache.tinkerpop.gremlin.console.groovy.plugin;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteAcceptor;
import org.apache.tinkerpop.gremlin.groovy.plugin.RemoteException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.codehaus.groovy.tools.shell.Groovysh;

import javax.security.sasl.SaslException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

/**
 * A {@link RemoteAcceptor} that takes input from the console and sends it to Gremlin Server over the standard
 * Java driver.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DriverRemoteAcceptor implements RemoteAcceptor {
    private Cluster currentCluster;
    private Client.ClusteredClient currentClient;
    private int timeout = 180000;
    private Map<String,String> aliases = new HashMap<>();

    private static final String TOKEN_RESET = "reset";
    private static final String TOKEN_SHOW = "show";
    private static final String TOKEN_MAX = "max";
    private static final String TOKEN_TIMEOUT = "timeout";
    private static final String TOKEN_ALIAS = "alias";
    private static final List<String> POSSIBLE_TOKENS = Arrays.asList(TOKEN_TIMEOUT, TOKEN_ALIAS);

    private final Groovysh shell;

    public DriverRemoteAcceptor(final Groovysh shell) {
        this.shell = shell;
    }

    @Override
    public Object connect(final List<String> args) throws RemoteException {
        if (args.size() != 1) throw new RemoteException("Expects the location of a configuration file as an argument");

        try {
            this.currentCluster = Cluster.open(args.get(0));
            this.currentClient = this.currentCluster.connect();
            this.currentClient.init();
            return String.format("Connected - " + this.currentCluster);
        } catch (final FileNotFoundException ignored) {
            throw new RemoteException("The 'connect' option must be accompanied by a valid configuration file");
        } catch (final Exception ex) {
            throw new RemoteException("Error during 'connect' - " + ex.getMessage(), ex);
        }
    }

    @Override
    public Object configure(final List<String> args) throws RemoteException {
        final String option = args.size() == 0 ? "" : args.get(0);
        if (!POSSIBLE_TOKENS.contains(option))
            throw new RemoteException(String.format("The 'config' option expects one of ['%s'] as an argument", String.join(",", POSSIBLE_TOKENS)));

        final List<String> arguments = args.subList(1, args.size());

        if (option.equals(TOKEN_TIMEOUT)) {
            final String errorMessage = "The timeout option expects a positive integer representing milliseconds or 'max' as an argument";
            if (arguments.size() != 1) throw new RemoteException(errorMessage);
            try {
                final int to = arguments.get(0).equals(TOKEN_MAX) ? Integer.MAX_VALUE : Integer.parseInt(arguments.get(0));
                if (to <= 0) throw new RemoteException(errorMessage);
                this.timeout = to;
                return "Set remote timeout to " + to + "ms";
            } catch (Exception ignored) {
                throw new RemoteException(errorMessage);
            }
        } else if (option.equals(TOKEN_ALIAS)) {
            if (arguments.size() == 1 && arguments.get(0).equals(TOKEN_RESET)) {
                aliases.clear();
                return "Aliases cleared";
            }

            if (arguments.size() == 1 && arguments.get(0).equals(TOKEN_SHOW)) {
                return aliases;
            }

            if (arguments.size() % 2 != 0)
                throw new RemoteException("Arguments to alias must be 'reset' to clear any existing alias settings or key/value alias/binding pairs");

            final Map<String,Object> aliasMap = ElementHelper.asMap(arguments.toArray());
            aliases.clear();
            aliasMap.forEach((k,v) -> aliases.put(k, v.toString()));
            return aliases;
        }

        return this.toString();
    }

    @Override
    public Object submit(final List<String> args) throws RemoteException {
        final String line = RemoteAcceptor.getScript(String.join(" ", args), this.shell);

        try {
            final List<Result> resultSet = send(line);
            this.shell.getInterp().getContext().setProperty(RESULT, resultSet);
            return resultSet.stream().map(result -> result.getObject()).iterator();
        } catch (SaslException sasl) {
            throw new RemoteException("Security error - check username/password and related settings", sasl);
        } catch (Exception ex) {
            final Optional<ResponseException> inner = findResponseException(ex);
            if (inner.isPresent()) {
                final ResponseException responseException = inner.get();
                if (responseException.getResponseStatusCode() == ResponseStatusCode.SERVER_ERROR_SERIALIZATION)
                    throw new RemoteException(String.format("Server could not serialize the result requested. Server error - %s. Note that the class must be serializable by the client and server for proper operation.", responseException.getMessage()));
                else
                    throw new RemoteException(responseException.getMessage());
            } else if (ex.getCause() != null) {
                final Throwable rootCause = ExceptionUtils.getRootCause(ex);
                if (rootCause instanceof TimeoutException)
                    throw new RemoteException("Host did not respond in a timely fashion - check the server status and submit again.");
                else
                    throw new RemoteException(rootCause.getMessage());
            } else
                throw new RemoteException(ex.getMessage());
        }
    }

    @Override
    public void close() throws IOException {
        if (this.currentClient != null) this.currentClient.close();
        if (this.currentCluster != null) this.currentCluster.close();
    }

    private List<Result> send(final String gremlin) throws SaslException {
        try {
            return this.currentClient.submitAsync(gremlin, aliases, Collections.emptyMap()).get()
                    .all().get(this.timeout, TimeUnit.MILLISECONDS);
        } catch(TimeoutException ignored) {
            throw new IllegalStateException("Request timed out while processing - increase the timeout with the :remote command");
        } catch (Exception e) {
            // handle security error as-is and unwrapped
            final Optional<Throwable> throwable  = Stream.of(ExceptionUtils.getThrowables(e)).filter(t -> t instanceof SaslException).findFirst();
            if (throwable.isPresent())
                throw (SaslException) throwable.get();

            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        return "Gremlin Server - [" + this.currentCluster + "]";
    }

    private Optional<ResponseException> findResponseException(final Throwable ex) {
        if (ex instanceof ResponseException)
            return Optional.of((ResponseException) ex);

        if (null == ex.getCause())
            return Optional.empty();

        return findResponseException(ex.getCause());
    }
}
