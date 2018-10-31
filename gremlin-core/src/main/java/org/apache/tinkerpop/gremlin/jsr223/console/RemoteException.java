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
package org.apache.tinkerpop.gremlin.jsr223.console;

import java.util.Optional;

/**
 * A mapper {@code Exception} to be thrown when there are problems with processing a command given to a
 * {@link RemoteAcceptor}.  The message provided to the exception will be displayed to the user in the Console.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RemoteException extends Exception {
    private String remoteStackTrace = null;

    public RemoteException(final String message) {
        this(message, (String) null);
    }

    public RemoteException(final String message, final String remoteStackTrace) {
        super(message);
        this.remoteStackTrace = remoteStackTrace;
    }

    public RemoteException(final String message, final Throwable cause) {
        this(message, cause, null);
    }

    public RemoteException(final String message, final Throwable cause, final String remoteStackTrace) {
        super(message, cause);
        this.remoteStackTrace = remoteStackTrace;
    }

    public RemoteException(final Throwable cause) {
        super(cause);
    }

    /**
     * The stacktrace produced by the remote server.
     */
    public Optional<String> getRemoteStackTrace() {
        return Optional.ofNullable(remoteStackTrace);
    }
}
