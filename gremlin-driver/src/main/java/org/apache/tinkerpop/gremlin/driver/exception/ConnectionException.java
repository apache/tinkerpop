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
package org.apache.tinkerpop.gremlin.driver.exception;

import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;

/**
 * This exception signifies network connection failure.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ConnectionException extends RuntimeException {
    private URI uri;
    private InetSocketAddress address;

    public ConnectionException(final URI uri, final InetSocketAddress addy, final String message) {
        super(message);
        this.address = addy;
        this.uri = uri;
    }

    public ConnectionException(final URI uri, final Throwable cause) {
        super(cause);
        this.uri = uri;
        this.address = null;
    }

    public ConnectionException(final URI uri, final String message, final Throwable cause) {
        super(message, cause);
        this.uri = uri;
        this.address = null;
    }

    public ConnectionException(final URI uri, final InetSocketAddress addy, final String message, final Throwable cause) {
        super(message, cause);
        this.address = addy;
        this.uri = uri;
    }

    public URI getUri() {
        return uri;
    }

    public Optional<InetSocketAddress> getAddress() {
        return Optional.ofNullable(address);
    }
}
