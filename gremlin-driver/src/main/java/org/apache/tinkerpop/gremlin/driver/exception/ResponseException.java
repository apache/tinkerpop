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

import org.apache.tinkerpop.gremlin.driver.message.ResponseStatusCode;

import java.util.List;
import java.util.Optional;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class ResponseException extends Exception {
    private ResponseStatusCode responseStatusCode;
    private String remoteStackTrace = null;
    private List<String> remoteExceptionHierarchy = null;

    public ResponseException(final ResponseStatusCode responseStatusCode, final String serverMessage) {
        super(serverMessage);
        this.responseStatusCode = responseStatusCode;
    }

    public ResponseException(final ResponseStatusCode responseStatusCode, final String serverMessage,
                             final List<String> remoteExceptionHierarchy, final String remoteStackTrace) {
        this(responseStatusCode, serverMessage);
        this.remoteExceptionHierarchy = remoteExceptionHierarchy;
        this.remoteStackTrace = remoteStackTrace;
    }

    public ResponseStatusCode getResponseStatusCode() {
        return responseStatusCode;
    }

    public Optional<String> getRemoteStackTrace() {
        return Optional.ofNullable(remoteStackTrace);
    }

    public Optional<List<String>> getRemoteExceptionHierarchy() {
        return Optional.ofNullable(remoteExceptionHierarchy);
    }
}