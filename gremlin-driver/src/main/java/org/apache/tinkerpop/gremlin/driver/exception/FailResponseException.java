/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.driver.exception;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;

import java.util.Collections;
import java.util.Map;

/**
 * Provides a {@link Failure} implementation for {@link ResponseException}. This exception is thrown instead of
 * a {@code ResponseException} when the server response carries an {@code exception} of {@link #EXCEPTION_NAME} which
 * indicates that a step in the traversal failed by way of {@link GraphTraversal#fail()}. This approach helps make
 * remote exception handling for that step more consistent with the local {@link GraphTraversal#fail()} behavior.
 */
public class FailResponseException extends ResponseException implements Failure {

    /**
     * The {@code exception} name returned in a server response when a {@link GraphTraversal#fail()} step is triggered.
     */
    public static final String EXCEPTION_NAME = "ServerFailStepException";

    public FailResponseException(final HttpResponseStatus responseStatusCode, final String serverMessage) {
        super(responseStatusCode, serverMessage, EXCEPTION_NAME);
    }

    @Override
    public Map<String, Object> getMetadata() {
        return Collections.emptyMap();
    }

    @Override
    public Traverser.Admin getTraverser() {
        return null;
    }

    @Override
    public Traversal.Admin getTraversal() {
        return null;
    }
}
