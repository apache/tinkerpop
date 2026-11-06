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
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FailResponseExceptionTest {

    @Test
    public void shouldBeAResponseExceptionAndFailure() {
        final FailResponseException ex = new FailResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "make it stop");
        assertThat(ex, instanceOf(ResponseException.class));
        assertThat(ex, instanceOf(Failure.class));
        assertEquals(HttpResponseStatus.INTERNAL_SERVER_ERROR, ex.getResponseStatusCode());
        assertEquals(FailResponseException.EXCEPTION_NAME, ex.getRemoteException());
        assertEquals("make it stop", ex.getMessage());
    }

    @Test
    public void shouldCreateFailResponseExceptionWhenRemoteExceptionIsFailStep() {
        final ResponseException ex = ResponseException.create(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "make it stop", FailResponseException.EXCEPTION_NAME);
        assertThat(ex, instanceOf(FailResponseException.class));
        assertThat(ex, instanceOf(Failure.class));
    }

    @Test
    public void shouldCreatePlainResponseExceptionWhenRemoteExceptionIsNotFailStep() {
        final ResponseException ex = ResponseException.create(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "boom", "ServerErrorException");
        assertThat(ex, instanceOf(ResponseException.class));
        assertThat(ex, not(instanceOf(FailResponseException.class)));
    }

    @Test
    public void shouldReturnEmptyFailureContextSinceItIsNotTransmittedFromServer() {
        final FailResponseException ex = new FailResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "make it stop");
        assertTrue(ex.getMetadata().isEmpty());
        assertNull(ex.getTraverser());
        assertNull(ex.getTraversal());
    }

    @Test
    public void shouldFormatWithoutThrowingWhenFailureContextIsAbsent() {
        final FailResponseException ex = new FailResponseException(HttpResponseStatus.INTERNAL_SERVER_ERROR, "make it stop");

        // format() must not NPE even though getTraversal()/getTraverser() return null for a remotely
        // reconstructed Failure
        final String formatted = ex.format();

        assertNotNull(formatted);
        assertThat(formatted, containsString("fail() Step Triggered"));
        assertThat(formatted, containsString("Message  > make it stop"));
        assertThat(formatted, containsString("Metadata > {}"));
    }
}
