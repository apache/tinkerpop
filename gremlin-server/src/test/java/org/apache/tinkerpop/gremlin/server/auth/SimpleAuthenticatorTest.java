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
package org.apache.tinkerpop.gremlin.server.auth;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class SimpleAuthenticatorTest {
    private Authenticator authenticator;

    @Before
    public void setup() {
        authenticator = new SimpleAuthenticator();
    }

    @Test
    public void shouldAlwaysRequireAuthentication() {
        assertTrue(authenticator.requireAuthentication());
    }

    @Test
    public void shouldCreateNewPlainTextSaslNegotiator() {
        final Authenticator.SaslNegotiator negotiator1 = authenticator.newSaslNegotiator(null);
        final Authenticator.SaslNegotiator negotiator2 = authenticator.newSaslNegotiator(null);

        assertNotEquals(negotiator1, negotiator2);
        assertNotEquals(negotiator2, negotiator1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowNullConfigInSetup() {
        authenticator.setup(null);
    }

    @Test
    public void shouldUseTinkerGraphForCredentialsStoreAndSucceed() throws Exception {
        final Map<String,Object> config = new HashMap<>();
        config.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
        authenticator.setup(config);

        final Authenticator.SaslNegotiator negotiator = authenticator.newSaslNegotiator(null);
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final byte[] nul = new byte[] {0};
        stream.write(nul);
        stream.write("stephen".getBytes());
        stream.write(nul);
        stream.write("password".getBytes());

        negotiator.evaluateResponse(stream.toByteArray());
        assertTrue(negotiator.isComplete());
        assertEquals("stephen", negotiator.getAuthenticatedUser().getName());
    }

    @Test(expected = AuthenticationException.class)
    public void shouldUseTinkerGraphForCredentialsStoreAndFail() throws Exception {
        final Map<String,Object> config = new HashMap<>();
        config.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
        authenticator.setup(config);

        final Authenticator.SaslNegotiator negotiator = authenticator.newSaslNegotiator(null);
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        final byte[] nul = new byte[] {0};
        stream.write(nul);
        stream.write("stephen".getBytes());
        stream.write(nul);
        stream.write("bad-password".getBytes());

        negotiator.evaluateResponse(stream.toByteArray());
        assertTrue(negotiator.isComplete());
        negotiator.getAuthenticatedUser();
    }
}
