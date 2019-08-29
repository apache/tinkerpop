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
package org.apache.tinkerpop.gremlin.driver;

import org.apache.tinkerpop.gremlin.server.AbstractGremlinServerIntegrationTest;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.TestClientFactory;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;

public class ClientAuthenticationIntegrateTest extends AbstractGremlinServerIntegrationTest {

    public static class Authenticator extends SimpleAuthenticator {

        @Override
        public void setup(Map<String, Object> config) {
        }

        @Override
        public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException {
            try {
                Thread.sleep(1000);
                return new AuthenticatedUser(credentials.get("username"));
            } catch (Exception ex) {
                throw new AuthenticationException(ex);
            }
        }
    }

    /**
     * Configure specific Gremlin Server settings for specific tests.
     */
    @Override
    public Settings overrideSettings(final Settings settings) {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        authSettings.authenticator = Authenticator.class.getName();
        authSettings.enableAuditLog = true;
        return settings;
    }

    @Test
    public void shouldBeThreadSafeWithAuthentication() throws Exception {
        final Cluster cluster = TestClientFactory.build()
                .credentials("username", "password")
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(1)
                .create();

        try {
            final Client client = cluster.connect().alias("g");
            final Future<ResultSet> future1 = client.submitAsync("1+1");
            final Future<ResultSet> future2 = client.submitAsync("1+1");
            assertEquals(2, future1.get().one().getInt());
            assertEquals(2, future2.get().one().getInt());
        } finally {
            cluster.close();
        }
    }
}
