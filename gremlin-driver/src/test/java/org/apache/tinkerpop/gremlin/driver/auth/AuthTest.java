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
package org.apache.tinkerpop.gremlin.driver.auth;

import org.apache.tinkerpop.gremlin.driver.Settings;
import org.junit.Test;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link Auth} factory dispatch. {@link Auth#from(Settings.AuthSettings)} selects the concrete
 * interceptor implementation from the configured {@code type}, so this verifies each supported type maps to the
 * right implementation and that an unrecognized type is rejected with a clear message.
 */
public class AuthTest {

    @Test
    public void shouldCreateBasicAuthFromSettings() {
        final Settings.AuthSettings settings = new Settings.AuthSettings();
        settings.type = Auth.AUTH_BASIC;
        settings.username = "user";
        settings.password = "secret";
        assertTrue(Auth.from(settings) instanceof Basic);
    }

    @Test
    public void shouldCreateSigv4AuthFromSettings() {
        final Settings.AuthSettings settings = new Settings.AuthSettings();
        settings.type = Auth.AUTH_SIGV4;
        settings.region = "us-east-1";
        settings.serviceName = "neptune-db";
        assertTrue(Auth.from(settings) instanceof Sigv4);
    }

    @Test
    public void shouldThrowForUnknownAuthType() {
        final Settings.AuthSettings settings = new Settings.AuthSettings();
        settings.type = "bogus";
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> Auth.from(settings));
        org.junit.Assert.assertEquals("Unknown auth type: bogus", ex.getMessage());
    }

    @Test
    public void shouldThrowForDefaultEmptyAuthType() {
        // AuthSettings.type defaults to the empty string, which is not a recognized type
        final IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> Auth.from(new Settings.AuthSettings()));
        org.junit.Assert.assertEquals("Unknown auth type: ", ex.getMessage());
    }

}
