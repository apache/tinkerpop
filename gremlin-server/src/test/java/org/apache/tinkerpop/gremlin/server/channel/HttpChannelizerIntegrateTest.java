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
package org.apache.tinkerpop.gremlin.server.channel;

import org.apache.http.NoHttpResponseException;
import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.Settings;

import org.junit.Test;
import org.junit.Assert;

import java.util.Map;
import java.util.HashMap;
import java.net.SocketException;

public class HttpChannelizerIntegrateTest extends AbstractGremlinServerChannelizerIntegrateTest {

    @Override
    public Settings overrideSettings(final Settings settings) {
        super.overrideSettings(settings);
        final String nameOfTest = name.getMethodName();
        if (nameOfTest.equals("shouldBreakOnInvalidAuthenticationHandler") ) {
            settings.authentication = getAuthSettings();
            settings.authentication.authenticationHandler = "Foo.class";
        }
        return settings;
    }

    @Test
    public void shouldBreakOnInvalidAuthenticationHandler() throws Exception {
        final CombinedTestClient client =  new CombinedTestClient(getProtocol());
        try {
            client.sendAndAssert("2+2", 4);
            Assert.fail("An exception should be thrown with an invalid authentication handler");
        } catch (NoHttpResponseException | SocketException e) {
        } finally {
            client.close();
        }
    }

    @Override
    public String getProtocol() {
        return HTTP;
    }

    @Override
    public String getSecureProtocol() {
        return HTTPS;
    }

    @Override
    public String getChannelizer() {
        return HttpChannelizer.class.getName();
    }

    @Override
    public Settings.AuthenticationSettings getAuthSettings() {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        final Map<String,Object> authConfig = new HashMap<>();
        authSettings.authenticator = SimpleAuthenticator.class.getName();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
        authSettings.config = authConfig;

        return authSettings;
    }

}
