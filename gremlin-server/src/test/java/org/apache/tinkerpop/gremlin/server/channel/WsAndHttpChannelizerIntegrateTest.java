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


import org.apache.tinkerpop.gremlin.server.auth.SimpleAuthenticator;
import org.apache.tinkerpop.gremlin.server.handler.SaslAndHttpBasicAuthenticationHandler;
import org.apache.tinkerpop.gremlin.server.Settings;

import java.util.Map;
import java.util.HashMap;

public class WsAndHttpChannelizerIntegrateTest extends AbstractGremlinServerChannelizerIntegrateTest {

    @Override
    public String getProtocol() {
        return WS_AND_HTTP;
    }

    @Override
    public String getSecureProtocol() {
        return WSS_AND_HTTPS;
    }

    @Override
    public String getChannelizer() {
        return WsAndHttpChannelizer.class.getName();
    }

    @Override
    public Settings.AuthenticationSettings getAuthSettings() {
        final Settings.AuthenticationSettings authSettings = new Settings.AuthenticationSettings();
        final Map<String,Object> authConfig = new HashMap<>();
        authSettings.authenticator = SimpleAuthenticator.class.getName();
        authSettings.authenticationHandler = SaslAndHttpBasicAuthenticationHandler.class.getName();
        authConfig.put(SimpleAuthenticator.CONFIG_CREDENTIALS_DB, "conf/tinkergraph-credentials.properties");
        authSettings.config = authConfig;

        return authSettings;
    }

}
