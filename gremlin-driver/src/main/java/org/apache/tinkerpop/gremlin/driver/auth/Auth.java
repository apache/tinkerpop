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
package org.apache.tinkerpop.gremlin.driver.auth;

import org.apache.tinkerpop.gremlin.driver.RequestInterceptor;
import org.apache.tinkerpop.gremlin.driver.Settings;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

public interface Auth extends RequestInterceptor {
    String AUTH_BASIC = "basic";
    String AUTH_SIGV4 = "sigv4";

    static Auth basic(final String username, final String password) {
        return new Basic(username, password);
    }

    static Auth sigv4(final String regionName, final String serviceName) {
        return new Sigv4(regionName, serviceName);
    }

    static Auth sigv4(final String regionName, final AwsCredentialsProvider awsCredentialsProvider, final String serviceName) {
        return new Sigv4(regionName, awsCredentialsProvider, serviceName);
    }

    static Auth from(Settings.AuthSettings settings) {
        if (settings.type.equals(AUTH_BASIC)) {
            return basic(settings.username, settings.password);
        }
        if (settings.type.equals(AUTH_SIGV4)) {
            return sigv4(settings.region, settings.serviceName);
        }
        throw new IllegalArgumentException("Unknown auth type: " + settings.type);
    }

    class AuthenticationException extends RuntimeException {
        public AuthenticationException(Exception cause) {
            super(cause);
        }
    }
}
