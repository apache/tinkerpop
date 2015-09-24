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

import java.util.HashMap;
import java.util.Map;

/**
 * Properties to supply to the {@link Cluster} for authentication purposes.
 */
public class AuthProperties {

    /**
     * An enum of the available authorization properties.
     */
    public enum Property {
        /**
         * The username.
         */
        USERNAME,

        /**
         * The password.
         */
        PASSWORD,

        /**
         * The protocol for which the authentication is being performed (e.g., "ldap").
         */
        PROTOCOL,

        /**
         * The name used as the index into the configuration for the {@code LoginContext}.
         */
        JAAS_ENTRY
    }

    private final Map<Property, String> properties = new HashMap<>();

    /**
     * Adds a {@link Property} with value to the authorization property set.
     */
    public AuthProperties with(final Property key, final String value) {
        properties.put(key, value);
        return this;
    }

    /**
     * Gets a property given the key.
     */
    public String get(final Property key) {
        return properties.get(key);
    }
}