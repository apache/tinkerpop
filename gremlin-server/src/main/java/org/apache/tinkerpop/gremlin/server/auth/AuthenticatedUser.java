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

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class AuthenticatedUser {
    public static final String ANONYMOUS_USERNAME = "anonymous";
    public static final AuthenticatedUser ANONYMOUS_USER = new AuthenticatedUser(ANONYMOUS_USERNAME);
    private final String name;

    public AuthenticatedUser(final String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * If {@link Authenticator} doesn't require authentication, this method may return true.
     */
    public boolean isAnonymous() {
        return this == ANONYMOUS_USER;
    }

    @Override
    public String toString() {
        return String.format("{User %s}", name);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)
            return true;

        if (!(o instanceof AuthenticatedUser))
            return false;

        final AuthenticatedUser u = (AuthenticatedUser) o;

        return name.equals(u.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
