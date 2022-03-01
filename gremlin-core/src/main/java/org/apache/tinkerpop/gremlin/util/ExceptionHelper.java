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
package org.apache.tinkerpop.gremlin.util;

import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * Utility class with helpful functions for exceptions.
 */
public final class ExceptionHelper {

    private ExceptionHelper() {}

    /**
     * A wrapper to Commons Lang {@code ExceptionUtils.getRootCause(Throwable)} which ensures that the root is either
     * an inner cause to the exception or the exception itself (rather than {@code null}).
     */
    public static Throwable getRootCause(final Throwable t) {
        final Throwable root = ExceptionUtils.getRootCause(t);
        return null == root ? t : root;
    }

    public static String getMessageOrName(final Throwable t) {
        return (null == t.getMessage() || t.getMessage().isEmpty()) ?
                t.getClass().getName() : t.getMessage();
    }

    public static String getMessageFromExceptionOrCause(final Throwable t) {
        return null == t.getCause() ? getMessageOrName(t) : getMessageOrName(t.getCause());
    }
}
