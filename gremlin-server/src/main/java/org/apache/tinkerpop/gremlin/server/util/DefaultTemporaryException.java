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
package org.apache.tinkerpop.gremlin.server.util;

import org.apache.tinkerpop.gremlin.structure.util.TemporaryException;

/**
 * This is a default {@link TemporaryException} implementation that server implementers could use if they wished
 * to indicate retry operations to the client. It is an overly simple class meant more for testing than be extended and
 * providers are encouraged to tag their own exceptions more directly with the {@link TemporaryException} interface
 * or to develop their own.
 */
public final class DefaultTemporaryException extends Exception implements TemporaryException {
    public DefaultTemporaryException() {
    }

    public DefaultTemporaryException(final String message) {
        super(message);
    }

    public DefaultTemporaryException(final String message, final Throwable cause) {
        super(message, cause);
    }

    public DefaultTemporaryException(final Throwable cause) {
        super(cause);
    }

    public DefaultTemporaryException(final String message, final Throwable cause, final boolean enableSuppression,
                                     final boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
