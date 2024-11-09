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
package org.apache.tinkerpop.gremlin.server.handler;

import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

/**
 * An exception that holds the error-related {@link ResponseMessage} which is meant to be returned to the calling
 * client.
 */
public class SessionException extends Exception {
    private final ResponseMessage responseMessage;

    public SessionException(final String message, final ResponseMessage responseMessage) {
        super(message);
        this.responseMessage = responseMessage;
    }

    public SessionException(final String message, final Throwable cause, final ResponseMessage responseMessage) {
        super(message, cause);
        this.responseMessage = responseMessage;
    }

    public ResponseMessage getResponseMessage() {
        return this.responseMessage;
    }
}
