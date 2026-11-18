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
package org.apache.tinkerpop.gremlin.driver.handler;

import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

import java.util.Collections;

/**
 * Internal markers shared by the HTTP response handlers.
 */
public final class HttpResponseMarkers {
    /**
     * Signals that all content of the response has been read.
     */
    public static final ResponseMessage LAST_CONTENT_READ_RESPONSE =
            ResponseMessage.build().code(HttpResponseStatus.NO_CONTENT).result(Collections.emptyList()).create();

    private HttpResponseMarkers() {
    }
}
