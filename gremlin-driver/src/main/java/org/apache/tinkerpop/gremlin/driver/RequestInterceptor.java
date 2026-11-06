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
package org.apache.tinkerpop.gremlin.driver;

/**
 * Interceptors are run as an ordered list to allow modification of the {@link HttpRequest} before it is sent to the
 * server. The interceptor receives an {@link HttpRequest} whose body starts as a
 * {@link org.apache.tinkerpop.gremlin.util.message.RequestMessage}. Interceptors mutate the request in place.
 * After all interceptors run, if the body is still a {@code RequestMessage} the driver will auto-serialize it to JSON.
 * Interceptors that need the serialized bytes (e.g., for signing) should call {@link HttpRequest#serializeBody()}.
 */
@FunctionalInterface
public interface RequestInterceptor {

    /**
     * Intercept and mutate the HTTP request before it is sent.
     *
     * @param httpRequest the mutable HTTP request
     */
    void intercept(HttpRequest httpRequest);
}
