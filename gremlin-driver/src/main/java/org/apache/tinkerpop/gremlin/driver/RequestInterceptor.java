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

import java.util.function.UnaryOperator;

/**
 * Interceptors are run as a list to allow modification of the HTTP request before it is sent to the server. The first
 * interceptor will be provided with a {@link HttpRequest} that holds a
 * {@link org.apache.tinkerpop.gremlin.util.message.RequestMessage} in the body. The final interceptor should contain a
 * {@code byte[]} in the body.
 */
public interface RequestInterceptor extends UnaryOperator<HttpRequest> {

}
