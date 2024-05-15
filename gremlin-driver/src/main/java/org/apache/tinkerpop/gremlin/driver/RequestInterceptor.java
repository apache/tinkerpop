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

import io.netty.handler.codec.http.FullHttpRequest;

import java.util.function.UnaryOperator;

/**
 * This function is called a {@code FullHttpRequest} constructed and allow it to be modified as needed before it is
 * sent to the server. Implementations are supplied to {@link Cluster.Builder#requestInterceptor(RequestInterceptor)}.
 * When this method is called is dependent on the {@link Channelizer} implementation.
 */
public interface RequestInterceptor extends UnaryOperator<FullHttpRequest> {

}
