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
package org.apache.tinkerpop.gremlin.driver.simple;

import org.apache.tinkerpop.gremlin.util.message.RequestMessage;
import org.apache.tinkerpop.gremlin.util.message.ResponseMessage;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public interface SimpleClient extends Closeable {

    public default void submit(final String gremlin, final Consumer<ResponseMessage> callback) throws Exception {
        submit(RequestMessage.build(gremlin).create(), callback);
    }

    public void submit(final RequestMessage requestMessage, final Consumer<ResponseMessage> callback) throws Exception;

    public default List<ResponseMessage> submit(final String gremlin) throws Exception {
        return submit(RequestMessage.build(gremlin).create());
    }

    public List<ResponseMessage> submit(final RequestMessage requestMessage) throws Exception;

    public default CompletableFuture<List<ResponseMessage>> submitAsync(final String gremlin) throws Exception {
        return submitAsync(RequestMessage.build(gremlin).create());
    }

    public CompletableFuture<List<ResponseMessage>> submitAsync(final RequestMessage requestMessage) throws Exception;
}
