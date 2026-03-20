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

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Defines the asynchronous request submission contract for Gremlin requests.
 * <p>
 * This interface is implemented by {@link Client} to provide non-blocking request submission. The returned
 * {@link CompletableFuture} completes when the write of the request is complete, not when the response is received.
 * <p>
 * Note: Transaction classes intentionally do not implement this interface because transactional operations require
 * sequential execution to maintain ordering guarantees over HTTP.
 * <p>
 * For synchronous submission, see {@link RequestSubmitter}.
 */
public interface RequestSubmitterAsync {

    /**
     * Submits a Gremlin script asynchronously.
     * <p>
     * The returned future completes when the write of the request is complete.
     *
     * @param gremlin the Gremlin script to execute
     * @return a future that completes with the results when the request write is complete
     */
    CompletableFuture<ResultSet> submitAsync(String gremlin);

    /**
     * Submits a Gremlin script with bound parameters asynchronously.
     * <p>
     * Prefer this method over string concatenation when executing scripts with variable
     * arguments, as parameterized scripts perform better.
     *
     * @param gremlin the Gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     * @return a future that completes with the results when the request write is complete
     */
    CompletableFuture<ResultSet> submitAsync(String gremlin, Map<String, Object> parameters);

    /**
     * Submits a Gremlin script with request options asynchronously.
     *
     * @param gremlin the Gremlin script to execute
     * @param options the options to supply for this request
     * @return a future that completes with the results when the request write is complete
     */
    CompletableFuture<ResultSet> submitAsync(String gremlin, RequestOptions options);
}
