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

/**
 * Defines the synchronous request submission contract for Gremlin requests.
 * <p>
 * This interface is implemented by both {@link Client} and transaction classes to ensure a consistent API for
 * submitting Gremlin scripts. The synchronous nature of these methods means they block until the request completes.
 * <p>
 * For asynchronous submission, see {@link RequestSubmitterAsync}.
 */
public interface RequestSubmitter {

    /**
     * Submits a Gremlin script and blocks until the response is received.
     *
     * @param gremlin the Gremlin script to execute
     * @return the results of the script execution
     */
    ResultSet submit(String gremlin);

    /**
     * Submits a Gremlin script with bound parameters and blocks until the response is received.
     * <p>
     * Prefer this method over string concatenation when executing scripts with variable
     * arguments, as parameterized scripts perform better.
     *
     * @param gremlin the Gremlin script to execute
     * @param parameters a map of parameters that will be bound to the script on execution
     * @return the results of the script execution
     */
    ResultSet submit(String gremlin, Map<String, Object> parameters);

    /**
     * Submits a Gremlin script with request options and blocks until the response is received.
     *
     * @param gremlin the Gremlin script to execute
     * @param options the options to supply for this request
     * @return the results of the script execution
     */
    ResultSet submit(String gremlin, RequestOptions options);
}
