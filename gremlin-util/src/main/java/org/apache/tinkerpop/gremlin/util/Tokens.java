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

import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * String constants used in gremlin-driver and gremlin-server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Tokens {
    private Tokens() {}

    /**
     * Argument name that allows definition of the number of iterations each HTTP chunk should contain -
     * overrides the @{code resultIterationBatchSize} server setting.
     */
    public static final String ARGS_BATCH_SIZE = "batchSize";

    /**
     * Argument name that allows to provide a map of key/value pairs to apply as variables in the context of
     * the Gremlin request sent to the server.
     */
    public static final String ARGS_BINDINGS = "bindings";

    /**
     * Argument name that allows definition of alias names for {@link Graph} and {@link TraversalSource} objects on
     * the remote system.
     */
    public static final String ARGS_G = "g";

    /**
     * Argument name that corresponds to the Gremlin to evaluate.
     */
    public static final String ARGS_GREMLIN = "gremlin";

    /**
     * Argument name that allows definition of the flavor of Gremlin used (e.g. gremlin-groovy) to process the request.
     */
    public static final String ARGS_LANGUAGE = "language";

    /**
     * Argument name that allows the override of the server setting that determines the maximum time to wait for a
     * request to execute on the server.
     */
    public static final String ARGS_EVAL_TIMEOUT = "evaluationTimeout";

    /**
     * The name of the argument that allows to control the serialization of properties on the server.
     */
    public static final String ARGS_MATERIALIZE_PROPERTIES = "materializeProperties";

    /**
     * The name of the value denoting that all properties of Element should be returned.
     * Should be used with {@code ARGS_MATERIALIZE_PROPERTIES}
     */

    public static final String MATERIALIZE_PROPERTIES_ALL = "all";

    /**
     * The name of the value denoting that only `ID` and `Label` of Element should be returned.
     * Should be used with {@code ARGS_MATERIALIZE_PROPERTIES}
     */
    public static final String MATERIALIZE_PROPERTIES_TOKENS = "tokens";

    /**
     * The key for the per request server-side timeout in milliseconds.
     */
    public static final String TIMEOUT_MS = "timeoutMs";

    /**
     * The key for server to bulk result as a form of optimization for driver requests.
     */
    public static final String BULK_RESULTS = "bulkResults";

    /**
     * A value that is a custom string that the user can pass to a server that might accept it for purpose of
     * identifying the kind of client it came from.
     */
    public static final String ARGS_USER_AGENT = "userAgent";
}
