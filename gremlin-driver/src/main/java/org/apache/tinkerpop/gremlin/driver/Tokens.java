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

import org.apache.tinkerpop.gremlin.driver.message.ResponseMessage;
import org.apache.tinkerpop.gremlin.process.traversal.Failure;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * String constants used in gremlin-driver and gremlin-server.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class Tokens {
    private Tokens() {}

    public static final String OPS_AUTHENTICATION = "authentication";
    public static final String OPS_BYTECODE = "bytecode";
    public static final String OPS_EVAL = "eval";
    public static final String OPS_INVALID = "invalid";
    public static final String OPS_CLOSE = "close";

    /**
     * The key for the unique identifier of the request.
     */
    public static final String REQUEST_ID = "requestId";

    /**
     * Argument name that allows definition of the number of iterations each {@link ResponseMessage} should contain -
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
    public static final String ARGS_ALIASES = "aliases";
    public static final String ARGS_FORCE = "force";

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
    public static final String ARGS_HOST = "host";
    public static final String ARGS_SESSION = "session";
    public static final String ARGS_MANAGE_TRANSACTION = "manageTransaction";

    /**
     * Argument name that is intended to be used with a session which when its value is {@code true} makes it so
     * that a processing error or request timeout will not close the session, but leave it to continue processing in
     * whatever state it may hold. This argument only applies to the {@code UnifiedChannelizer}.
     */
    public static final String ARGS_MAINTAIN_STATE_AFTER_EXCEPTION = "maintainStateAfterException";
    public static final String ARGS_SASL = "sasl";
    public static final String ARGS_SASL_MECHANISM = "saslMechanism";

    /**
     * A value that is a custom string that the user can pass to a server that might accept it for purpose of
     * identifying the kind of client it came from.
     */
    public static final String ARGS_USER_AGENT = "userAgent";

    public static final String VAL_TRAVERSAL_SOURCE_ALIAS = "g";

    /**
     * The value of this key holds a string representation of the data held by a {@link Failure} as produced by
     * {@link Failure#format()}.
     */
    public static final String STATUS_ATTRIBUTE_FAIL_STEP_MESSAGE = "failStepMessage";

    /**
     * Refers to the hierarchy of exception names for a particular exception thrown on the server.
     */
    public static final String STATUS_ATTRIBUTE_EXCEPTIONS = "exceptions";

    /**
     * Refers to the stacktrace for an exception thrown on the server
     */
    public static final String STATUS_ATTRIBUTE_STACK_TRACE = "stackTrace";

    /**
     * A {@link ResultSet#statusAttributes()} key for user-facing warnings.
     * <p>
     * Implementations that set this key should consider using one of
     * these two recommended value types:
     * <ul>
     *     <li>A {@code List} implementation containing
     *     references for which {@code String#valueOf(Object)} produces
     *     a meaningful return value.  For example, a list of strings.</li>
     *     <li>Otherwise, any single non-list object for which
     *     {@code String#valueOf(Object)} produces a meaningful return value.
     *     For example, a string.</li>
     * </ul>
     */
    public static final String STATUS_ATTRIBUTE_WARNINGS = "warnings";
}
