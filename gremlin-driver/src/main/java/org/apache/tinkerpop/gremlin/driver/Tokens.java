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
    public static final String OPS_GATHER = "gather";
    public static final String OPS_KEYS = "keys";
    public static final String OPS_CLOSE = "close";

    public static final String ARGS_BATCH_SIZE = "batchSize";
    public static final String ARGS_BINDINGS = "bindings";
    public static final String ARGS_ALIASES = "aliases";
    public static final String ARGS_FORCE = "force";
    public static final String ARGS_GREMLIN = "gremlin";
    public static final String ARGS_LANGUAGE = "language";
    public static final String ARGS_SCRIPT_EVAL_TIMEOUT = "scriptEvaluationTimeout";
    public static final String ARGS_HOST = "host";
    public static final String ARGS_SESSION = "session";
    public static final String ARGS_MANAGE_TRANSACTION = "manageTransaction";
    public static final String ARGS_SASL = "sasl";
    public static final String ARGS_SASL_MECHANISM = "saslMechanism";
    public static final String ARGS_SIDE_EFFECT = "sideEffect";
    public static final String ARGS_AGGREGATE_TO = "aggregateTo";
    public static final String ARGS_SIDE_EFFECT_KEY = "sideEffectKey";

    public static final String VAL_AGGREGATE_TO_BULKSET = "bulkset";
    public static final String VAL_AGGREGATE_TO_LIST = "list";
    public static final String VAL_AGGREGATE_TO_MAP = "map";
    public static final String VAL_AGGREGATE_TO_NONE = "none";
    public static final String VAL_AGGREGATE_TO_SET = "set";

    public static final String VAL_TRAVERSAL_SOURCE_ALIAS = "g";

    public static final String STATUS_ATTRIBUTE_EXCEPTIONS = "exceptions";
    public static final String STATUS_ATTRIBUTE_STACK_TRACE = "stackTrace";
}
