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
package org.apache.tinkerpop.gremlin.fluency;

/**
 * How a {@link Candidate} fared when run in place of its scenario's traversal.
 */
public enum Outcome {
    /**
     * The scenario passed with the candidate traversal.
     */
    PASS,

    /**
     * The driver produced no traversal.
     */
    NO_ANSWER,

    /**
     * The driver replied without writing a traversal, typically to explain why it gave none.
     */
    DECLINED,

    /**
     * The candidate is not valid gremlin-lang.
     */
    PARSE_FAILURE,

    /**
     * The candidate parsed but raised an error when it was iterated.
     */
    RUNTIME_ERROR,

    /**
     * The candidate ran but its result did not match the scenario's expected result.
     */
    WRONG_RESULT,

    /**
     * The candidate did not finish within the traversal timeout.
     */
    TIMEOUT
}
