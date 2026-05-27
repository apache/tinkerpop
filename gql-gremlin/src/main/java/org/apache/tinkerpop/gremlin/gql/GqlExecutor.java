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
package org.apache.tinkerpop.gremlin.gql;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.Iterator;
import java.util.Map;

/**
 * Executes a {@link GqlMatchPlan} against a graph and returns result rows.
 *
 * <p>Each result row is an {@code Element[]} whose indices correspond to the variable
 * indices defined in the plan; use {@link GqlMatchPlan#getVariables()} to map indices
 * back to variable names.</p>
 */
public interface GqlExecutor {

    /**
     * Executes the plan with no parameter bindings.
     *
     * @param plan the compiled execution plan
     * @return a lazy iterator of binding arrays
     */
    Iterator<Element[]> execute(GqlMatchPlan plan);

    /**
     * Executes the plan with the given parameter bindings.
     *
     * @param plan   the compiled execution plan
     * @param params parameter bindings for {@code $name} references in property predicates;
     *               may be empty if the query contains no parameter references
     * @return a lazy iterator of binding arrays
     */
    Iterator<Element[]> execute(GqlMatchPlan plan, Map<String, Object> params);
}
