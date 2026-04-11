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
package org.apache.tinkerpop.gremlin.tinkergraph.process.gql;

/**
 * Represents a vertex pattern element in a GQL MATCH clause. A node may have an optional
 * variable name for result binding and an optional label constraint for filtering.
 * Both fields are nullable: an anonymous node {@code ()} has neither variable nor label.
 */
public final class QueryNode {

    private final String variable;
    private final String label;

    public QueryNode(final String variable, final String label) {
        this.variable = variable;
        this.label = label;
    }

    /**
     * Returns the variable name bound to this node, or {@code null} if anonymous.
     */
    public String getVariable() {
        return variable;
    }

    /**
     * Returns the label constraint for this node, or {@code null} if unconstrained.
     */
    public String getLabel() {
        return label;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("(");
        if (variable != null) sb.append(variable);
        if (label != null) sb.append(':').append(label);
        sb.append(')');
        return sb.toString();
    }
}
